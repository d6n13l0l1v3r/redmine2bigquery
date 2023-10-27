#!/usr/bin/env bash
#
# This scripts dumps data from redmine's database into a bigquery dataset.
#
# Before invoking this script, a functional bq dataset should have been
# created as follows:
#  - bq mk --data_location EU redmine
#  - bq mk \
#	--schema \
#		id:integer,tracker:string,project:string,priority:string,category:string,status:string,resolution:string, \
#		author:string,assigned_to:string,start_date:timestamp,due_date:timestamp, \
#		estimated_hours:float,created_on:timestamp
#	-t redmine.issues
#  - bq mk \
#	--schema \
#		id:integer,issue_id:integer,user:string,notes:string,property:string, \
#		prop_key:string,value:string,old_value:string,created_on:timestamp \
#	-t redmine.changes
#

set -o errexit -o noclobber -o nounset -o pipefail # Safe defaults..

LC_ALL=C
LANG=C
PATH=/bin:/sbin:/usr/bin:/usr/sbin
export LC_ALL LANG PATH

SWD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

: ${MYSQL:=mysql}
: ${BQ:=bq}

MYSQLBCMD="${MYSQL} -ss -N -B --default-character-set=utf8 "
BQBCMD="${BQ} -q --headless"
MAXCHANGES=300

## Command Line Options parsing..
set +e
source ${SWD}/shflags
DEFINE_string 	'server'		'localhost'	"server's address to connect to"        's'
DEFINE_integer	'port'			3306		"server's port to connect to"           'p'
DEFINE_string	'username'		'redmine'	"server's username to auth with"        'u'
DEFINE_string	'dbname'		'redmine'	"database name to dump from"            'd'
DEFINE_string	'project'		''		"bigquery's project to use"		''
DEFINE_string	'dataset'		''		"bigquery's dataset to use"		''
DEFINE_integer	'max-issues'		150		"maximum number of issues to dump"	''
DEFINE_integer	'max-changes'		900		"maximum number of changes to dump"	''
DEFINE_integer	'max-days'		10		"maximum number of days to process"	''
DEFINE_string	'include-projects'	''		"projects to include in dump"		''
DEFINE_string	'exclude-projects'	''		"projects to exclude from dump"		''
set -e

finish () {
        : # none
}

warn () {
        echo "${BASH_SOURCE[1]}:${BASH_LINENO[0]} => " "$@" >&2
}

die () {
        local rc=$1
        shift
        [ -z "$@" ] || echo "${BASH_SOURCE[1]}:${BASH_LINENO[0]} => " "$@" >&2
        exit $rc
}

argument_invalid () {
        echo "$1" >&2
        flags_help
        exit 255
}

argument_required () {
        local NAME="FLAGS_$1"
        local VALUE=${!NAME:-}
        if [ -z "$VALUE" ]; then
                echo "ERROR: Required argument missing: $1" >&2
                flags_help
                exit 255
        fi
}

join_by () {
	local IFS="$1"; shift; echo "$*";
}

declare_issue_changes_view () {
	local -r dbname=${FLAGS_dbname}

	${MYSQLBCMD} "${dbname}" <<-_EOF
		-- Create issue_changes VIEW
		CREATE OR REPLACE
		-- ALGORITHM = TEMPTABLE
		VIEW issue_changes AS
			SELECT j.id, j.journalized_id AS issue_id, j.created_on, j.user_id, j.notes, jd.property, jd.prop_key, jd.value, jd.old_value
			FROM journals AS j
			LEFT JOIN journal_details AS jd ON (j.id = jd.journal_id)
			WHERE j.journalized_type = 'Issue'
			ORDER BY j.created_on ASC;
	_EOF
}

get_project_ids () {
	local result names tmp
	local -r projects="$1"
	local -r dbname=${FLAGS_dbname}

	[ -z "${projects}" ] && return 1

	names="${projects//'/\'}" ## Avoid sql injections..
	result=$(${MYSQLBCMD} "${dbname}" <<-_EOF
		SET @names = '${names}';
		SELECT GROUP_CONCAT(DISTINCT id ORDER BY id ASC SEPARATOR ',')
		FROM projects WHERE FIND_IN_SET(identifier, @names) > 0;
		_EOF
	)

	while :;
	do \
		local ids="${result}"
		tmp=$(${MYSQLBCMD} "${dbname}" <<-_EOF
			SELECT GROUP_CONCAT(DISTINCT id ORDER BY id ASC SEPARATOR ',')
		        FROM projects WHERE parent_id IN (${ids}) OR id IN (${ids});
			_EOF
		)

		[ "${tmp}" != "${result}" ] || break
		result="${tmp}"
	done

	echo ${result//,/ }
}

get_resolution_prop_id () {
	echo $(${MYSQLBCMD} "${dbname}" <<-_EOF
		SELECT id FROM custom_fields WHERE type = 'IssueCustomField' AND name = 'Resolution';
		_EOF
	)
}

get_bq_issue_lastid () {
	local -r dataset="${FLAGS_dataset}"
	local -r project=${FLAGS_project:+--project_id ${FLAGS_project}}

	${BQBCMD} --format csv query ${project} --use_legacy_sql=false \
		"SELECT 0 as id UNION ALL SELECT id FROM ${dataset}.issues ORDER BY id DESC LIMIT 1;" | tail -n 1
}

get_bq_changes_lastid () {
	local -r dataset="${FLAGS_dataset}"
	local -r project=${FLAGS_project:+--project_id ${FLAGS_project}}

	${BQBCMD} --format csv query ${project} --use_legacy_sql=false \
		"SELECT 0 as id UNION ALL SELECT id FROM ${dataset}.changes ORDER BY id DESC LIMIT 1;" | tail -n 1
}

get_bq_byday_startdate () {
	local -r dataset="${FLAGS_dataset}"
	local -r project=${FLAGS_project:+--project_id ${FLAGS_project}}

	${BQBCMD} --format csv query ${project} --use_legacy_sql=false <<-EOF |
		SELECT FORMAT("%t", r.date) FROM (
		  (SELECT 1 AS idx, DATE_ADD(i1.date, INTERVAL 1 DAY) AS date FROM ${dataset}.issuesbyday AS i1 ORDER BY i1.date DESC LIMIT 1)
		  UNION ALL
		  (SELECT 2 AS idx, CAST(created_on AS DATE) AS date FROM ${dataset}.issues ORDER BY created_on ASC LIMIT 1)
		  ) AS r
		ORDER BY idx ASC
		LIMIT 1;
	EOF
       	tail -n 1
}

fetch_issues () {
	local -i -r id=$1
	local projects=$2
	local -r dbname=${FLAGS_dbname}
	local -r dataset="${FLAGS_dataset}"
	local -i limit=${FLAGS_max_issues//'/\'}
	local -r date=$(date +"%Y-%m-%d 00:00:00")
	local -i count=0
	local -r resid=$(get_resolution_prop_id)

	[ -z "${projects}" -o "${projects}" = "0" ]&& projects="SELECT id FROM projects"

	${MYSQLBCMD} "${dbname}" <<-_EOF |
	SET @id = ${id};
	SET @date = '${date}';
	SET @resid = '${resid}';

	SELECT v.id, t.name, p.name, e.name, c.name, s.name, v.resolution,
		IF(LENGTH(u1.login) = 0, u1.lastname, u1.login) AS assigned_to, v.due_date,
		IF(LENGTH(u2.login) = 0, u2.lastname, u2.login) AS author,
		v.created_on
	FROM (
		SELECT i.id,
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = 'tracker_id' ORDER BY id ASC LIMIT 1), i.tracker_id)) AS tracker_id,
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = 'project_id' ORDER BY id ASC LIMIT 1), i.project_id)) AS project_id,
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = 'priority_id' ORDER BY id ASC LIMIT 1), i.priority_id)) AS priority_id,
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = 'category_id' ORDER BY id ASC LIMIT 1), i.category_id)) AS category_id,
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = 'status_id' ORDER BY id ASC LIMIT 1), i.status_id)) AS status_id,
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = 'assigned_to_id' ORDER BY id ASC LIMIT 1), i.assigned_to_id)) AS assigned_to_id,
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = 'due_date' ORDER BY id ASC LIMIT 1), i.due_date)) AS due_date,
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = @resid AND old_value <> '' ORDER BY id ASC LIMIT 1), NULL)) as resolution,
			i.author_id, i.created_on
		FROM issues AS i
		WHERE i.id > @id AND i.created_on < @date AND i.project_id IN (${projects})
		ORDER BY i.id ASC
		LIMIT ${limit}
	) AS v
	LEFT JOIN trackers AS t ON (t.id = v.tracker_id)
	LEFT JOIN projects AS p ON (p.id = v.project_id)
	LEFT JOIN enumerations AS e ON (e.id = v.priority_id)
	LEFT JOIN issue_categories AS c ON (c.id = v.category_id)
	LEFT JOIN issue_statuses AS s ON (s.id = v.status_id)
	LEFT JOIN users AS u1 ON (u1.id = v.assigned_to_id)
	LEFT JOIN users AS u2 ON (u2.id = v.author_id)
	;
	_EOF
	{
		while IFS=$'\t' read -r -a values
		do
			if [ $count -eq 0 ]
			then \
				#TODO Need to confirm if we planing to use same table or change to v2
				echo "INSERT INTO ${dataset}.issues (" \
				     "id, tracker, project, priority, category, status, resolution, " \
				     "assigned_to, due_date, author, created_on) " \
				     "VALUES "
				count=$(($count+1))
			else
				echo -en ", "
			fi
			#echo "${values[@]} -- ${#values[@]}" >&2
			for i in {1..9}
			do
				if [ "$i" -eq 6 -o "$i" -eq 7 -o "$i" -eq 8 -o "$i" -eq 9 ];
				then \
					[ "${values[$i]}" != "NULL" ]&& values[$i]="'${values[$i]//\'/\'}'"
				else
					values[$i]="'${values[$i]//\'/\'}'"
				fi
			done
			echo "($( IFS=','; echo "${values[*]}" )) "

		done
		[ $count -eq 0 ]&& echo "SELECT NULL FROM ${dataset}.issues WHERE 1=0" || :
	}
}

fetch_changes () {
	local -i -r id=$1
	local -i -r limit=${2//'/\'}
	local -r dbname=${FLAGS_dbname}
	local -r dataset="${FLAGS_dataset}"
	local -r date=$(date +"%Y-%m-%d 00:00:00")
	local -r resid=$(get_resolution_prop_id)
	local -i count=0

	${MYSQLBCMD} "${dbname}" <<-_EOF |
		SET @id = ${id};
		SET @date = '${date}';
		SET @resid = '${resid}';

		SELECT c.id, c.issue_id,
			IF(LENGTH(u.login) = 0, u.lastname, u.login) AS user,
			IF(LENGTH(notes) = 0, NULL, BASE64_ENCODE('**Text not imported**')) AS notes,
			IF(LENGTH(property) = 0, NULL, property) AS property,
			IF(LENGTH(prop_key) = 0, NULL,
				CASE prop_key
				WHEN 'category_id' THEN 'category'
				WHEN 'status_id' THEN 'status'
				WHEN 'assigned_to_id' THEN 'assigned_to'
				WHEN 'tracker_id' THEN 'tracker'
				WHEN 'project_id' THEN 'project'
				WHEN 'priority_id' THEN 'priority'
				WHEN @resid THEN 'resolution'
				ELSE prop_key
				END
			) AS prop_key,
			IF(property = 'attr',
				CASE prop_key
				WHEN 'subject' THEN BASE64_ENCODE('*Subject not imported*')
				WHEN 'description' THEN BASE64_ENCODE('*Description not imported*')
				WHEN 'category_id' THEN BASE64_ENCODE((SELECT name FROM issue_categories WHERE id = value))
				WHEN 'status_id' THEN BASE64_ENCODE((SELECT name FROM issue_statuses WHERE id = value))
				WHEN 'assigned_to_id' THEN BASE64_ENCODE(
					(SELECT IF(LENGTH(login) = 0, lastname, login) FROM users WHERE id = value)
				)
				WHEN 'tracker_id' THEN BASE64_ENCODE((SELECT name FROM trackers WHERE id = value))
				WHEN 'project_id' THEN BASE64_ENCODE((SELECT name FROM projects WHERE id = value))
				WHEN 'priority_id' THEN BASE64_ENCODE((SELECT name FROM enumerations WHERE type = 'IssuePriority' AND id = value))
				ELSE IF(LENGTH(value) = 0, NULL, BASE64_ENCODE(value))
				END,
				IF(LENGTH(value) = 0, NULL, BASE64_ENCODE(value))

			) AS value,
			IF(property = 'attr',
				CASE prop_key
				WHEN 'subject' THEN BASE64_ENCODE('*Subject not imported*')
				WHEN 'description' THEN BASE64_ENCODE('*Description not imported*')
				WHEN 'category_id' THEN BASE64_ENCODE((SELECT name FROM issue_categories WHERE id = old_value))
				WHEN 'status_id' THEN BASE64_ENCODE((SELECT name FROM issue_statuses WHERE id = old_value))
				WHEN 'assigned_to_id' THEN BASE64_ENCODE(
					(SELECT IF(LENGTH(login) = 0, lastname, login) FROM users WHERE id = old_value)
				)
				WHEN 'tracker_id' THEN BASE64_ENCODE((SELECT name FROM trackers WHERE id = old_value))
				WHEN 'project_id' THEN BASE64_ENCODE((SELECT name FROM projects WHERE id = old_value))
				WHEN 'priority_id' THEN BASE64_ENCODE((SELECT name FROM enumerations WHERE type = 'IssuePriority' AND id = old_value))
				ELSE IF(LENGTH(old_value) = 0, NULL, BASE64_ENCODE(old_value))
				END,
				IF(LENGTH(old_value) = 0, NULL, BASE64_ENCODE(old_value))
			) AS old_value,
			c.created_on
		FROM issue_changes AS c
		LEFT JOIN users AS u ON (c.user_id = u.id)
		WHERE c.id > @id AND c.created_on < @date
		ORDER BY c.id ASC
		LIMIT ${limit}
	_EOF
	{
		while IFS=$'\t' read -r -a values
		do
			if [ $count -eq 0 ]
			then \
				echo "INSERT INTO ${dataset}.changes (" \
				     "id, issue_id, user, notes, property, prop_key, " \
				     "value, old_value, created_on) " \
				     "VALUES "
				count=$(($count+1))
			else
				echo -en ", "
			fi
			for i in {2..8}
			do
				if [ "$i" -eq 3 -o "$i" -eq 6 -o "$i" -eq 7 ];
				then \
					[ "${values[$i]}" != "NULL" ]&& values[$i]="CAST(FROM_BASE64('${values[$i]}') AS STRING)"
				elif [ "$i" -ge 2 -o "$i" -le 7 ];
				then \
					[ "${values[$i]}" != "NULL" ]&& values[$i]="'${values[$i]}'"
				else
					values[$i]="'${values[$i]}'"
				fi
			done
			echo "($( IFS=','; echo "${values[*]}" )) "
		done
		[ $count -eq 0 ]&& echo "SELECT NULL FROM ${dataset}.issues WHERE 1=0" || :
	}
}

#TODO Need to confirm if we planing to use same table or change to v2
create_bq_byday_table ()
{
	local -r dataset="${FLAGS_dataset}"
	local -r project=${FLAGS_project:+--project_id ${FLAGS_project}}

	${BQBCMD} ls ${project} ${dataset} |grep TABLE | \
		grep -q -E '[[:space:]]+issuesbyday[[:space:]]+' \
			&& return 0
	echo "Creating (issues) by day table.."
	${BQBCMD} mk --schema \
		"$(cat <<-EOF
			id:integer,date:date,
			tracker:string,project:string,category:string,priority:string,status:string,assigned_to:string,
			resolution:string,created_on:timestamp,updated_on:timestamp
		EOF)" \
		-t ${dataset}.issuesbyday
}

#TODO Need to confirm if we planing to use same destination_table or change to v2

update_byday_table ()
{
	local -r startdate=$1
	local -r dataset="${FLAGS_dataset}"
	local -r project=${FLAGS_project:+--project_id ${FLAGS_project}}
	local -i maxdays=${FLAGS_max_days}

	${BQBCMD} query ${project} --format none \
		--use_legacy_sql=false --allow_large_results \
		--destination_table="${dataset}.issuesbyday" --append_table \
		--parameter "startdate:DATE:${startdate}" \
		--parameter "maxdays:INTEGER:${maxdays}" \
	<<-EOF
		WITH Changes AS (
			SELECT
				issue_id,
				MAX(CASE WHEN property = 'attr' AND prop_key = 'status' THEN value END) AS status,
				MAX(CASE WHEN property = 'attr' AND prop_key = 'assigned_to' THEN value END) AS assigned_to,
				MAX(CASE WHEN property = 'attr' AND prop_key = 'tracker' THEN value END) AS tracker,
				MAX(CASE WHEN property = 'attr' AND prop_key = 'project' THEN value END) AS project,
				MAX(CASE WHEN property = 'attr' AND prop_key = 'category' THEN value END) AS category,
				MAX(CASE WHEN property = 'attr' AND prop_key = 'priority' THEN value END) AS priority,
				MAX(CASE WHEN property = 'cf' AND prop_key = 'resolution' THEN value END) AS resolution,
				MAX(created_on) AS last_change_date
			FROM
				${dataset}.changes
			WHERE
				(property = 'attr' AND prop_key IN ('status', 'assigned_to', 'tracker', 'project', 'category', 'priority'))
				OR (property = 'cf' AND prop_key = 'resolution')
			GROUP BY
				issue_id
		)
		SELECT
		r.date,
		r.id,
		r.created_on,
		(IFNULL(LAST_VALUE(c.status) OVER ws, r.status)) AS status,
		(IFNULL(LAST_VALUE(c.assigned_to) OVER wa, r.assigned_to)) AS assigned_to,
		(IFNULL(LAST_VALUE(c.tracker) OVER wt, r.tracker)) AS tracker,
		(IFNULL(LAST_VALUE(c.project) OVER wp, r.project)) AS project,
		(IFNULL(LAST_VALUE(c.category) OVER wc, r.category)) AS category,
		(IFNULL(LAST_VALUE(c.priority) OVER wpr, r.priority)) AS priority,
		(IFNULL(LAST_VALUE(c.resolution) OVER wres, r.resolution)) AS resolution,
		(GREATEST(
			COALESCE(MAX(c.last_change_date) OVER ws, r.created_on),
			COALESCE(MAX(c.last_change_date) OVER wa, r.created_on),
			COALESCE(MAX(c.last_change_date) OVER wt, r.created_on),
			COALESCE(MAX(c.last_change_date) OVER wp, r.created_on),
			COALESCE(MAX(c.last_change_date) OVER wc, r.created_on),
			COALESCE(MAX(c.last_change_date) OVER wpr, r.created_on),
			COALESCE(MAX(c.last_change_date) OVER wres, r.created_on),
			r.created_on
		)) AS updated_on
		FROM
		(
			SELECT
			cx.date,
			r1.id,
			r1.created_on,
			r1.status,
			r1.assigned_to,
			r1.tracker,
			r1.project,
			r1.category,
			r1.priority,
			r1.resolution
			FROM
			${dataset}.issues AS r1
			CROSS JOIN (
				SELECT
					*
				FROM
					UNNEST(
						GENERATE_DATE_ARRAY(
							@startdate,
							LEAST(DATE_ADD(@startdate, INTERVAL @maxdays DAY), DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)),
							INTERVAL 1 DAY
						)
					) AS date
			) AS cx
			WHERE
			r1.created_on <= CAST(cx.date AS TIMESTAMP)
		) AS r
		LEFT OUTER JOIN
		Changes AS c
		ON
		r.id = c.issue_id
		WINDOW
		ws AS (PARTITION BY c.issue_id ORDER BY c.last_change_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
		wa AS (PARTITION BY c.issue_id ORDER BY c.last_change_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
		wt AS (PARTITION BY c.issue_id ORDER BY c.last_change_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
		wp AS (PARTITION BY c.issue_id ORDER BY c.last_change_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
		wc AS (PARTITION BY c.issue_id ORDER BY c.last_change_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
		wpr AS (PARTITION BY c.issue_id ORDER BY c.last_change_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING),
		wres AS (PARTITION BY c.issue_id ORDER BY c.last_change_date ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
		ORDER BY
		r.date,
		r.id DESC;
	EOF

}

main () {
	local prjids
	local -r dbname="${FLAGS_dbname}"
	local -r dataset="${FLAGS_dataset}"
	local -r project=${FLAGS_project:+--project_id ${FLAGS_project}}
	local -a includes=() excludes=() projects=()
	local -i max_issues=${FLAGS_max_issues}
	local -i max_changes=${FLAGS_max_changes}
	local -i max_days=${FLAGS_max_days}
	local -i lastid=0

	declare_issue_changes_view

	if [ ${max_issues} -gt 0 ]
	then \
		includes=($(get_project_ids "${FLAGS_include_projects}" || echo '0'))
		excludes=($(get_project_ids "${FLAGS_exclude_projects}" || echo '0'))
		projects=(0 $(comm -23 <(printf '%s\n' "${includes[@]}" | sort) <(printf '%s\n' "${excludes[@]}" | sort)))
		prjids=$( IFS=','; echo "${projects[*]}" )
		lastid=$(get_bq_issue_lastid)

		echo -en "Exporting issues, starting at last id: ${lastid}... "

		fetch_issues ${lastid} "$(IFS=','; echo "${projects[*]}" )" | \
			${BQBCMD} query ${project} --dataset_id="${dataset}" --nouse_legacy_sql

		echo "done!"
	fi

	if [ ${max_changes} -gt 0 ]
	then \
		local -i max=${max_changes}
		local -i limit=0

		#for ((i=0; i<=$[${max}/${MAXCHANGES}]; i++))
		while : ;
		do \
			limit=$(( ${max} < ${MAXCHANGES} ? ${max} : ${MAXCHANGES} ))
			max=$(( ${max}-${limit} ))
			lastid=$(get_bq_changes_lastid)

			echo -en "Exporting changes, starting at last id: ${lastid} (limit: ${limit})... "

			fetch_changes ${lastid} ${limit} | \
				${BQBCMD} query ${project} --dataset_id="${dataset}" --nouse_legacy_sql

			echo "done!"

			[ ${max} -le 0 ]&& break
		done
	fi

	# TODO: remove orphan changes.. (ie. those not referended by any issue)

	if [ ${max_days} -gt 0 ]
	then \
		local start=$(get_bq_byday_startdate)
		echo -en "Updating 'Issues By Day' table... (start: ${start}) "

		create_bq_byday_table
		update_byday_table ${start} >/dev/null \
			|| die $? "ERROR: BQ query failed! (error: $?)"

		echo "done!"
	fi

	echo "finished!"

        exit 0
}

trap finish EXIT

# parse the command-line
FLAGS "$@" || exit $?
eval set -- "${FLAGS_ARGV}"

[ -x "${MYSQL}" -o -x "$(command -v ${MYSQL})" ]|| die 255 "ERROR: Missing 'mysql' command!"
[ -x "${BQ}" -o -x "$(command -v ${BQ})" ]|| die 255 "ERROR: Missing 'bq' command!"

# Validate arguments..
for arg in dbname dataset project
do \
        argument_required $arg
done

main "$@" # Let's go for it.. :)

# vim: ai ts=4 sw=4 noet sts=4 ft=sh
