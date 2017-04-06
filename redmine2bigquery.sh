#!/usr/bin/env bash
#
# This scripts dumps data from redmine's database into a bigquery dataset.
#
# Before invoking this script, a functional bq dataset should have been
# created as follows:
#  - bq mk --data_location EU redmine
#  - bq mk \
#	--schema \
#		id:integer,tracker:string,project:string,priority:string,status:string, \
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

## Command Line Options parsing..
set +e
source ${SWD}/shflags
DEFINE_string 	'server'		'localhost'	"server's address to connect to"        's'
DEFINE_integer	'port'			3306		"server's port to connect to"           'p'
DEFINE_string	'username'		'redmine'	"server's username to auth with"        'u'
DEFINE_string	'dbname'		'redmine'	"database name to dump from"            'd'
DEFINE_string	'project'		''		"bigquery's project to use"		''
DEFINE_string	'dataset'		''		"bigquery's dataset to use"		''
DEFINE_integer	'max-issues'		100		"maximum number of issues to dump"	''
DEFINE_integer	'max-changes'		300		"maximum number of changes to dump"	''
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

fetch_issues () {
	local -i -r id=$1
	local projects=$2
	local -r dbname=${FLAGS_dbname}
	local -r dataset="${FLAGS_dataset}"
	local -i limit=${FLAGS_max_issues//'/\'}
	local -r date=$(date +"%Y-%m-%d 00:00:00")
	local -i count=0

	[ -z "${projects}" -o "${projects}" = "0" ]&& projects="SELECT id FROM projects"

	${MYSQLBCMD} "${dbname}" <<-_EOF |
	SET @id = ${id};
	SET @date = '${date}';

	SELECT v.id, t.name, p.name, e.name, s.name, 
		IF(LENGTH(u1.login) = 0, u1.lastname, u1.login) AS assigned_to, v.due_date, 
		IF(LENGTH(u2.login) = 0, u2.lastname, u2.login) AS author, 
		v.created_on
	FROM (
		SELECT i.id, 
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = 'tracker_id' LIMIT 1), i.tracker_id)) AS tracker_id,
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = 'project_id' LIMIT 1), i.project_id)) AS project_id,
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = 'priority_id' LIMIT 1), i.priority_id)) AS priority_id,
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = 'status_id' LIMIT 1), i.status_id)) AS status_id,
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = 'assigned_to_id' LIMIT 1), i.assigned_to_id)) AS assigned_to_id,
			(COALESCE((SELECT old_value FROM issue_changes WHERE issue_id = i.id AND prop_key = 'due_date' LIMIT 1), i.due_date)) AS due_date,	
			i.author_id, i.created_on
		FROM issues AS i
		WHERE i.id > @id AND i.created_on < @date AND i.project_id IN (${projects})
		ORDER BY i.id ASC
		LIMIT ${limit}
	) AS v
	LEFT JOIN trackers AS t ON (t.id = v.tracker_id)
	LEFT JOIN projects AS p ON (p.id = v.project_id)
	LEFT JOIN enumerations AS e ON (e.id = v.priority_id)
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
				echo "INSERT INTO ${dataset}.issues (" \
				     "id, tracker, project, priority, status, " \
				     "assigned_to, due_date, author, created_on) " \
				     "VALUES "
				count=$(($count+1))
			else
				echo -en ", "
			fi
			for i in {1..8}
			do
				if [ "$i" -eq 6 ];
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

fetch_changes () {
	local -i -r id=$1
	local -r dbname=${FLAGS_dbname}
	local -r dataset="${FLAGS_dataset}"
	local -i limit=${FLAGS_max_changes//'/\'}
	local -r date=$(date +"%Y-%m-%d 00:00:00")
	local -i count=0

	${MYSQLBCMD} "${dbname}" <<-_EOF |
		SET @id = ${id};
		SET @date = '${date}';

		SELECT c.id, c.issue_id,
			IF(LENGTH(u.login) = 0, u.lastname, u.login) AS user,
			IF(LENGTH(notes) = 0, NULL, BASE64_ENCODE('**Text not imported**')) AS notes,
			IF(LENGTH(property) = 0, NULL, property) AS property,
			IF(LENGTH(prop_key) = 0, NULL, prop_key) AS prop_key, 
			IF(((property = 'attr' AND prop_key IN ('subject', 'description')) OR property = 'attachment'),
				BASE64_ENCODE('*Text not imported*'),
				IF(LENGTH(value) = 0, NULL, BASE64_ENCODE(value))
			) AS value,
			IF(((property = 'attr' AND prop_key IN ('subject', 'description')) OR property = 'attachment'),
				BASE64_ENCODE('*Text not imported*'),
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

main () {
	local -r dbname="${FLAGS_dbname}"
	local -r dataset="${FLAGS_dataset}"
	local -r project=${FLAGS_project:+--project_id ${FLAGS_project}}
	local -a includes=() excludes=() projects=()
	local -i lastid
	local prjids

	includes=($(get_project_ids "${FLAGS_include_projects}" || echo '0'))
	excludes=($(get_project_ids "${FLAGS_exclude_projects}" || echo '0'))
	projects=(0 $(comm -23 <(printf '%s\n' "${includes[@]}" | sort) <(printf '%s\n' "${excludes[@]}" | sort)))
	prjids=$( IFS=','; echo "${projects[*]}" )
	lastid=$(get_bq_issue_lastid)

	declare_issue_changes_view

        echo -en "Exporting issues, starting at last id: ${lastid}..."

#	echo "INCLUDES => [${#includes[@]}] ${includes[@]}"
#	echo "EXCLUDES => [${#excludes[@]}] ${excludes[@]}"
#	echo "PROJECTS => [${#projects[@]}] ${projects[@]}"

#	${MYSQLBCMD} "${dbname}" <<-_EOF
#		SELECT identifier FROM projects WHERE id IN (${prjids});
#	_EOF

	fetch_issues ${lastid} "$(IFS=','; echo "${projects[*]}" )" | \
		${BQBCMD} query ${project} --dataset_id="${dataset}" --nouse_legacy_sql

	echo "done!"

	lastid=$(get_bq_changes_lastid)
	echo -en "Exporting changes, starting at last id: ${lastid}..."

	fetch_changes ${lastid} | \
		${BQBCMD} query ${project} --dataset_id="${dataset}" --nouse_legacy_sql

	echo "done!"

	# TODO: remove horfan changes.. (ie. those not referended by any issue)

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
