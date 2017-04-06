SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for base64_data
-- ----------------------------
DROP TABLE IF EXISTS `base64_data`;
CREATE TABLE `base64_data` (
  `c` char(1) CHARACTER SET utf8 COLLATE utf8_bin DEFAULT NULL,
  `val` tinyint(4) DEFAULT NULL
) ENGINE=MyISAM DEFAULT CHARSET=utf8;

-- ----------------------------
-- Records of base64_data
-- ----------------------------
INSERT INTO `base64_data` VALUES ('A', '0');
INSERT INTO `base64_data` VALUES ('B', '1');
INSERT INTO `base64_data` VALUES ('C', '2');
INSERT INTO `base64_data` VALUES ('D', '3');
INSERT INTO `base64_data` VALUES ('E', '4');
INSERT INTO `base64_data` VALUES ('F', '5');
INSERT INTO `base64_data` VALUES ('G', '6');
INSERT INTO `base64_data` VALUES ('H', '7');
INSERT INTO `base64_data` VALUES ('I', '8');
INSERT INTO `base64_data` VALUES ('J', '9');
INSERT INTO `base64_data` VALUES ('K', '10');
INSERT INTO `base64_data` VALUES ('L', '11');
INSERT INTO `base64_data` VALUES ('M', '12');
INSERT INTO `base64_data` VALUES ('N', '13');
INSERT INTO `base64_data` VALUES ('O', '14');
INSERT INTO `base64_data` VALUES ('P', '15');
INSERT INTO `base64_data` VALUES ('Q', '16');
INSERT INTO `base64_data` VALUES ('R', '17');
INSERT INTO `base64_data` VALUES ('S', '18');
INSERT INTO `base64_data` VALUES ('T', '19');
INSERT INTO `base64_data` VALUES ('U', '20');
INSERT INTO `base64_data` VALUES ('V', '21');
INSERT INTO `base64_data` VALUES ('W', '22');
INSERT INTO `base64_data` VALUES ('X', '23');
INSERT INTO `base64_data` VALUES ('Y', '24');
INSERT INTO `base64_data` VALUES ('Z', '25');
INSERT INTO `base64_data` VALUES ('a', '26');
INSERT INTO `base64_data` VALUES ('b', '27');
INSERT INTO `base64_data` VALUES ('c', '28');
INSERT INTO `base64_data` VALUES ('d', '29');
INSERT INTO `base64_data` VALUES ('e', '30');
INSERT INTO `base64_data` VALUES ('f', '31');
INSERT INTO `base64_data` VALUES ('g', '32');
INSERT INTO `base64_data` VALUES ('h', '33');
INSERT INTO `base64_data` VALUES ('i', '34');
INSERT INTO `base64_data` VALUES ('j', '35');
INSERT INTO `base64_data` VALUES ('k', '36');
INSERT INTO `base64_data` VALUES ('l', '37');
INSERT INTO `base64_data` VALUES ('m', '38');
INSERT INTO `base64_data` VALUES ('n', '39');
INSERT INTO `base64_data` VALUES ('o', '40');
INSERT INTO `base64_data` VALUES ('p', '41');
INSERT INTO `base64_data` VALUES ('q', '42');
INSERT INTO `base64_data` VALUES ('r', '43');
INSERT INTO `base64_data` VALUES ('s', '44');
INSERT INTO `base64_data` VALUES ('t', '45');
INSERT INTO `base64_data` VALUES ('u', '46');
INSERT INTO `base64_data` VALUES ('v', '47');
INSERT INTO `base64_data` VALUES ('w', '48');
INSERT INTO `base64_data` VALUES ('x', '49');
INSERT INTO `base64_data` VALUES ('y', '50');
INSERT INTO `base64_data` VALUES ('z', '51');
INSERT INTO `base64_data` VALUES ('0', '52');
INSERT INTO `base64_data` VALUES ('1', '53');
INSERT INTO `base64_data` VALUES ('2', '54');
INSERT INTO `base64_data` VALUES ('3', '55');
INSERT INTO `base64_data` VALUES ('4', '56');
INSERT INTO `base64_data` VALUES ('5', '57');
INSERT INTO `base64_data` VALUES ('6', '58');
INSERT INTO `base64_data` VALUES ('7', '59');
INSERT INTO `base64_data` VALUES ('8', '60');
INSERT INTO `base64_data` VALUES ('9', '61');
INSERT INTO `base64_data` VALUES ('+', '62');
INSERT INTO `base64_data` VALUES ('/', '63');
INSERT INTO `base64_data` VALUES ('=', '0');

-- ----------------------------
-- Function structure for BASE64_DECODE
-- ----------------------------
DROP FUNCTION IF EXISTS `BASE64_DECODE`;
DELIMITER ;;
CREATE DEFINER=`root`@`localhost` FUNCTION `BASE64_DECODE`(input BLOB) RETURNS blob
    DETERMINISTIC
    SQL SECURITY INVOKER
BEGIN
	DECLARE ret BLOB DEFAULT '';
	DECLARE done TINYINT DEFAULT 0;

	IF input IS NULL THEN
		RETURN NULL;
	END IF;

each_block:
	WHILE NOT done DO BEGIN
		DECLARE accum_value BIGINT UNSIGNED DEFAULT 0;
		DECLARE in_count TINYINT DEFAULT 0;
		DECLARE out_count TINYINT DEFAULT 3;

each_input_char:
		WHILE in_count < 4 DO BEGIN
			DECLARE first_char CHAR(1);
	
			IF LENGTH(input) = 0 THEN
				RETURN ret;
			END IF;
	
			SET first_char = SUBSTRING(input,1,1);
			SET input = SUBSTRING(input,2);
	
			BEGIN
				DECLARE tempval TINYINT UNSIGNED;
				DECLARE error TINYINT DEFAULT 0;
				DECLARE base64_getval CURSOR FOR SELECT val FROM base64_data WHERE c = first_char;
				DECLARE CONTINUE HANDLER FOR SQLSTATE '02000' SET error = 1;
	
				OPEN base64_getval;
				FETCH base64_getval INTO tempval;
				CLOSE base64_getval;

				IF error THEN
					ITERATE each_input_char;
				END IF;

				SET accum_value = (accum_value << 6) + tempval;
			END;

			SET in_count = in_count + 1;

			IF first_char = '=' THEN
				SET done = 1;
				SET out_count = out_count - 1;
			END IF;
		END; END WHILE;

		-- We've now accumulated 24 bits; deaccumulate into bytes

		-- We have to work from the left, so use the third byte position and shift left
		WHILE out_count > 0 DO BEGIN
			SET ret = CONCAT(ret,CHAR((accum_value & 0xff0000) >> 16));
			SET out_count = out_count - 1;
			SET accum_value = (accum_value << 8) & 0xffffff;
		END; END WHILE;
	
	END; END WHILE;

	RETURN ret;
END
;;
DELIMITER ;

-- ----------------------------
-- Function structure for BASE64_ENCODE
-- ----------------------------
DROP FUNCTION IF EXISTS `BASE64_ENCODE`;
DELIMITER ;;
CREATE DEFINER=`YOUR DATABASE`@`%` FUNCTION `BASE64_ENCODE`(input BLOB) RETURNS blob
    DETERMINISTIC
    SQL SECURITY INVOKER
BEGIN
    DECLARE ret BLOB DEFAULT '';
    DECLARE done TINYINT DEFAULT 0;
    IF input IS NULL THEN
        RETURN NULL;
    END IF;
each_block:
    WHILE NOT done DO BEGIN
        DECLARE accum_value BIGINT UNSIGNED DEFAULT 0;
        DECLARE in_count TINYINT DEFAULT 0;
        DECLARE out_count TINYINT;
each_input_char:
        WHILE in_count < 3 DO BEGIN
            DECLARE first_char BLOB(1);

            IF LENGTH(input) = 0 THEN
                SET done = 1;
                SET accum_value = accum_value << (8 * (3 - in_count));
                LEAVE each_input_char;
            END IF;

            SET first_char = SUBSTRING(input,1,1);
            SET input = SUBSTRING(input,2);

            SET accum_value = (accum_value << 8) + ASCII(first_char);
            SET in_count = in_count + 1;
        END; END WHILE;

        -- We've now accumulated 24 bits; deaccumulate into base64 characters
        -- We have to work from the left, so use the third byte position and shift left
        CASE
            WHEN in_count = 3 THEN SET out_count = 4;
            WHEN in_count = 2 THEN SET out_count = 3;
            WHEN in_count = 1 THEN SET out_count = 2;
            ELSE RETURN ret;
        END CASE;

        WHILE out_count > 0 DO BEGIN
            BEGIN
                DECLARE out_char CHAR(1);
                DECLARE base64_getval CURSOR FOR SELECT c FROM base64_data WHERE val = (accum_value >> 18);
                OPEN base64_getval;
                FETCH base64_getval INTO out_char;
                CLOSE base64_getval;
                SET ret = CONCAT(ret,out_char);
                SET out_count = out_count - 1;
                SET accum_value = accum_value << 6 & 0xffffff;
            END;
        END; END WHILE;
        CASE
            WHEN in_count = 2 THEN SET ret = CONCAT(ret,'=');
            WHEN in_count = 1 THEN SET ret = CONCAT(ret,'==');
            ELSE BEGIN END;
        END CASE;

    END; END WHILE;
    RETURN ret;
END
;;
DELIMITER ;
