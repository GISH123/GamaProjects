-- Maple Modeling Middle Data:49_money_increase
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
	characterid,
	money_i,
	money_f,
	money_f - money_i AS money_diff
FROM (
	SELECT 	AA.characterid,
	    COALESCE (money_i,0) AS money_i,
	    money_f
	FROM(
		SELECT characterid,
			s_money as money_f
		FROM maple_all.gamedb_character
		WHERE dt = '[:Date1]'
            AND world = '[:gw]'
            AND accountid IS NOT NULL
		)AA
	INNER JOIN (
		SELECT characterid,
            s_money as money_i
		FROM maple_all.gamedb_character
		WHERE dt = '[:Date0]'
            AND world = '[:gw]'
            AND accountid IS NOT NULL
		)BB
	ON AA.characterid = BB.characterid
    WHERE money_i != money_f
)A
ORDER BY characterid;



