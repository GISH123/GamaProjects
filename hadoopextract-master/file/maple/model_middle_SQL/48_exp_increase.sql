-- Maple Modeling Middle Data:48_exp_increase
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
	characterid,
	DDD.allexp + AAA.exp_i AS exp_i,
	CCC.allexp + AAA.exp_f AS exp_f,
	CCC.allexp + AAA.exp_f - DDD.allexp - AAA.exp_i exp_gain,
	level_i ,
	level_f ,
	level_f - level_i AS level_gain
FROM (
	SELECT
		characterid,
		accountid,
        MAX(exp_i) AS exp_i,
        MAX(level_i) AS level_i,
        MAX(exp_f) AS exp_f,
        MAX(level_f) AS level_f
	FROM(
			SELECT
				characterid,
				accountid,
				0 AS exp_i,
				0 AS level_i,
				s_exp AS exp_f ,
				b_level AS level_f,
				world
			FROM maple_all.gamedb_character
			WHERE dt = '[:Date1]'
				AND world = '[:gw]'
				AND accountid IS NOT NULL
			UNION ALL

			SELECT
				characterid,
				accountid,
				s_exp AS exp_i ,
				b_level as level_i,
				0 AS exp_f,
				0 AS level_f,
				world
            FROM maple_all.gamedb_character
            WHERE dt ='[:Date0]'
                AND world = '[:gw]'
                AND accountid IS NOT NULL
	)AA
	GROUP BY
		world,
		accountid,
		characterid
    HAVING
        level_i != level_f
	    OR exp_i != exp_f
)AAA
INNER JOIN gtwpd.maple_othertable_levelexp CCC
	ON AAA.level_f = CCC.level
INNER JOIN gtwpd.maple_othertable_levelexp DDD
	ON AAA.level_i = DDD.level

;