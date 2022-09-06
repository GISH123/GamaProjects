INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT
    AA.characterid,
    SUM(CASE WHEN table_name  = 'friend' THEN metadata ELSE 0 END) ,
    SUM(CASE WHEN table_name  = 'album' THEN metadata ELSE 0 END ),
    SUM(CASE WHEN table_name  = 'meso_buy' THEN metadata ELSE 0 END ),
    SUM(CASE WHEN table_name  = 'meso_sell' THEN metadata ELSE 0 END ),
    SUM(CASE WHEN table_name  = 'exp' THEN metadata ELSE 0 END)
FROM
(
	SELECT
		DISTINCT characterid
	FROM
		gtwpd.maple_middle_monthly_login_info
	WHERE
		world = '[:gw]'
        AND dt = '[:Date0]'
)AA
INNER JOIN
(
	SELECT
		characterid,
		'friend' AS table_name,
		count(*) AS metadata
	FROM
		maple_all.gamedb_friend
	WHERE world = '[:gw]'
		AND dt = '[:DateNext]'
	GROUP BY characterid

    UNION ALL

	SELECT
		characterid,
		'album' AS table_name,
		count(*) AS metadata
	FROM
		maple_all.gamedb_beautyalbum
	WHERE world = '[:gw]'
		AND dt = '[:DateNext]'
	GROUP BY characterid

    UNION ALL

	SELECT
		characterid,
		'meso_buy' AS table_name,
		SUM(buy_volumn) AS metadata
	FROM
		gtwpd.maple_middle_mesotrade_buy
	WHERE world = '[:gw]'
		AND dt between '[:Date0]' AND '[:DateN]'
	GROUP BY characterid

    UNION ALL

	SELECT
		characterid,
		'meso_sell' AS table_name,
		SUM(sell_volumn) AS metadata
	FROM
		gtwpd.maple_middle_mesotrade_sell
	WHERE world = '[:gw]'
		AND dt between '[:Date0]' AND '[:DateN]'
	GROUP BY characterid

    UNION ALL

	SELECT
		characterid,
		'exp' AS table_name,
		SUM(exp_gain) AS metadata
	FROM
		gtwpd.maple_middle_exp_increase
	WHERE world = '[:gw]'
		AND dt between '[:Date0]' AND '[:DateN]'
	GROUP BY characterid


)BB
ON AA.characterid = BB.characterid
GROUP BY AA.characterid
;