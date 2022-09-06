-- Maple Modeling Middle Data:46_item_used
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    CharacterID,
    SUM(CASE WHEN usage = 'ATK' THEN 1 ELSE 0 END ) AS I_ATK,
    SUM(CASE WHEN usage = 'DEF' THEN 1 ELSE 0 END ) AS I_DEF,
    SUM(CASE WHEN usage = 'ACC' THEN 1 ELSE 0 END ) AS I_ACC,
    SUM(CASE WHEN usage = 'SPL' THEN 1 ELSE 0 END ) AS I_SPL,
    SUM(CASE WHEN usage = 'PZL' THEN 1 ELSE 0 END ) AS I_PZL,
    SUM(CASE WHEN usage is null THEN 1 ELSE 0 END ) AS I_INV,
	SUM(CASE WHEN is_cash = 1 AND pos >= 0 THEN 1 ELSE 0 END ) AS C_INV,
	SUM(CASE WHEN is_cash = 1 AND pos < 0 THEN 1 ELSE 0 END ) AS C_EQP
	
FROM(
	SELECT characterid,
		AA.pos,
		is_cash,
		BB.usage
	FROM
	(
		SELECT characterid,
			pos,
			MAX(is_cash) AS is_cash
			FROM gtwpd.maple_middle_item_log
			WHERE
				dt = '[:Date0]'
				AND world = '[:gw]'
			GROUP BY characterid, pos
	)AA

	LEFT JOIN gtwpd.maple_otherTable_item_usage BB
	ON AA.pos = BB.pos
)AAA
GROUP BY characterid;