-- Maple Middle : cash_acc
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
	AA.serviceaccountid AS serviceaccountid, 
	BB.serviceaccountid IS NOT NULL AS is_tw,
	total_price
FROM
(
	SELECT
		LOWER(nexonclubid) AS serviceaccountid,
		SUM(price) total_price
	FROM
		maple_extract.gamedb_cashitemlog
	WHERE 1=1
		AND dt = '[:Date0]'
		AND actionid IN (1,2,9)
	GROUP BY LOWER(nexonclubid)
)AA
LEFT JOIN
(
	SELECT
		DISTINCT LOWER(serviceaccountid) AS serviceaccountid
	FROM
		bf_extract.p_serviceaccount
	WHERE 1=1
		AND dt BETWEEN '[:DateN]' AND '[:DateN3]'
		AND servicecode ='610074'
        AND serviceregion = 'T9'
)BB
ON AA.serviceaccountid = BB.serviceaccountid
;

