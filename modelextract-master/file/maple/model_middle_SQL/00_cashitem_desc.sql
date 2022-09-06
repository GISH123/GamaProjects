-- Maple Middle : cashitem_desc
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
	AA.commodityid AS commodityid,
	BB.serviceaccountid IS NOT NULL AS is_tw,
	is_gift,
	number,
	SUM(qty) AS qty,
	SUM(total_price) AS total_price,
	COUNT(AA.serviceaccountid) AS aid_count
FROM
(
	SELECT
		LOWER(nexonclubid) AS serviceaccountid,
		-- actionid 1買, 2 or 9 送禮
		NOT actionid = 1 is_gift,
		commodityid,
		number,
		COUNT(*) AS qty,
		SUM(price) total_price
	FROM
		maple_extract.gamedb_cashitemlog
	WHERE 1=1
		AND dt = '[:Date0]'
		AND actionid IN (1,2,9)
	GROUP BY commodityid ,number,LOWER(nexonclubid),(NOT actionid = 1)
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
GROUP BY commodityid,number, BB.serviceaccountid IS NOT NULL, is_gift
;

