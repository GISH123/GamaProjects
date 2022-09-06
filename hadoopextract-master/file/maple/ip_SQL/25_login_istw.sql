-- Maple Middle : Login_ISTW
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
	email,
	accountid,
	(serviceaccountid is NOT NULL) AS is_TW
FROM
(
	SELECT
		DISTINCT email, accountid
	FROM
		gtwpd.maple_middle_login_duration_new
	WHERE
		dt = '[:Date0]'
)AA
LEFT JOIN
(
	SELECT
		DISTINCT serviceaccountid
	FROM
		bf_extract.p_serviceaccount
	WHERE 1=1
		AND dt BETWEEN '[:DateN]' AND '[:DateN3]'
		AND servicecode ='610074'
        AND serviceregion = 'T9'
)BB
ON LOWER(AA.email) = LOWER(BB.serviceaccountid);