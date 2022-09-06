-- Maple Middle : Login_Mappings
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
	email,
	MAX(accountid) AS accountid,
	SUM(CASE WHEN type = 'iplog' THEN 1 else 0 END) AS in_iplog,
    SUM(CASE WHEN type = 'acclog' THEN 1 else 0 END) AS in_acclog,
    SUM(CASE WHEN type = 'bf' THEN 1 else 0 END) AS in_beanfun
FROM
    (
    SELECT
        DISTINCT LOWER(serviceaccountid) AS email,
        '' AS accountid,
        'bf' AS type
    FROM
        bf_extract.beanfundb_history_play
    WHERE 1=1
        AND dt BETWEEN '[:Date0]' AND '[:Date1]'
        AND servicecode ='610074'
        AND serviceregion = 'T9'

    UNION ALL

    SELECT
        DISTINCT LOWER(email) AS email , accountid,
        'iplog' AS type
    FROM
        gtwpd.maple_iplog
    WHERE
        dt BETWEEN '[:Date0]' AND '[:Date1]'

    UNION ALL

    SELECT
        DISTINCT LOWER(email) AS email , accountid,
        'acclog' AS type
    FROM
        gtwpd.maple_middle_login_account
    WHERE
        dt BETWEEN '[:Date0]' AND '[:Date1]'
    )AA
GROUP BY email
;