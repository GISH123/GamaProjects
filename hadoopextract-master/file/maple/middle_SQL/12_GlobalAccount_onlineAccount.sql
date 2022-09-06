-- Maple: middle_data: globalaccount_[:tableName]
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    [:cols]
FROM
(
    SELECT
        *
    FROM
        maple_all.globalaccount_[:tableName]
    WHERE
        dt = '[:Date1]'
)AA
INNER JOIN
-- 篩選該日有上線的
(
    SELECT
        DISTINCT accountid
    FROM
       gtwpd.maple_middle_login_duration_new
    WHERE
        dt = '[:Date0]'
)BB
ON AA.[:timeCol] = BB.accountid
;
