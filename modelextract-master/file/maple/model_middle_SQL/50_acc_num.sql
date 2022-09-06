-- Maple Modeling Middle Data:50_acc_num
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT AA.*
FROM
(
    SELECT
        accountid,
        COUNT(*) AS count
    FROM
        maple_all.gamedb_character
    WHERE
        dt = '[:Date1]'
        AND world = '[:gw]'
        AND accountid IS NOT NULL
    GROUP BY accountid
)AA
INNER JOIN
-- 篩選當日有上線者
(
    SELECT
        DISTINCT accountid
    FROM
        gtwpd.maple_middle_login_duration_new
    WHERE
        dt = '[:Date0]'
        AND world = '[:gw]'
)BB
ON AA.accountid = BB.accountid ;

