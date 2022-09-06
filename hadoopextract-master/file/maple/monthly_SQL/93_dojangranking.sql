INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    BB.characterid AS characterid,
    BB.accountid AS accountid,
    '[:gw]' AS world,
    '[:Date0]' AS dt
FROM
(
    SELECT
        DISTINCT characterid
    FROM  maple_extract.gamedb_dojangranking
    WHERE
        world = '[:gw]'
        AND dt between '[:Date0]' AND '[:DateN]'
)AA
INNER JOIN
(
    SELECT
   characterid,
   accountid
    FROM  gtwpd.maple_middle_monthly_login_info
    WHERE
        world = '[:gw]'
        AND dt between '[:Date0]' AND '[:DateN]'
)BB
ON AA.characterid = BB.characterid
;

