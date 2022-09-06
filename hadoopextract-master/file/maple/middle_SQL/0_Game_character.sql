-- Maple: middle_data: gamedb_[:tableName]
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    [:cols]
FROM
(
    SELECT
        *
    FROM
        maple_all.gamedb_character
    WHERE
        dt = '[:Date1]'
        AND world = '[:gw]'
)AA
INNER JOIN
-- 篩選該日有上線的
(
    SELECT
        charactername
    FROM
        gtwpd.maple_middle_login_duration_new
    WHERE
        dt = '[:Date0]'
        AND world = '[:gw]'
)BB
ON AA.charactername = BB.charactername
;
