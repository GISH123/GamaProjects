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
        AND [:gwcol] = '[:gwInt]'
)AA
INNER JOIN
-- 篩選該日有上線的
(
    SELECT
        characterid
    FROM
        maple_extract.gamedb_character
    WHERE
        dt = '[:Date0]'
        AND world = '[:gw]'
)BB
ON AA.characterid = BB.characterid
;
