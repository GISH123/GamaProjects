-- Maple: middle_data: gamedb_[:tableName]
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    [:cols]
FROM
    maple_all.gamedb_[:tableName] AA
WHERE
    dt = '[:Date1]'
    AND world = '[:gw]'
    AND [:timeCol] BETWEEN '[:Date-0]' AND '[:Date-1]'
;