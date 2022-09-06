INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    [:cols]
FROM
    maple_all.gamedb_[:tableName]
WHERE
    dt = '[:Date0]'
    AND world = '[:gw]'
;
