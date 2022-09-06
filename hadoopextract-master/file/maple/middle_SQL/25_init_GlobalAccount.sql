INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    [:cols]
FROM
    maple_all.globalaccount_[:tableName]
WHERE
    dt = '[:Date0]'
;
