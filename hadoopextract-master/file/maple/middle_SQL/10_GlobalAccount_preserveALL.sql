-- Maple: middle_data: globalaccount_[:tableName]
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    [:cols]
FROM
    maple_all.globalaccount_[:tableName] AA
WHERE
    dt = '[:Date1]'
;
