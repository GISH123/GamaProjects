-- Maple: middle_data: globalaccount_[:tableName]
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    [:cols]
FROM
    maple_all.globalaccount_[:tableName] AA
WHERE
    dt = '[:Date1]'
    AND [:timeCol] BETWEEN '[:Date-0]' AND '[:Date-1]'
;