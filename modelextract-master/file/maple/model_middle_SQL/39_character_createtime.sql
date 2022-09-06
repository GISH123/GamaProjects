-- Maple Modeling Middle Data:26_character_createtime.sql
INSERT OVERWRITE DIRECTORY '[:FilePath]'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    characterid ,
    charactername,
    accountid,
    registerdate
FROM
    maple_all.globalaccount_character
WHERE 1=1
    AND dt = [:Date1]
    AND registerdate >= '[:Date-0]'
    AND registerdate < '[:Date-1]'
    AND gameworldid = [:gwid]
;