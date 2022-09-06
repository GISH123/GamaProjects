-- Maple Middle : IPLog (Before 2021/05/16)
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    from_unixtime(unix_timestamp(logtime,'yyyyMMddHHmmss'))	as logtime,
    type,
    accountid,
    email,
    charactername,
    ip,
    gameworldid
FROM
        gtwpd.maple_iplogdata_temp_[:Date0]
WHERE
    gameworldid = [:gwid]
    AND logtime between '[:Date0_0]' and '[:Date1_0]'
;
-- Maple Middle : IPLog (After 2021/05/17)
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    logtime,
    type,
    accountid,
    email,
    charactername,
    ip,
    gameworldid
FROM
        gtwpd.maple_iplogdata_temp_[:Date0]
WHERE
    gameworldid = [:gwid]
    AND logtime between '[:Date0Line]' and '[:Date1Line]'
;
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT
    DISTINCT
    logtime,
    type,
    accountid,
    email,
    charactername,
    ip,
    gameworldid
FROM
        gtwpd.maple_iplogdata_temp_[:Date0]
WHERE
    gameworldid = [:gwid]
    AND logtime between '[:Date0Line]' and '[:Date1Line]'
;