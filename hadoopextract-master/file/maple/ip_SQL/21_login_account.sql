-- Maple Middle : Login_Account (Before 2021/05/16)
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT
    substr(logtime,1,14) AS logtime,
    split(account,'[\\(\\)]')[0] AS email ,
    split(account,'[\\(\\)]')[1] AS accountid,
	ip ,
	login
FROM
    gtwpd.maple_iplog_account_[:Date0]
WHERE
    substr(logtime,1,8) = '[:Date0]'
;
-- Maple Middle : Login_Account (After 2021/05/17)
INSERT OVERWRITE DIRECTORY '[:FilePath]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ','

SELECT
    logtime,
    split(account,'[\\(\\)]')[0] AS email ,
    split(account,'[\\(\\)]')[1] AS accountid,
	ip ,
	login
FROM
    gtwpd.maple_iplog_account_[:Date0]
WHERE
    logtime between '[:Date0Line]' and '[:Date1Line]'
;