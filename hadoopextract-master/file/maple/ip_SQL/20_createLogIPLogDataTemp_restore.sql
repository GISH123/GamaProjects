DROP TABLE IF EXISTS gtwpd.maple_iplog_account_[:Date0] ;
CREATE EXTERNAL TABLE IF NOT EXISTS gtwpd.maple_iplog_account_[:Date0] (
	logtime string,
	account string,
	ip string,
	login int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/hive/warehouse/maple.db/ALL/Log/LogCenter/dt=[:fileDate]/LoginIpLogData' ;
