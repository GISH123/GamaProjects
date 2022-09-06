DROP TABLE IF EXISTS gtwpd.maple_iplogdata_temp_[:fileDate];
CREATE EXTERNAL TABLE gtwpd.maple_iplogdata_temp_[:fileDate]  (
    logtime	string,
    type string,
    accountid int,
    email string,
    charactername string,
    ip string,
    gameworldid	int
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n'
LOCATION '/user/hive/warehouse/maple.db/ALL/LogCenter/dt=[:fileDate]/IpLogData' ;

