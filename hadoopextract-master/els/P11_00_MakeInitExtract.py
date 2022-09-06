import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
from package.common.database.postgreCtrl import PostgresCtrl
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.sqlTool import SqlTool
from package.common.inputCtrl import inputCtrl
from package.common.sshCtrl import SSHCtrl
from dotenv import load_dotenv
import pandas
import datetime

load_dotenv(dotenv_path="env/hive.env")

hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

def main(parametersData = {}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    gameName = ""
    dbName = ""

    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "makedate":
            makeDateStrArr = parametersData[key]
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]

    if makeDateStrArr == [] and startDateStr == "" and endDateStr == "":
        makeTimeArr = [
            nowZeroTime - datetime.timedelta(days=7)
            , nowZeroTime - datetime.timedelta(days=3)
        ]


    makeTimeArr = []
    if makeDateStrArr != []:
        for makeDateStr in makeDateStrArr:
            makeTimeArr.append(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d"))
    else:
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeTimeArr.append(makeDatetime)
            makeDatetime = makeDatetime + datetime.timedelta(days=1)

    for makeTime in makeTimeArr:
        print("Run [MakeInit] to {}".format(makeTime.strftime("%Y-%m-%d")))
        MakeInitDataMain(makeTime)

def MakeInitDataMain(makeTime):
    databaseInfo = {
        "gameName": "els"
        , "dbName": "init"
        , "tableName": "onlinetime"
        , "insertSqlInit" : getInsertDataSQLByOnlineTime()
        , "partitionedInit": "dt={}"
    }
    MakeInitDataDetail(makeTime, databaseInfo)

    databaseInfo = {
        "gameName": "els"
        , "dbName": "init"
        , "tableName": "onliontimebyaccount"
        , "insertSqlInit": getInsertDataSQLByAccountOnlionTime()
        , "partitionedInit": "dt={}"
    }
    MakeInitDataDetail(makeTime, databaseInfo)

    databaseInfo = {
        "gameName": "els"
        , "dbName": "init"
        , "tableName": "onlinetimebyip"
        , "insertSqlInit": getInsertDataSQLByIPOnlineTime()
        , "partitionedInit": "dt={}"
    }
    MakeInitDataDetail(makeTime, databaseInfo)

def MakeInitDataDetail(makeTime, databaseInfo):
    gameName = databaseInfo["gameName"]
    dbName = databaseInfo["dbName"]
    tableName = databaseInfo["tableName"]

    filePathInit = "/user/hive/warehouse/{}_extract.db/{}/{}/dt={}"
    partitionedInit = databaseInfo["partitionedInit"]
    insertSqlAllInit = databaseInfo["insertSqlInit"]

    tableFullName = "{}_extract.{}_{}".format(gameName, dbName, tableName)
    filePathStr = filePathInit.format(gameName, dbName, tableName, makeTime.strftime("%Y%m%d"))
    partitionedStr = partitionedInit.format(makeTime.strftime("%Y%m%d"))

    insertDataSQLAll = insertSqlAllInit.replace("[:DateLine]", makeTime.strftime("%Y-%m-%d")).replace("[:DateNoLine]", makeTime.strftime("%Y%m%d"))
    insertDataSQLAll = insertDataSQLAll.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr)
    insertDataSQLAll = insertDataSQLAll.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr).replace("[:FilePath]", filePathStr)

    insertDataSQLArr = insertDataSQLAll.split(";")[:-1]
    for insertDataSQL in insertDataSQLArr:
        print(insertDataSQL)
        hiveCtrl.executeSQL_TCByCount(insertDataSQL, 10)

def getCreateTableSQLByAccountOnlionTime():
    return """
        CREATE External TABLE els_extract.init_onliontimebyaccount ( 
            logindate timestamp
            , logoutdate timestamp
            , useruid bigint 
        ) PARTITIONED BY ( dt string ) 
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
        LOCATION '/user/hive/warehouse/els_extract.db/init'
    """

def getInsertDataSQLByAccountOnlionTime():
    return """
        INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/els_extract.db/init/onliontimebyaccount/dt=[:DateNoLine]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
        SELECT 
            DISTINCT 
            CASE WHEN AA.logintime <= '[:DateLine] 00:00:00' THEN '[:DateLine] 00:00:00' ELSE DATE_FORMAT(AA.logintime,'yyyy-MM-dd HH:mm:ss') END as logintime
            , CASE WHEN AA.logouttime >= '[:DateLine] 23:59:59' THEN '[:DateLine] 23:59:59' ELSE DATE_FORMAT(AA.logouttime,'yyyy-MM-dd HH:mm:ss') END as logouttime
            , AA.useruid
        FROM els_all.account_aconnectlog AA
        WHERE 1 = 1 
            AND AA.logouttime >= DATE_ADD('[:DateLine]',0)
            AND AA.logintime < DATE_ADD('[:DateLine]',1)
            AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
            AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd') ; 

        ALTER TABLE [:TableName] DROP IF EXISTS PARTITION ( [:Partitioned] ) ; 
        ALTER TABLE [:TableName] ADD IF NOT EXISTS PARTITION ( [:Partitioned] ) LOCATION '[:FilePath]';
    """

def getCreateTableSQLByIPOnlineTime():
    return """
        CREATE External TABLE els_extract.init_onlinetimebyip ( 
            logindate timestamp
            , logoutdate timestamp
            , useruid bigint
            , ip string
        ) PARTITIONED BY ( dt string ) 
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
        LOCATION '/user/hive/warehouse/els_extract.db/init'
    """

def getInsertDataSQLByIPOnlineTime():
    return """
        INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/els_extract.db/init/onlinetimebyip/dt=[:DateNoLine]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
        SELECT 
            DISTINCT 
            CASE WHEN AA.logintime <= '[:DateLine] 00:00:00' THEN '[:DateLine] 00:00:00' ELSE DATE_FORMAT(AA.logintime,'yyyy-MM-dd HH:mm:ss') END as logintime
            , CASE WHEN AA.logouttime >= '[:DateLine] 23:59:59' THEN '[:DateLine] 23:59:59' ELSE DATE_FORMAT(AA.logouttime,'yyyy-MM-dd HH:mm:ss') END as logouttime
            , AA.useruid
            , AA.ip
        FROM els_all.account_aconnectlog AA
        WHERE 1 = 1 
            AND AA.logouttime >= DATE_ADD('[:DateLine]',0)
            AND AA.logintime < DATE_ADD('[:DateLine]',1)
            AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
            AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd') ; 

        ALTER TABLE [:TableName] DROP IF EXISTS PARTITION ( [:Partitioned] ) ; 
        ALTER TABLE [:TableName] ADD IF NOT EXISTS PARTITION ( [:Partitioned] ) LOCATION '[:FilePath]';
    """

def getCreateTableSQLByOnlineTime():
    return """
        CREATE External TABLE els_extract.init_onlinetime ( 
            logindate timestamp
            , logoutdate timestamp
            , useruid bigint
            , unituid bigint
            , serveruid bigint
            , username string
        ) PARTITIONED BY ( dt string ) 
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
        LOCATION '/user/hive/warehouse/els_extract.db/init'
    """

def getInsertDataSQLByOnlineTime():
    return """
        INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/els_extract.db/init/onlinetime/dt=[:DateNoLine]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
        SELECT 
            DISTINCT 
            CASE WHEN AA.logintime <= '[:DateLine] 00:00:00' THEN '[:DateLine] 00:00:00' ELSE DATE_FORMAT(AA.logintime,'yyyy-MM-dd HH:mm:ss') END as logintime
            , CASE WHEN AA.logouttime >= '[:DateLine] 23:59:59' THEN '[:DateLine] 23:59:59' ELSE DATE_FORMAT(AA.logouttime,'yyyy-MM-dd HH:mm:ss') END as logouttime
            , AA.useruid
            , AA.unituid
            , AA.serveruid
            , AA.nickname as username
        FROM els_all.statistics_lunitconnectlog AA
        WHERE 1 = 1 
            AND AA.logouttime >= DATE_ADD('[:DateLine]',0)
            AND AA.logintime < DATE_ADD('[:DateLine]',1)
            AND (
                    (
                        '[:DateNoLine]' >='20201224' 
                        AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd') 
                        AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
                    )
                OR ('[:DateNoLine]' < '20201224' AND AA.dt >= '20201223' AND AA.dt <= '20201231')
                ); 

        ALTER TABLE [:TableName] DROP IF EXISTS PARTITION ( [:Partitioned] ) ; 
        ALTER TABLE [:TableName] ADD IF NOT EXISTS PARTITION ( [:Partitioned] ) LOCATION '[:FilePath]';
    """

if __name__ == "__main__":
    main()
