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
    startDateTime = (nowZeroTime - datetime.timedelta(days=14)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
    endDateTime = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr

    makeTimeArr = []
    if makeDateStrArr != []:
        for makeDateStr in makeDateStrArr:
            makeTimeArr.append(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d"))
    else:
        startDateTime = datetime.datetime.strptime(startDateTime, "%Y-%m-%d")
        endDateTime = datetime.datetime.strptime(endDateTime, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeTimeArr.append(makeDatetime)
            makeDatetime = makeDatetime + datetime.timedelta(days=1)

    for makeTime in makeTimeArr:
        print("Run [MakeInit] to {}".format(makeTime.strftime("%Y-%m-%d")))
        MakeInitDataMain(makeTime)

def MakeInitDataMain(makeTime):
        databaseInfo = {
            "gameName": "cso"
            , "dbName": "init"
            , "tableName": "loginlogout"
            , "insertSqlInit": getInsertDataSQLByLoginLogout()
            , "partitionedInit": "dt={}"
            , "gameServer": [""]
        }
        MakeInitDataDetail(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "cso"
            , "dbName": "init"
            , "tableName": "onlinetime"
            , "insertSqlInit": getInsertDataSQLByOnlineTime()
            , "partitionedInit": "dt={}"
            , "gameServer": [""]
        }
        MakeInitDataDetail(makeTime, databaseInfo)


def MakeInitDataDetail (makeTime,databaseInfo) :
    dropPartitionCodeInit = "ALTER TABLE [:TableName] DROP IF EXISTS PARTITION ( [:Partitioned] ) "
    alterPartitionCodeInit = "ALTER TABLE [:TableName] ADD IF NOT EXISTS PARTITION ( [:Partitioned] ) LOCATION '[:FilePath]'"
    gameName = databaseInfo["gameName"]
    dbName = databaseInfo["dbName"]
    tableName = databaseInfo["tableName"]
    if len(databaseInfo["gameServer"]) == 1:
        filePathInit = "/user/hive/warehouse/{}_extract.db/{}/{}/dt={}{}"
    else:
        filePathInit = "/user/hive/warehouse/{}_extract.db/{}/{}/dt={}/world={}"
    partitionedInit = databaseInfo["partitionedInit"]
    insertSqlInit = databaseInfo["insertSqlInit"]
    gameServer = databaseInfo["gameServer"]


    for world in gameServer :
        insertDataSQL = insertSqlInit.replace("[:DateLine]",makeTime.strftime("%Y-%m-%d")).replace("[:DateNoLine]",makeTime.strftime("%Y%m%d"))
        insertDataSQL = insertDataSQL.replace("[:World]", world)

        try:
            #print(insertDataSQL)
            hiveCtrl.executeSQL_TCByCount(insertDataSQL,3)
        except:
            print("insertData failed")

        tableFullName = "{}_extract.{}_{}".format(gameName,dbName,tableName)
        filePathStr  = filePathInit.format(gameName,dbName,tableName,makeTime.strftime("%Y%m%d"),world)
        partitionedStr = partitionedInit.format(makeTime.strftime("%Y%m%d"),world)

        dropPartitionCode = dropPartitionCodeInit.replace("[:TableName]",tableFullName).replace("[:Partitioned]",partitionedStr)
        alterPartitionCode = alterPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr).replace("[:FilePath]", filePathStr)

        try:
            print(dropPartitionCode)
            hiveCtrl.executeSQL(dropPartitionCode)
            print("Info:Table Partition is exist , Do delete")
        except:
            print("Info:Table Partition is not exist")
        try:
            print(alterPartitionCode)
            hiveCtrl.executeSQL(alterPartitionCode)
            print("Info:Table Partition is alter")
        except:
            print("Info:Table Partition is exist")


def getCreateTableSQLByLoginLogout():
    return """
        CREATE External TABLE cso_extract.init_loginlogout ( 
            logtime timestamp
            , account_id string 
            , ip string
            , logtype string
        ) PARTITIONED BY ( dt string ) 
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
        LOCATION '/user/hive/warehouse/cso_extract.db/init'
    """


def getInsertDataSQLByLoginLogout():
    return """
        INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/cso_extract.db/init/loginlogout/dt=[:DateNoLine]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
        SELECT 
            logtime
            , account_id
            , ip
            , logtype
        FROM (
            SELECT DISTINCT
                 logintime as logtime
                 , id as account_id
                 , ip
                 , 'login' as logtype
            FROM cso_all.cso_log_bd_log_login AA
            WHERE 1 = 1 
            AND AA.logintime >= date_format(DATE_SUB('[:DateLine]',0), 'yyyy-MM-dd HH:mm:ss')
            AND AA.logintime < date_format(DATE_ADD('[:DateLine]', 1), 'yyyy-MM-dd HH:mm:ss')
            AND (
                    (
                        '[:DateNoLine]' >='20200909' 
                        AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd') 
                        AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
                    )
                OR ('[:DateNoLine]' >= '20200901' '[:DateNoLine]' < '20200909' AND AA.dt = '20200909')
                OR ('[:DateNoLine]' < '20200901' AND AA.dt = '20200831')
                )

            union all
            
            SELECT DISTINCT
                logouttime as logtime
                , id as account_id
                , ip
                , 'logout' as logtype
            FROM cso_all.cso_log_bd_log_logout AA
            WHERE 1 = 1 
            AND AA.logouttime >= date_format(DATE_SUB('[:DateLine]',0), 'yyyy-MM-dd HH:mm:ss')
            AND AA.logouttime < date_format(DATE_ADD('[:DateLine]', 1), 'yyyy-MM-dd HH:mm:ss')
            AND (
                    (
                        '[:DateNoLine]' >='20200909' 
                        AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd') 
                        AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
                    )
                OR ('[:DateNoLine]' >= '20200901' '[:DateNoLine]' < '20200909' AND AA.dt = '20200909')
                OR ('[:DateNoLine]' < '20200901' AND AA.dt = '20200831')
                )
        ) TMP
        WHERE logtime is not null
        GROUP BY 
            logtime
            , account_id
            , ip
            , logtype
        ORDER BY logtime
    """

#-----------------------------------------

def getCreateTableSQLByOnlineTime():
    return """
        CREATE External TABLE cso_extract.init_onlinetime ( 
            account_id string
            , ip string
            , login_time string
            , logout_time string
            , playtime bigint
        ) PARTITIONED BY ( dt string ) 
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
        LOCATION '/user/hive/warehouse/cso_extract.db/init'
    """

def getInsertDataSQLByOnlineTime():
    return """
        WITH basicdata AS (
            SELECT account_id,
            AA.ip,
            AA.logtime,
            AA.logtype,
            LAG(AA.logtime) OVER (PARTITION BY AA.account_id ORDER BY AA.logtime) AS login_time
            FROM cso_extract.init_loginlogout AA
            WHERE 1 = 1
            AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',14),'yyyyMMdd')
            AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-14),'yyyyMMdd')
         )
        INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/cso_extract.db/init/onlinetime/dt=[:DateNoLine]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
        SELECT AAA.account_id,
               AAA.ip,
               AAA.login_time,
               AAA.logtime AS logout_time,
               unix_timestamp(logtime,'yyyy-MM-dd HH:mm:ss') - unix_timestamp(login_time,'yyyy-MM-dd HH:mm:ss') as playtime
        FROM basicdata AAA
        WHERE 1 = 1
        AND AAA.logtype = 'logout'
        AND AAA.logtime >= DATE_ADD('[:DateLine]',0)
        AND AAA.login_time < DATE_ADD('[:DateLine]',1)
    """


if __name__ == "__main__":
    main()



