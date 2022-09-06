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
import time

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
    tableNameArray = []  # 11_10 階段 如何重作中繼站資料

    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "makedate":
            makeDateStrArr = parametersData[key]
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]
        if key == "tableNameArray":
            tableNameArray = parametersData[key]

    makeTimeArr = []
    if makeDateStrArr == [] and startDateStr == "" and endDateStr == "":
        makeTimeArr = [
            nowZeroTime - datetime.timedelta(days=7)
            , nowZeroTime - datetime.timedelta(days=2)
        ]
    elif makeDateStrArr != [] :
        for makeDateStr in makeDateStrArr:
            makeTimeArr.append(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d"))
    else :
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeTimeArr.append(makeDatetime)
            makeDatetime = makeDatetime + datetime.timedelta(days=1)
    makeInfo = {
        "tableNameArray" : tableNameArray
    }
    for makeTime in makeTimeArr:
        print("Run [MakeInit] to {}".format(makeTime.strftime("%Y-%m-%d")))
        makeInfo["makeTime"] = makeTime
        MakeInitDataMain(makeInfo)

def MakeInitDataMain(makeInfo):
    makeTime = makeInfo["makeTime"]
    tableNameArray = makeInfo["tableNameArray"]
    databaseInfo = {
        "gameName": "wod"
        , "dbName": "init"
        , "tableName": "onliontime"
        , "insertSqlInit" : getInsertDataSQLByCharacterOnlionTime()
        , "partitionedInit": "dt={}"
    }
    if "onliontime" in tableNameArray or tableNameArray == []:
        MakeInitDataDetail(makeTime, databaseInfo)

    databaseInfo = {
        "gameName": "wod"
        , "dbName": "init"
        , "tableName": "onliontimebyaccount"
        , "insertSqlInit": getInsertDataSQLByAccountOnlionTime()
        , "partitionedInit": "dt={}"
    }
    if "onliontimebyaccount" in tableNameArray or tableNameArray == []:
        MakeInitDataDetail(makeTime, databaseInfo)

def MakeInitDataDetail (makeTime,databaseInfo) :
    insertSqlInit = databaseInfo["insertSqlInit"]
    insertDataSQL = insertSqlInit.replace("[:DateLine]",makeTime.strftime("%Y-%m-%d")).replace("[:DateNoLine]",makeTime.strftime("%Y%m%d"))
    try:
        print(insertDataSQL)
        hiveCtrl.executeSQL(insertDataSQL)
    except:
        print("error")

def getCreateTableSQLByCharacterOnlionTime():
    return """
        CREATE External TABLE wod_extract.init_onliontime ( 
            worldid string
            , logintime timestamp
            , logouttime timestamp
            , logincount bigint
            , logoutcount bigint
            , accountid string
            , platformid string
            , charid string 
            , charname string
            , playertype string
            , accessip string
        ) PARTITIONED BY ( dt string ) 
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
        LOCATION '/user/hive/warehouse/wod_extract.db/init/onliontime'	
    """

def getCreateTableSQLByAccountOnlionTime():
    return """
        CREATE External TABLE wod_extract.init_onliontimebyaccount ( 
            worldid string
            , logintime timestamp
            , logouttime timestamp
            , logincount bigint
            , logoutcount bigint
            , accountid string
            , platformid string
            , playertype string
            , accessip string
        ) PARTITIONED BY ( dt string ) 
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
        LOCATION '/user/hive/warehouse/wod_extract.db/init/onliontimebyaccount'
    """

def getInsertDataSQLByCharacterOnlionTime():
    return """
        INSERT OVERWRITE TABLE wod_extract.init_onliontime PARTITION(dt='[:DateNoLine]')
        SELECT 
            AAAA.worldid
            , CASE WHEN AAAA.logintime <= '[:DateLine] 00:00:00' THEN '[:DateLine] 00:00:00' ELSE DATE_FORMAT(AAAA.logintime,'yyyy-MM-dd HH:mm:ss') END as logintime
            , CASE WHEN AAAA.logouttime >= '[:DateLine] 23:59:59' THEN '[:DateLine] 23:59:59' ELSE DATE_FORMAT(AAAA.logouttime,'yyyy-MM-dd HH:mm:ss') END as logouttime
            , CASE WHEN AAAA.logintime <= '[:DateLine] 00:00:00' THEN 0 ELSE 1 END as logincount
            , CASE WHEN AAAA.logouttime >= '[:DateLine] 23:59:59' THEN 0 ELSE 1 END as logoutcount
            , AAAA.accountno as accountno 
            , AAAA.accountname
            , AAAA.charid
            , AAAA.charname
            , AAAA.playertype
            , AAAA.accessip
        FROM (
            SELECT
                BBB.TYPE AS logintype
                , regexp_replace(BBB.createdate,'T',' ') AS logintime
                , BBB.accountno as accountno 
                , BBB.accountname AS accountname
                , BBB.charid
                , BBB.charname
                , BBB.playertype
                , BBB.worldid
                , BBB.accessip
                , lead(BBB.TYPE) OVER(PARTITION BY BBB.accountname ORDER BY regexp_replace(BBB.createdate,'T',' ')) as logouttype
                , lead(regexp_replace(BBB.createdate,'T',' ')) OVER(PARTITION BY BBB.accountname ORDER BY regexp_replace(BBB.createdate,'T',' ')) as logouttime
            FROM (
                SELECT
                    AA.*
                FROM wod_all.topics_user_accessdisconnect_s AA
                WHERE 1 = 1
                    AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-7),'yyyyMMdd')
                    AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
                UNION ALL
                SELECT
                    AA.*
                FROM wod_all.topics_user_accessconnect_s AA
                WHERE 1 = 1
                    AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-7),'yyyyMMdd')
                    AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
                UNION ALL
                SELECT
                    AA.*
                FROM wod_all.topics_char_accessdisconnect AA
                WHERE 1 = 1
                    AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-7),'yyyyMMdd')
                    AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
                UNION ALL
                SELECT
                    AA.*
                FROM wod_all.topics_char_accessconnect AA
                WHERE 1 = 1
                    AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-7),'yyyyMMdd')
                    AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
            ) AAA
            LATERAL VIEW JSON_TUPLE ( AAA.DATA
                , 'type'
                , 'createdate'
                , 'accountno'
                , 'accountname'
                , 'charid'
                , 'charname'
                , 'playertype'
                , 'worldid'
                , 'accessip'
            ) BBB AS
                type
                , createdate
                , accountno
                , accountname
                , charid
                , charname
                , playertype
                , worldid
                , accessip
            WHERE 1 = 1
        ) AAAA
        WHERE 1 = 1
            AND AAAA.logintype = 'Char_AccessConnect'
            AND AAAA.logouttype in ('Char_AccessDisconnect','User_AccessConnect')
            AND AAAA.logintime < DATE_ADD('[:DateLine]',1)
            AND AAAA.logouttime >= DATE_ADD('[:DateLine]',0)
        ORDER BY
            logintime
    """

def getInsertDataSQLByAccountOnlionTime():
    return """
        INSERT OVERWRITE TABLE wod_extract.init_onliontimebyaccount PARTITION(dt='[:DateNoLine]')
        SELECT
            AAAA.worldid
            , CASE WHEN AAAA.logintime <= '[:DateLine] 00:00:00' THEN '[:DateLine] 00:00:00' ELSE DATE_FORMAT(AAAA.logintime,'yyyy-MM-dd HH:mm:ss') END as logintime
            , CASE WHEN AAAA.logouttime >= '[:DateLine] 23:59:59' THEN '[:DateLine] 23:59:59' ELSE DATE_FORMAT(AAAA.logouttime,'yyyy-MM-dd HH:mm:ss') END as logouttime
            , CASE WHEN AAAA.logintime <= '[:DateLine] 00:00:00' THEN 0 ELSE 1 END as logincount
            , CASE WHEN AAAA.logouttime >= '[:DateLine] 23:59:59' THEN 0 ELSE 1 END as logoutcount
            , AAAA.accountno as accountno 
            , AAAA.accountname
            , AAAA.playertype
            , AAAA.accessip
        FROM (
            SELECT
                BBB.TYPE AS logintype
                , regexp_replace(BBB.createdate,'T',' ') AS logintime
                , BBB.accountno as accountno 
                , BBB.accountname AS accountname
                , BBB.playertype
                , BBB.worldid
                , BBB.accessip
                , lead(BBB.TYPE) OVER(PARTITION BY BBB.accountname ORDER BY regexp_replace(BBB.createdate,'T',' ')) as logouttype
                , lead(regexp_replace(BBB.createdate,'T',' ')) OVER(PARTITION BY BBB.accountname ORDER BY regexp_replace(BBB.createdate,'T',' ')) as logouttime
            FROM (
                SELECT
                    AA.*
                FROM wod_all.topics_user_accessdisconnect_s AA
                WHERE 1 = 1
                    AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-7),'yyyyMMdd')
                    AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
                UNION ALL
                SELECT
                    AA.*
                FROM wod_all.topics_user_accessconnect_s AA
                WHERE 1 = 1
                    AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-7),'yyyyMMdd')
                    AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
                UNION ALL
                SELECT
                    AA.*
                FROM wod_all.topics_char_accessdisconnect AA
                WHERE 1 = 1
                    AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-7),'yyyyMMdd')
                    AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
                UNION ALL
                SELECT
                    AA.*
                FROM wod_all.topics_char_accessconnect AA
                WHERE 1 = 1
                    AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-7),'yyyyMMdd')
                    AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
            ) AAA
            LATERAL VIEW JSON_TUPLE ( AAA.DATA
                , 'type'
                , 'createdate'
                , 'accountno'
                , 'accountname'
                , 'playertype'
                , 'worldid'
                , 'accessip'
            ) BBB AS
                type
                , createdate
                , accountno
                , accountname
                , playertype
                , worldid
                , accessip
            WHERE 1 = 1
        ) AAAA
        WHERE 1 = 1
            AND AAAA.logintype = 'User_AccessConnect'
            AND AAAA.logouttype = 'User_AccessDisconnect'
            AND AAAA.logintime < DATE_ADD('[:DateLine]',1)
            AND AAAA.logouttime >= DATE_ADD('[:DateLine]',0)
        ORDER BY
            logintime 
    """

if __name__ == "__main__":
    main()




