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
        "gameName": "tdn"
        , "dbName": "init"
        , "tableName": "onliontime"
        , "insertSqlInit" : getInsertDataSQLByCharacterOnlionTime()
        , "partitionedInit": "dt={}"
    }

    MakeInitDataDetail(makeTime, databaseInfo)

    databaseInfo = {
        "gameName": "tdn"
        , "dbName": "init"
        , "tableName": "onliontimebyaccount"
        , "insertSqlInit": getInsertDataSQLByAccountOnlionTime()
        , "partitionedInit": "dt={}"
    }
    MakeInitDataDetail(makeTime, databaseInfo)

    databaseInfo = {
        "gameName": "tdn"
        , "dbName": "init"
        , "tableName": "onliontimebybf"
        , "insertSqlInit": getInsertDataSQLByBFOnlionTime()
        , "partitionedInit": "dt={}"
    }
    MakeInitDataDetail(makeTime, databaseInfo)


def MakeInitDataDetail (makeTime,databaseInfo) :
    dropPartitionCodeInit = "ALTER TABLE [:TableName] DROP IF EXISTS PARTITION ( [:Partitioned] ) "
    alterPartitionCodeInit = "ALTER TABLE [:TableName] ADD IF NOT EXISTS PARTITION ( [:Partitioned] ) LOCATION '[:FilePath]'"
    gameName = databaseInfo["gameName"]
    dbName = databaseInfo["dbName"]
    tableName = databaseInfo["tableName"]
    filePathInit = "/user/hive/warehouse/{}_extract.db/{}/{}/dt={}"
    partitionedInit = databaseInfo["partitionedInit"]
    insertSqlInit = databaseInfo["insertSqlInit"]

    insertDataSQL = insertSqlInit.replace("[:DateLine]",makeTime.strftime("%Y-%m-%d")).replace("[:DateNoLine]",makeTime.strftime("%Y%m%d"))

    try:
        print(insertDataSQL)
        hiveCtrl.executeSQL(insertDataSQL)
    except:
        pass

    tableFullName = "{}_extract.{}_{}".format(gameName,dbName,tableName)
    filePathStr  = filePathInit.format(gameName,dbName,tableName,makeTime.strftime("%Y%m%d"))
    partitionedStr = partitionedInit.format(makeTime.strftime("%Y%m%d"))

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

def getCreateTableSQLByCharacterOnlionTime():
    return """
        CREATE External TABLE tdn_extract.init_onliontime ( 
            logindate timestamp
            , logoutdate timestamp
            , characterid bigint 
        ) PARTITIONED BY ( dt string ) 
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
        LOCATION '/user/hive/warehouse/tdn_extract.db/init'  
    """

def getCreateTableSQLByAccountOnlionTime():
    return """
        CREATE External TABLE tdn_extract.init_onliontimebyaccount ( 
            logindate timestamp
            , logoutdate timestamp
            , accountid bigint 
        ) PARTITIONED BY ( dt string ) 
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
        LOCATION '/user/hive/warehouse/tdn_extract.db/init'
    """

def getCreateTableSQLByAccountOnlionTime():
    return """
        CREATE External TABLE tdn_extract.init_onliontimebybf ( 
            logindate timestamp
            , logoutdate timestamp
            , mainaccountid string 
            , serviceaccountid string 
            , ipaddress string
        ) PARTITIONED BY ( dt string ) 
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
        LOCATION '/user/hive/warehouse/tdn_extract.db/init'
    """

def getInsertDataSQLByCharacterOnlionTime():
    return """
        INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/tdn_extract.db/init/onliontime/dt=[:DateNoLine]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
        SELECT 
            CASE WHEN AA.logindate <= '[:DateLine] 00:00:00' THEN '[:DateLine] 00:00:00' ELSE DATE_FORMAT(AA.logindate,'yyyy-MM-dd HH:mm:ss') END as logindate
            , CASE WHEN AA.logoutdate >= '[:DateLine] 23:59:59' THEN '[:DateLine] 23:59:59' ELSE DATE_FORMAT(AA.logoutdate,'yyyy-MM-dd HH:mm:ss') END as logoutdate
            , AA.characterid
        FROM tdn_all.dnstaging_characterlogoutlogs AA
        WHERE 1 = 1 
            AND AA.logoutdate >= DATE_ADD('[:DateLine]',0)
            AND AA.logindate < DATE_ADD('[:DateLine]',1)
            AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-5),'yyyyMMdd')
            AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
    """

def getInsertDataSQLByAccountOnlionTime():
    return """
        INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/tdn_extract.db/init/onliontimebyaccount/dt=[:DateNoLine]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
        SELECT 
            CASE WHEN AA.logindate <=  '[:DateLine] 00:00:00' THEN '[:DateLine] 00:00:00' ELSE DATE_FORMAT(AA.logindate,'yyyy-MM-dd HH:mm:ss') END as logindate
            , CASE WHEN AA.logoutdate >= '[:DateLine] 23:59:59' THEN '[:DateLine] 23:59:59' ELSE DATE_FORMAT(AA.logoutdate,'yyyy-MM-dd HH:mm:ss') END as logoutdate
            , AA.accountid
        FROM tdn_all.dnstaging_accountlogoutlogs AA
        WHERE 1 = 1 
            AND AA.logoutdate >= DATE_ADD('[:DateLine]',0)
            AND AA.logindate < DATE_ADD('[:DateLine]',1)
            AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',-5),'yyyyMMdd')
            AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
    """

def getInsertDataSQLByBFOnlionTime():
    return """
        INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/tdn_extract.db/init/onliontimebybf/dt=[:DateNoLine]' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
        SELECT 
            CASE WHEN AA.logintime <=  '[:DateLine] 00:00:00' THEN '[:DateLine] 00:00:00' ELSE DATE_FORMAT(AA.logintime,'yyyy-MM-dd HH:mm:ss') END as logindate
            , CASE WHEN AA.logouttime >= '[:DateLine] 23:59:59' THEN '[:DateLine] 23:59:59' ELSE DATE_FORMAT(AA.logouttime,'yyyy-MM-dd HH:mm:ss') END as logoutdate
            , AA.mainaccountid
            , lower(AA.serviceaccountid) as serviceaccountid
            , AA.ipaddress 
        FROM bf_extract.beanfundb_history_play AA
        WHERE 1 = 1 
            AND AA.servicecode = '611653' 
            AND AA.logouttime >= DATE_ADD('[:DateLine]',0)
            AND AA.logintime < DATE_ADD('[:DateLine]',1)
            AND AA.dt >= DATE_FORMAT(DATE_ADD('[:DateLine]',0),'yyyyMMdd')
            AND AA.dt <= DATE_FORMAT(DATE_ADD('[:DateLine]',7),'yyyyMMdd')
    """


if __name__ == "__main__":
    main()



