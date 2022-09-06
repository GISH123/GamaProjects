import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hiveCtrl_2 import HiveSingleCtrl
from package.common.telegramCtrl import TelegramCtrl
from dotenv import load_dotenv
import pandas, time
import datetime

load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/Telegram_GAA.env")

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

telegramCtrl = TelegramCtrl()
startTime = time.time()

logTableArr = [
    ""
]

def MakeInitDataMain():
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - datetime.timedelta(days=4)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    # startDateStr = "2020-08-01"
    # endDateStr = "2020-08-18"
    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime

    while makeTime <= endDateTime:
        # account list
        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "init"
            , "tableName": "activeaccount"
            , "insertSqlInit": getInsertDataSQLByAccountOnlionTime()
            , "partitionedInit": "dt={} , world = {}"
            , "gameServer" : ["00", "01", "02", "03", "04", "05"]
        }
        MakeInitDataDetail(makeTime, databaseInfo)

        makeTime = makeTime + datetime.timedelta(days=1)


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
            hiveCtrl.executeSQL(insertDataSQL)
        except:
            print(insertDataSQL)
            print("insertData failed")

        tableFullName = "{}_extract.{}_{}".format(gameName,dbName,tableName)
        filePathStr  = filePathInit.format(gameName,dbName,tableName,makeTime.strftime("%Y%m%d"),world)
        partitionedStr = partitionedInit.format(makeTime.strftime("%Y%m%d"),world)

        dropPartitionCode = dropPartitionCodeInit.replace("[:TableName]",tableFullName).replace("[:Partitioned]",partitionedStr)
        alterPartitionCode = alterPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr).replace("[:FilePath]", filePathStr)

        try:
            print(dropPartitionCode)
            hiveCtrl.executeSQL(dropPartitionCode)
            print("Info:Table Partition is delete")
        except:
            print("Info:Table Partition is error")
        try:
            print(alterPartitionCode)
            hiveCtrl.executeSQL(alterPartitionCode)
            print("Info:Table Partition is alter")
        except:
            print("Info:Table Partition is error")

def getCreateTableSQLByAccountOnlionTime():
    return """
        CREATE External TABLE bnb_extract.init_activeaccount ( 
            last_connect string
            , id string
            , user_name string
        ) PARTITIONED BY ( dt string, world string) 
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n' 
        LOCATION '/user/hive/warehouse/bnb_extract.db/init'
    """

def getInsertDataSQLByAccountOnlionTime():
    return """
        INSERT OVERWRITE DIRECTORY '/user/hive/warehouse/bnb_extract.db/init/activeaccount/dt=[:DateNoLine]/world=[:World]/' ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
        SELECT DISTINCT
             AA.lastconn as last_connect
             ,AA.id
             ,AA.name as user_name
        FROM bnb_all.userinfo_fxuseraccount AA
        WHERE 1 = 1 
        AND AA.id is not null
        AND AA.lastconn is not null
        AND AA.dt = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')
        AND world = [:World]
        AND substr(AA.lastconn, 0 , 10) >= DATE_SUB('[:DateLine]', 7)
    """


if __name__ == "__main__":
    print('start 11_00_MakeInitExtract')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    MakeInitDataMain()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))



