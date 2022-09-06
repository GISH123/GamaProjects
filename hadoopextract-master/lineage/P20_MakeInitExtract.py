import os, sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.inputCtrl import inputCtrl
from package.common.telegramCtrl import TelegramCtrl
from package.common.extracth.extracthCtrl_2_1_0 import ExtracthCtrl
from dotenv import load_dotenv
import pandas
import datetime
import time

load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/PostgreSQL.env")
load_dotenv(dotenv_path="env/Telegram_GAA.env")

hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST2")
    , port=int(os.getenv("HIVE_PORT2"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

extracthCtrl = ExtracthCtrl()

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

telegramCtrl = TelegramCtrl()
startTime = time.time()


def main(parametersData = {}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    gameName = ""
    dataBaseArr = []
    tableNameArr = []

    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "makedate":
            makeDateStrArr = parametersData[key]
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]
        if key == "gamename":
            gameName = parametersData[key][0]
        if key == "dataBaseArr":
            dataBaseArr = parametersData[key]
        if key == "tableNameArr":
            tableNameArr = parametersData[key]

    if makeDateStrArr == []:
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameName = "lineage" if gameName == "" else gameName
    dataBaseArr = ["init"] if dataBaseArr == [] else dataBaseArr
    tableNameArr = ["lineage_onliontimebyaccount", "lineage_onliontimebycharacter"] if tableNameArr == [] else tableNameArr
    tableInfoArr = [
        # ["ExtractTable, SourceTable, ColumnsType, Columns, LoginTimeColumnName, LogoutTimeColumnName]
        ["lineage_onliontimebyaccount", "lindb_user_account"
            , "lastlogin timestamp, lastlogout timestamp, account string"
            , "lastlogin, lastlogout, account", "lastlogin", "lastlogout"]
        , ["lineage_onliontimebycharacter", "linworld_user_town"
            , "enter_date timestamp, leave_date timestamp, user_id string, uid bigint, world string"
            , "enter_date, leave_date, user_id, uid, world", "enter_date", "leave_date"]
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
        dateLine = makeTime.strftime("%Y-%m-%d")
        # 建立共用DB Extract
        if dataBaseArr != []:
            for dataBase in dataBaseArr:
                databaseInfo = {
                    "gameName": gameName.lower()
                    , "dbName": dataBase.lower()
                    , "worldNameArr": [""]
                    , "hiveDB": "extract"
                    , "tableNameArr": tableNameArr
                    , "tableInfoArr": tableInfoArr
                    , "partitionedInit": "dt string"
                    , "filePathInit": f"/user/hive/warehouse/{gameName.lower()}_[:hiveDB].db/{dataBase.lower()}/[:tableName]"
                    , "createSQLInit": createTableSQL()
                    , "partitionedAlterInit": "dt='[:dateNoLine]'"
                    , "insertSQLInit": InsertDataSQL()
                }
                print(f"Insert {gameName} {dataBase} Init {dateLine} Data")
                startRunTime = time.time()
                MakeInitDataDetail(makeTime, databaseInfo)
                # chkInsertData(makeTime, databaseInfo)
                print(f"Insert {dataBase} Init {dateLine} Data Total Used {time.time() - startRunTime} seconds.")

    print(f"Insert {gameName} HadoopExtract Data Total Used ", time.time() - startTime, "seconds.")


def MakeInitDataDetail (makeTime, databaseInfo):
    gameName = databaseInfo["gameName"]
    dbName = databaseInfo["dbName"]
    hiveDB = databaseInfo["hiveDB"]
    tableNameArr = databaseInfo["tableNameArr"]
    tableInfoArr = databaseInfo["tableInfoArr"]
    partitioned = databaseInfo["partitionedInit"]
    filePathInit = databaseInfo["filePathInit"]
    createSQLInit = databaseInfo["createSQLInit"]
    partitionedAlter = databaseInfo["partitionedAlterInit"]
    insertSQLInit = databaseInfo["insertSQLInit"]
    dateNoLine = makeTime.strftime("%Y%m%d")
    dateLine = makeTime.strftime("%Y-%m-%d")

    createSQLArr = []
    insertSQLArr = []
    for tableName in tableNameArr:
        fullExtractTableName = f"{gameName}_{hiveDB}.{dbName}_{tableName}"
        filePath = filePathInit.replace("[:hiveDB]", hiveDB).replace("[:tableName]", tableName)
        createSQL = createSQLInit.replace("[:fullExtractTableName]", fullExtractTableName)
        createSQL = createSQL.replace("[:partitioned]", partitioned).replace("[:filePath]", filePath)
        insertSQL = insertSQLInit.replace("[:fullExtractTableName]", fullExtractTableName)
        insertSQL = insertSQL.replace("[:PartitionedAlter]", partitionedAlter).replace("[:dateNoLine]", dateNoLine)
        insertSQL = insertSQL.replace("[:gameName]", gameName).replace("[:dateLine]", dateLine)
        for columns in tableInfoArr:
            if tableName == columns[0]:
                createSQL = createSQL.replace("[:colunmsType]", columns[2])
                insertSQL = insertSQL.replace("[:sourceTable]", columns[1]).replace("[:columns]", columns[3])
                insertSQL = insertSQL.replace("[:insertLoginTime]", columns[4])
                insertSQL = insertSQL.replace("[:insertLogoutTime]", columns[5])
                # print(createSQL)
                # print(insertSQL)
                createSQLArr.append(createSQL)
                insertSQLArr.append(insertSQL)
        print(f"Insert {gameName} Init {tableName} {dateLine} Data")
        startRunTime = time.time()
        extracthCtrl.runsql(hiveCtrl, createSQLArr)
        extracthCtrl.runsql(hiveCtrl, insertSQLArr)
        print(f"Insert Init {tableName} {dateLine} Data Total Used {time.time() - startRunTime} seconds.")


def createTableSQL():
    return """
        CREATE EXTERNAL TABLE IF NOT EXISTS [:fullExtractTableName] ( 
            [:colunmsType]
        ) PARTITIONED BY ( [:partitioned] ) 
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' 
        LOCATION '[:filePath]'
        ;
    """


def InsertDataSQL():
    return """
        -- [:gameName] MakeData
        INSERT OVERWRITE TABLE [:fullExtractTableName] PARTITION([:PartitionedAlter]) 
        SELECT DISTINCT [:columns] 
        FROM [:gameName]_all.[:sourceTable]
        WHERE 1 = 1 
            AND (([:insertLogoutTime] >= DATE_ADD('[:dateLine]',0) 
                AND [:insertLogoutTime] < DATE_ADD('[:dateLine]',1)) 
                OR [:insertLogoutTime] IS NULL) 
            AND [:insertLoginTime] >= DATE_ADD('[:dateLine]',-14) 
            AND [:insertLoginTime] < DATE_ADD('[:dateLine]',1) 
            AND dt >= DATE_FORMAT(DATE_ADD('[:dateLine]',-14),'yyyyMMdd') 
            AND dt <= DATE_FORMAT(DATE_ADD('[:dateLine]',1),'yyyyMMdd')
        ; 
    """


def chkInsertData(makeTime, databaseInfo):
    gameName = databaseInfo["gameName"]
    dbName = databaseInfo["dbName"]
    hiveDB = databaseInfo["hiveDB"]
    tableNameArr = databaseInfo["tableNameArr"]
    dateNoLine = makeTime.strftime("%Y%m%d")
    dateLine = makeTime.strftime("%Y-%m-%d")

    print(f"{dbName} {dateLine} insert data check : ")
    for tableName in tableNameArr:
        showPartitionsSQL = "show partitions {}_{}.{}_{} partition(dt = '{}')".format(gameName, hiveDB, dbName, tableName, dateNoLine)
        # print(showPartitionsSQL)
        if hiveCtrl.searchSQL_TCByCount(showPartitionsSQL, 3).count()['partition'] == 0:
            message = u'\U0000203C' + " Info: {}_{}.{}_{} insert data fail.".format(gameName, hiveDB, dbName, tableName)
            print(message)
            # 送出Telegrame告警
            # telegramSend(message)


def telegramSend(message):
    try:
        telegramCtrl.sendMessageByUrl(url=os.getenv("BDA_TELEGRAM_URL"), userid=os.getenv("BDA_TELEGRAM_USERID"), massage=message)
        pass
    except Exception as e:
        print(e)
        pass


if __name__ == "__main__":
    print('start 11_00_MakeInitExtract')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStr = (nowZeroTime - datetime.timedelta(days=8)).strftime("%Y-%m-%d")
    main({"startdate": [makeDateStr], "enddate": [makeDateStr]})
    makeDateStr = (nowZeroTime - datetime.timedelta(days=4)).strftime("%Y-%m-%d")
    main({"startdate": [makeDateStr], "enddate": [makeDateStr]})
    makeDateStr = (nowZeroTime - datetime.timedelta(days=3)).strftime("%Y-%m-%d")
    main({"startdate": [makeDateStr], "enddate": [makeDateStr]})
    makeDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    main({"startdate": [makeDateStr], "enddate": [makeDateStr]})
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    # 開始前確認Table的Location Path是否正確
    # ALTER TABLE lineage_extract.init_onliontimebyaccount SET LOCATION '/user/hive/warehouse/lineage_extract.db/init/onliontimebyaccount';
    # ALTER TABLE lineage_extract.init_onliontimebycharacter SET LOCATION '/user/hive/warehouse/lineage_extract.db/init/init_onliontimebycharacter';