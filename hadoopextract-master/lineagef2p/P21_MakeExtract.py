import os, sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.inputCtrl import inputCtrl
from package.common.telegramCtrl import TelegramCtrl
# from package.common.extracth.extracthCtrl import ExtracthCtrl
from package.common.extracth.extracthCtrl_2_1_0 import ExtracthCtrl
from package.common.database.postgreCtrl import PostgresCtrl
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

postgresCtrl = PostgresCtrl(
    host=os.getenv("POSTGRES_HOST182")
    , port=os.getenv("POSTGRES_PORT")
    , user=os.getenv("POSTGRES_USER")
    , password=os.getenv("POSTGRES_PASSWORD")
    , database="Hadoop"
    , schema="extractinfo"
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
    gameWorldArr = []

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
        if key == "gameWorldArr":
            gameWorldArr = parametersData[key]

    if makeDateStrArr == []:
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameName = "lineage" if gameName == "" else gameName
    if dataBaseArr == [] and gameWorldArr == []:
        dataBaseArr = ["LinDBF2P", "PurchaseAgentDB"]
        gameWorldArr = [
            # [伺服器線編號, DBName, 中繼區TableName, 關閉時間]
            ["01", "LINWORLD101", "linworld_f2p", "2099-12-31"]
            , ["02", "LINWORLD102", "linworld_f2p", "2099-12-31"]
            , ["03", "LINWORLD103", "linworld_f2p", "2099-12-31"]
            , ["06", "LINWORLD106", "linworld_f2p", "2099-12-31"]
            , ["23", "LINWORLD123", "linworld_f2p", "2099-12-31"]
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
        extracttableInitCode = "SELECT * FROM extractinfo.extractmakeinfo WHERE gamename = '[:gameName]' AND dbname = '[:dataBase]'"
        # 建立共用DB Extract
        if dataBaseArr != []:
            for dataBase in dataBaseArr:
                extracttableSQL = extracttableInitCode.replace("[:gameName]", gameName.lower()).replace("[:dataBase]", dataBase.lower())
                extracttable = postgresCtrl.searchSQL(extracttableSQL)
                databaseInfo = {
                    "gameName": gameName.lower()
                    , "dbName": dataBase.lower()
                    , "worldNameArr": [""]
                    , "partitionedInit": "dt string"
                    , "partitionedAlterInit": "dt='[:DateNoLine]'"
                    , "partitionedPathInit": "dt=[:DateNoLine]"
                    , "partitionedSQLInit": "dt='[:DateNoLine]'"
                    # , "csvfile": "file/lineageF2P_[:dataBase]_extracttable.csv".replace("[:dataBase]", dataBase.lower())
                    , "csvfile": ""
                    , "extracttable": extracttable
                    , "filePathInit": f"/user/hive/warehouse/{gameName.lower()}_extract.db/{dataBase.lower()}/[:tableName]"
                    , "typeArray": []
                    , "tableNameArray": []
                }
                print(f"Insert {gameName} {dataBase} Extract {dateLine} Data")
                startRunTime = time.time()
                extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
                # chkInsertData(makeTime, databaseInfo)
                print(f"Insert {dataBase} Extract {dateLine} Data Total Used {time.time() - startRunTime} seconds.")

        # 建立World Extract
        if gameWorldArr != []:
            worldNameArr = []
            for gameWorld in gameWorldArr:
                if makeTime <= datetime.datetime.strptime(gameWorld[3], "%Y-%m-%d"):
                    dataBase = gameWorld[2]
                    worldNameArr.append(gameWorld[0])
            extracttableSQL = extracttableInitCode.replace("[:gameName]", gameName.lower()).replace("[:dataBase]", dataBase.lower())
            extracttable = postgresCtrl.searchSQL(extracttableSQL)
            databaseInfo = {
                "gameName": gameName.lower()
                , "dbName": dataBase.lower()
                , "worldNameArr": worldNameArr
                , "partitionedInit": "dt string , world string"
                , "partitionedAlterInit": "dt='[:DateNoLine]' , world='[:World]'"
                , "partitionedPathInit": "dt=[:DateNoLine]/world=[:World]"
                , "partitionedSQLInit": "dt='[:DateNoLine]' AND world= '[:World]'"
                # , "csvfile": "file/lineageF2P_[:dataBase]_extracttable.csv".replace("[:dataBase]", dataBase.lower())
                , "csvfile": ""
                , "extracttable": extracttable
                , "filePathInit": f"/user/hive/warehouse/{gameName.lower()}_extract.db/{dataBase.lower()}/[:tableName]"
                , "typeArray": []
                , "tableNameArray": []
            }
            print(f"Insert {gameName} {dataBase} Extract {dateLine} Data")
            startRunTime = time.time()
            extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
            # chkInsertData(makeTime, databaseInfo)
            print(f"Insert {dataBase} Extract {dateLine} Data Total Used {time.time() - startRunTime} seconds.")

    print(f"Insert {gameName} HadoopExtract Data Total Used ", time.time() - startTime, "seconds.")


def chkInsertData(makeTime, databaseInfo):
    if databaseInfo["csvfile"] == "":
        tableDataFrame = databaseInfo["extracttable"]
    else:
        tableDataFrame = pandas.read_csv(databaseInfo["csvfile"], encoding="utf_8_sig")
    dbName = databaseInfo["dbName"]
    dataLine = format(makeTime.strftime("%Y-%m-%d"))
    #利用排序決定執行順序
    tableDataFrame = tableDataFrame.sort_values(by=["layers", "type"])

    exLayer = 0
    print(f"{dbName} {dataLine} insert data check : ")

    while exLayer <= 100:
        # print("{} insert data check exLayer : {}".format(makeTime.strftime("%Y-%m-%d"), str(exLayer)))
        exLayerTableDataFrame = tableDataFrame[tableDataFrame["layers"].isin([exLayer])]

        if len(exLayerTableDataFrame) == 0:
            exLayer = exLayer + 1
            continue
        # 確認資料是否有正常寫入
        for tableIndex, tableRow in exLayerTableDataFrame.iterrows():
            showPartitionsSQL = "show partitions {}_{}.{}_{} partition(dt = '{}')".format(str(tableRow["gamename"]), str(tableRow["hivedb"]), str(tableRow["dbname"]), str(tableRow["tablename"]), str(makeTime.strftime("%Y%m%d")))
            # print(showPartitionsSQL)
            if hiveCtrl.searchSQL_TCByCount(showPartitionsSQL, 3).count()['partition'] == 0:
                message = u'\U0000203C' + " Info: {}_{}.{}_{} insert data fail.".format(str(tableRow["gamename"]), str(tableRow["hivedb"]), str(tableRow["dbname"]), str(tableRow["tablename"]))
                print(message)
                # 送出Telegrame告警
                telegramSend(message)

        exLayer = exLayer + 1


def telegramSend(message):
    try:
        telegramCtrl.sendMessageByUrl(url=os.getenv("BDA_TELEGRAM_URL"), userid=os.getenv("BDA_TELEGRAM_USERID"), massage=message)
        pass
    except Exception as e:
        print(e)
        pass


if __name__ == "__main__":
    print('start 11_10_MakeALL')
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
