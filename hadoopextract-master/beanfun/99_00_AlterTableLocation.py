import os, sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.inputCtrl import inputCtrl
from package.common.telegramCtrl import TelegramCtrl
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
    gameName = "bf" if gameName == "" else gameName
    if dataBaseArr == [] and gameWorldArr == []:
        dataBaseArr = ["beanfundb"]
        gameWorldArr = []

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
        extracttableInitCode = "SELECT * FROM extractinfo.extractmakeinfo WHERE gamename = '[:gameName]' AND dbname = '[:dataBase]'"
        # 共用DB Extract
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
                print(f"Alter {gameName} {dataBase} Location")
                startRunTime = time.time()
                changeTableLocation(databaseInfo)
                print(f"Alter {dataBase} Location Total Used {time.time() - startRunTime} seconds.")

        # World Extract
        if gameWorldArr != []:
            worldNameArr = []
            for gameWorld in gameWorldArr:
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
            print(f"Alter {gameName} {dataBase} Location")
            startRunTime = time.time()
            changeTableLocation(databaseInfo)
            print(f"Alter {dataBase} Location Total Used {time.time() - startRunTime} seconds.")

    print(f"Alter {gameName} HadoopExtract Location Total Used ", time.time() - startTime, "seconds.")


def changeTableLocation(databaseInfo):
    if databaseInfo["csvfile"] == "":
        tableDataFrame = databaseInfo["extracttable"]
    else:
        tableDataFrame = pandas.read_csv(databaseInfo["csvfile"], encoding="utf_8_sig")
    # 建立Alter Location SQL
    alterTableLocationSQLArr = []
    for tableIndex, tableRow in tableDataFrame.iterrows():
        alterTableLocationSQLInit = f"ALTER TABLE [:tableFullName] SET LOCATION '/user/hive/warehouse/[:tablePath]';"
        tableFullName = f"{tableRow['gamename']}_{tableRow['hivedb']}.{tableRow['dbname']}_{tableRow['tablename']}"
        tablePath = f"{tableRow['gamename']}_{tableRow['hivedb']}.db/{tableRow['dbname']}/{tableRow['tablename']}"
        alterTableLocationSQL = alterTableLocationSQLInit.replace("[:tableFullName]", tableFullName)
        alterTableLocationSQL = alterTableLocationSQL.replace("[:tablePath]", tablePath)
        # print(alterTableLocationSQL)
        alterTableLocationSQLArr.append(alterTableLocationSQL)

    # print(alterTableLocationSQLArr)
    extracthCtrl.runsql(hiveCtrl, alterTableLocationSQLArr)


if __name__ == "__main__":
    print('start 99_00_AlterTableLocation')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
