import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hiveCtrl_2 import HiveSingleCtrl
from package.common.telegramCtrl import TelegramCtrl
from package.common.sshCtrl import SSHCtrl
from package.common.extracth.extracthCtrl_2 import ExtracthCtrl
import pandas
import re
import datetime
import random
import time, asyncio
import os
from dotenv import load_dotenv

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

extracthCtrl = ExtracthCtrl()

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

telegramCtrl = TelegramCtrl()
startTime = time.time()

tableFilterArray = [
    ["TABLE_NAME", r"^[A-Za-z0-9_]+$", True]
]

tableFilterArray_UserInfo = [
    "fall"
    , "falldata"
    , "gloveflower"
    , "homerun"
    , "lovecountry"
    , "loveflower"
    , "pin"
    , "pingu"
]

tableFilterArray_UserInventory = [
    "changeshadow"
    ,"cookiecomplete"
    ,"lovecountry0"
    ,"tmp_flower_"
]

tableFilterArray_Userdynamic = [
    "bir"
    ,"penguin"
    ,"userladder"
    ,"lussi_tmp"
    ,"moneygift"
    ,"log"
    ,"zo"
]

def MakeDBTableDataMain():
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
        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "UserInfo"
            , "worldNameArr": ["00", "01", "02", "03", "04", "05"]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]' , world='[:World]'"
            , "partitionedPathInit": "dt=[:DateNoLine]/world=[:World]"
            , "partitionedSQLInit": "dt='[:DateNoLine]' AND world= '[:World]'"
            , "csvfile": "file/bnb_UserInfo_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/UserInfo01"
            , "specialFilter": tableFilterArray_UserInfo
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "UserInventory"
            , "worldNameArr": ["00", "01", "02", "03", "04", "05"]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]' , world='[:World]'"
            , "partitionedPathInit": "dt=[:DateNoLine]/world=[:World]"
            , "partitionedSQLInit": "dt='[:DateNoLine]' AND world= '[:World]'"
            , "csvfile": "file/bnb_UserInventory_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/UserInfo00"
            , "specialFilter": tableFilterArray_UserInventory
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "Userdynamic"
            , "worldNameArr": ["00", "01", "02", "03", "04", "05"]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]' , world='[:World]'"
            , "partitionedPathInit": "dt=[:DateNoLine]/world=[:World]"
            , "partitionedSQLInit": "dt='[:DateNoLine]' AND world= '[:World]'"
            , "csvfile": "file/bnb_Userdynamic_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Userdynamic03"
            , "specialFilter": tableFilterArray_Userdynamic
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "CAConfig"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_CAConfig_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/CAConfig"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "CAGuild"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_CAGuild_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/CAGuild"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "CAItem"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_CAItem_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/CAItem"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "CAMarket"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_CAMarket_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/CAMarket"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "CARankey"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_CARankey_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/CARankey"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "CASchool"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_CASchool_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/CASchool"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "CATaiwanStat"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_CATaiwanStat_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/CATaiwanStat"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "CAUtils"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_CAUtils_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/CAUtils"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "FxMessage"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_FxMessage_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/FxMessage"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "LadderInfo"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_LadderInfo_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/LadderInfo"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "LadderRank"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_LadderRank_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/LadderRank"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "RPGDynamic"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_RPGDynamic_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/RPGDynamic"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "RPGInventory"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_RPGInventory_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/RPGInventory"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "ServerLogDB"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_ServerLogDB_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/ServerLogDB"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "TranxLog"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_TranxLog_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/TranxLog"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "UserContent"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_UserContent_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/UserContent"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        databaseInfo = {
            "gameName": "bnb"
            , "dbName": "UserEditLog"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/bnb_UserEditLog_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/UserEditLog"
            , "specialFilter": []
        }
        extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo)
        chkInsertData(makeTime, databaseInfo)

        makeTime = makeTime + datetime.timedelta(days=1)

    print("Insert Data Total Used ", time.time() - startTime, "seconds.")

def chkInsertData(makeTime, databaseInfo):

    tableDataFrame = pandas.read_csv(databaseInfo["csvfile"], encoding="utf_8_sig")

    if databaseInfo["specialFilter"] is not None:
        specialFilter = databaseInfo["specialFilter"]
    else:
        specialFilter = []
    dataFrameTableFilter = tableDataFrame

    #利用排序決定執行順序
    tableDataFrame = tableDataFrame.sort_values(by=["layers", "type"])

    exLayer = 0
    print("{} insert data check : ".format(makeTime.strftime("%Y-%m-%d")))

    while exLayer <= 100:
        # print("{} insert data check exLayer : {}".format(makeTime.strftime("%Y-%m-%d"), str(exLayer)))
        exLayerTableDataFrame = tableDataFrame[tableDataFrame["layers"].isin([exLayer])]

        if len(exLayerTableDataFrame) == 0:
            exLayer = exLayer + 1
            continue
        # 確認資料是否有正常寫入
        for tableIndex, tableRow in exLayerTableDataFrame.iterrows():
            if tableRow["tablename"][:-2] in specialFilter:
                showPartitionsSQL = "show partitions {}_{}.{}_{} partition(dt = '{}')".format(str(tableRow["gamename"]), str(tableRow["hivedb"]), str(tableRow["dbname"]), str(tableRow["tablename"][:-2]), str(makeTime.strftime("%Y%m%d")))
            else:
                showPartitionsSQL = "show partitions {}_{}.{}_{} partition(dt = '{}')".format(str(tableRow["gamename"]), str(tableRow["hivedb"]), str(tableRow["dbname"]), str(tableRow["tablename"]), str(makeTime.strftime("%Y%m%d")))
            # print(showPartitionsSQL)
            if hiveCtrl.searchSQL_TCByCount(showPartitionsSQL,3).count()['partition'] == 0:
                message = u'\U0000203C' + " Info: {}_{}.{}_{} insert data fail.".format(str(tableRow["gamename"]), str(tableRow["hivedb"]), str(tableRow["dbname"]), str(tableRow["tablename"]))
                print(message)
                # 送出Telegrame告警
                try:
                    telegramCtrl.sendMessageByUrl(url=os.getenv("BDA_TELEGRAM_URL"), userid=os.getenv("BDA_TELEGRAM_USERID"), massage=message)
                except Exception as e:
                    print(e)
                    pass
            else:
                # print("Info: {}_{}.{}_{} insert data success.".format(str(tableRow["gamename"]), str(tableRow["hivedb"]) ,str(tableRow["dbname"]), str(tableRow["tablename"])))
                pass
        exLayer = exLayer + 1


if __name__ == "__main__":
    print('start 11_10_MakeALL')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    MakeDBTableDataMain()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
