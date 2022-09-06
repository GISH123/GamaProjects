import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hiveCtrl_2 import HiveSingleCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.telegramCtrl import TelegramCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl
from dotenv import load_dotenv
import os, sys, re, datetime
import random, pandas, time
from package.common.inputCtrl import inputCtrl

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

telegramCtrl = TelegramCtrl()
extracthCtrl = ExtracthCtrl()
startTime = time.time()

pandas.set_option("display.max_columns", None)
pandas.set_option("display.max_rows", None)
pandas.set_option("display.width", 1000)

cloumnStringArr =[
    ["end", "end_"]
    ,["world", "world_"]
    ,["dt", "dt_"]
    ,["map", "map_"]
]

dataTypeArr =[
    ["date", "timestamp"]
    ,["datetime", "timestamp"]
    ,["datetime2", "timestamp"]
    ,["elapsedtime", "timestamp"]
    ,["smalldatetime","timestamp"]
    ,["text","String"]
    ,["nchar","String"]
    ,["nvarchar","String"]
    ,["ntext","String"]
    ,["varbinary","String"]
    ,["varchar","String"]
    ,["image","String"]
    ,["binary","String"]
    ,["bit","String"]
    ,["char","String"]
    ,["xml","String"]
    ,["uniqueidentifier","String"]
    ,["tinyint","int"]
    ,["int","int"]
    ,["smallint","int"]
    ,["bigint","bigint"]
    ,["smallmoney","bigint"]
    ,["money","bigint"]
    ,["float","decimal"]
    ,["real","decimal"]
    ,["decimal","decimal"]
    ,["numeric","decimal"]
]

tableFilterArray = [
    ["TABLE_NAME", r"^[A-Za-z0-9_]+$", True]
    , ["TABLE_TYPE", r"BASE TABLE",True]
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

#----------------------------------------------------------------------------------------------------

def main():
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - datetime.timedelta(days=4)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    parametersData = inputCtrl.makeParametersData(sys.argv)
    for key in parametersData.keys():
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]

    # startDateStr = "2020-08-01"
    # endDateStr = "2020-08-18"
    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime

    while makeTime <= endDateTime:
        MakeGameTableMain(makeTime)
        MakeGamePartitionMain(makeTime)
        makeTime = makeTime + datetime.timedelta(days=1)

def MakeGameTableMain(makeTime):
    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_UserInfo.sql"
        , "inputXlsName": "bnb_UserInfo"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.UserInfo_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string , world string"
        , "specialFilter": tableFilterArray_UserInfo
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_UserInventory.sql"
        , "inputXlsName": "bnb_UserInventory"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.UserInventory_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string , world string"
        , "specialFilter": tableFilterArray_UserInventory
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_Userdynamic.sql"
        , "inputXlsName": "bnb_Userdynamic"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.Userdynamic_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string , world string"
        , "specialFilter": tableFilterArray_Userdynamic
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_CAConfig.sql"
        , "inputXlsName": "bnb_CAConfig"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.CAConfig_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_CAGuild.sql"
        , "inputXlsName": "bnb_CAGuild"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.CAGuild_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_CAItem.sql"
        , "inputXlsName": "bnb_CAItem"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.CAItem_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_CAMarket.sql"
        , "inputXlsName": "bnb_CAMarket"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.CAMarket_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_CARankey.sql"
        , "inputXlsName": "bnb_CARankey"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.CARankey_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_CASchool.sql"
        , "inputXlsName": "bnb_CASchool"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.CASchool_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_CATaiwanStat.sql"
        , "inputXlsName": "bnb_CATaiwanStat"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.CATaiwanStat_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_CAUtils.sql"
        , "inputXlsName": "bnb_CAUtils"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.CAUtils_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_FxMessage.sql"
        , "inputXlsName": "bnb_FxMessage"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.FxMessage_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_LadderInfo.sql"
        , "inputXlsName": "bnb_LadderInfo"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.LadderInfo_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_LadderRank.sql"
        , "inputXlsName": "bnb_LadderRank"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.LadderRank_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_RPGDynamic.sql"
        , "inputXlsName": "bnb_RPGDynamic"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.RPGDynamic_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_RPGInventory.sql"
        , "inputXlsName": "bnb_RPGInventory"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.RPGInventory_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_ServerLogDB.sql"
        , "inputXlsName": "bnb_ServerLogDB"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.ServerLogDB_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_TranxLog.sql"
        , "inputXlsName": "bnb_TranxLog"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.TranxLog_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_UserContent.sql"
        , "inputXlsName": "bnb_UserContent"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.UserContent_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_bnb_UserEditLog.sql"
        , "inputXlsName": "bnb_UserEditLog"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bnb_all.UserEditLog_"
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/Table"
        , "partitionedInit": "dt string"
        , "specialFilter": []
    }
    MakeGameTableDetail(makeTime, databaseInfo)

def MakeGamePartitionMain(makeTime):
    databaseInfo = {
        "inputXlsName": "bnb_UserInfo"
        , "gameWorldArrMap": [
            ["00", "UserInfo00"]
            , ["01", "UserInfo01"]
            , ["02", "UserInfo02"]
            , ["03", "UserInfo03"]
            , ["04", "UserInfo04"]
            , ["05", "UserInfo05"]
        ]
        , "schmeaNameInit": "bnb_all.userinfo_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]', world='[:WorldNumber]'"
        , "specialFilter": tableFilterArray_UserInfo
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    # CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_UserInventory"
        , "gameWorldArrMap": [
            ["00", "UserInventory00"]
            , ["01", "UserInventory01"]
            , ["02", "UserInventory02"]
            , ["03", "UserInventory03"]
            , ["04", "UserInventory04"]
            , ["05", "UserInventory05"]
        ]
        , "schmeaNameInit": "bnb_all.userinventory_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]', world='[:WorldNumber]'"
        , "specialFilter": tableFilterArray_UserInventory
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    # CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_Userdynamic"
        , "gameWorldArrMap": [
            ["00", "Userdynamic00"]
            , ["01", "Userdynamic01"]
            , ["02", "Userdynamic02"]
            , ["03", "Userdynamic03"]
            , ["04", "Userdynamic04"]
            , ["05", "Userdynamic05"]
        ]
        , "schmeaNameInit": "bnb_all.userdynamic_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]', world='[:WorldNumber]'"
        , "specialFilter": tableFilterArray_Userdynamic
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    # CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_CAConfig"
        , "gameWorldArrMap": [
            ["", "CAConfig"]
        ]
        , "schmeaNameInit": "bnb_all.CAConfig_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_CAGuild"
        , "gameWorldArrMap": [
            ["", "CAGuild"]
        ]
        , "schmeaNameInit": "bnb_all.CAGuild_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_CAItem"
        , "gameWorldArrMap": [
            ["", "CAItem"]
        ]
        , "schmeaNameInit": "bnb_all.CAItem_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_CAMarket"
        , "gameWorldArrMap": [
            ["", "CAMarket"]
        ]
        , "schmeaNameInit": "bnb_all.CAMarket_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_CARankey"
        , "gameWorldArrMap": [
            ["", "CARankey"]
        ]
        , "schmeaNameInit": "bnb_all.CARankey_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_CASchool"
        , "gameWorldArrMap": [
            ["", "CASchool"]
        ]
        , "schmeaNameInit": "bnb_all.CASchool_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_CATaiwanStat"
        , "gameWorldArrMap": [
            ["", "CATaiwanStat"]
        ]
        , "schmeaNameInit": "bnb_all.CATaiwanStat_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_CAUtils"
        , "gameWorldArrMap": [
            ["", "CAUtils"]
        ]
        , "schmeaNameInit": "bnb_all.CAUtils_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_FxMessage"
        , "gameWorldArrMap": [
            ["", "FxMessage"]
        ]
        , "schmeaNameInit": "bnb_all.FxMessage_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_LadderInfo"
        , "gameWorldArrMap": [
            ["", "LadderInfo"]
        ]
        , "schmeaNameInit": "bnb_all.LadderInfo_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_LadderRank"
        , "gameWorldArrMap": [
            ["", "LadderRank"]
        ]
        , "schmeaNameInit": "bnb_all.LadderRank_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_RPGDynamic"
        , "gameWorldArrMap": [
            ["", "RPGDynamic"]
        ]
        , "schmeaNameInit": "bnb_all.RPGDynamic_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_RPGInventory"
        , "gameWorldArrMap": [
            ["", "RPGInventory"]
        ]
        , "schmeaNameInit": "bnb_all.RPGInventory_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_ServerLogDB"
        , "gameWorldArrMap": [
            ["", "ServerLogDB"]
        ]
        , "schmeaNameInit": "bnb_all.ServerLogDB_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_TranxLog"
        , "gameWorldArrMap": [
            ["", "TranxLog"]
        ]
        , "schmeaNameInit": "bnb_all.TranxLog_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_UserContent"
        , "gameWorldArrMap": [
            ["", "UserContent"]
        ]
        , "schmeaNameInit": "bnb_all.UserContent_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "bnb_UserEditLog"
        , "gameWorldArrMap": [
            ["", "UserEditLog"]
        ]
        , "schmeaNameInit": "bnb_all.UserEditLog_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bnb.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "specialFilter": []
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

#----------------------------------------------------------------------------------------------

def MakeGameTableDetail (makeTime, databaseInfo):
    outputSQLFile = databaseInfo["outputSQLFile"]
    inputXlsName = databaseInfo["inputXlsName"]
    tableFilterArray = databaseInfo["tableFilterArray"]
    schmeaNameInit = databaseInfo["schmeaNameInit"]
    filePathInit = databaseInfo["filePathInit"]
    partitionedInit = databaseInfo["partitionedInit"]
    tableDataFrame = pandas.read_csv("file/" + inputXlsName + "_table.csv")
    columnDataFrame = pandas.read_csv("file/" + inputXlsName + "_column.csv")
    if databaseInfo["specialFilter"] is not None:
        specialFilter = databaseInfo["specialFilter"]
    else:
        specialFilter = []
    dataFrameTableFilter = tableDataFrame

    for tableFilter in tableFilterArray:
        dataFrameTableMask = dataFrameTableFilter[tableFilter[0]].str.contains(tableFilter[1])
        if tableFilter[2] == True:
            dataFrameTableFilter = dataFrameTableFilter[dataFrameTableMask]
        else:
            dataFrameTableFilter = dataFrameTableFilter[~dataFrameTableMask]

    createStrInit = "CREATE External TABLE [:tableName] ( \n" \
                + "    [:coulnmName] \n) " \
                + "PARTITIONED BY ( [:partitioned] ) \n" \
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' \n" \
                + "LOCATION '[:filePath]'  \n;"

    print("Start Create", databaseInfo["inputXlsName"], makeTime.strftime("%Y-%m-%d"), "Tables.")
    taskCreateSQLStrArr = []
    for tableIndex, tableRow in dataFrameTableFilter.iterrows():
        dataFrameColumnMask_tablename = columnDataFrame["TABLE_NAME"] == tableRow["TABLE_NAME"]
        dataFrameColumnFilter = columnDataFrame[dataFrameColumnMask_tablename]
        dataFrameColumnFilter = dataFrameColumnFilter.sort_values(by="ORDINAL_POSITION", ascending=True)
        tableName = tableRow["TABLE_NAME"].lower()

        #BNB table name differ with server number
        if tableName[:-2] in specialFilter:
            tableFullName = schmeaNameInit + tableName[:-2]
        else:
            tableFullName = schmeaNameInit + tableName

        coulnmNameStr = ""
        partitionedStr = partitionedInit
        fileFullInit = filePathInit

        for columnIndex, columnRow in dataFrameColumnFilter.iterrows():
            columnName = str(columnRow["COLUMN_NAME"]).lower().replace(" ", "")

            if bool(re.search(r"^[A-Za-z0-9_]+$",columnName)) == False:
                columnName = "column" + str(random.randint(1, 49999))

            if columnRow["ORDINAL_POSITION"] >= 2:
                coulnmNameStr = coulnmNameStr + "\n    , "

            for cloumnString in cloumnStringArr:
                if columnName == cloumnString[0]:
                    columnName = cloumnString[1]

            for dataType in dataTypeArr:
                if columnRow["DATA_TYPE"] == dataType[0]:
                    coulnmNameStr = coulnmNameStr + columnName + " " + dataType[1]

        createStr = createStrInit.replace("[:tableName]", tableFullName).replace("[:coulnmName]", coulnmNameStr).replace("[:partitioned]", partitionedStr).replace("[:filePath]",fileFullInit)

        # 若Table不存在則建立Table
        checkCreateTableCodeInit = "SHOW TABLES IN [:DBName] '[:TableName]'"
        checkCreateTableCode = checkCreateTableCodeInit.replace("[:DBName]", tableFullName.split('.')[0]).replace("[:TableName]", tableFullName.split('.')[1])
        if hiveCtrl.searchSQL(checkCreateTableCode).count()['tab_name'] == 0:
            taskCreateSQLStrArr.append(createStr)
        else:
            # print(tableFullName + ' Table is exists')
            pass

    startRunTime = time.time()
    extracthCtrl.runsql(hiveCtrl, taskCreateSQLStrArr)
    print("Created", makeTime.strftime("%Y-%m-%d"), "Tables Total Used", time.time() - startRunTime, "seconds.")

def MakeGamePartitionDetail(makeTime, databaseInfo):
    dropPartitionCodeInit = "ALTER TABLE [:TableName] DROP IF EXISTS PARTITION ( [:Partitioned] ) ;"
    alterPartitionCodeInit = "ALTER TABLE [:TableName] ADD IF NOT EXISTS PARTITION ( [:Partitioned] ) LOCATION '[:FilePath]' ;"
    inputXlsName = databaseInfo["inputXlsName"]
    gameWorldArrMap = databaseInfo["gameWorldArrMap"]
    tableFilterArray = databaseInfo["tableFilterArray"]
    schmeaNameInit = databaseInfo["schmeaNameInit"]
    filePathInit = databaseInfo["filePathInit"]
    partitionedInit = databaseInfo["partitionedInit"]
    if databaseInfo["specialFilter"] is not None:
        specialFilter = databaseInfo["specialFilter"]
    else:
        specialFilter = []
    tableDataFrame = pandas.read_csv("file/" + inputXlsName + "_table.csv")
    dataFrameTableFilter = tableDataFrame
    for tableFilter in tableFilterArray:
        dataFrameTableMask = dataFrameTableFilter[tableFilter[0]].str.contains(tableFilter[1])
        if tableFilter[2] == True:
            dataFrameTableFilter = dataFrameTableFilter[dataFrameTableMask]
        else:
            dataFrameTableFilter = dataFrameTableFilter[~dataFrameTableMask]

    print("Start Create", databaseInfo["inputXlsName"], makeTime.strftime("%Y-%m-%d"), "Partitions.")
    taskDropSQLStrArr = []
    taskAlterSQLStrArr = []
    for tableIndex, tableRow in dataFrameTableFilter.iterrows():
        tableName_ori = tableRow["TABLE_NAME"]
        tableName = tableRow["TABLE_NAME"].lower()

        # 排除只有部份DB有的Table
        noCreateNameArray = [
            "aa", "tmp_userinfodata_080403", "tset", "FxUserAccount_tmp", "FxUserAccount_tmp_0807", "tmp_newer_account_event",
            "aaa", "FxTranx_backup", "FxTranxOld_bak", "FxUserWishItem", "Lovecountry", "temp_201805", "tmp_error_Item02", "Sendreward1",
            "couple0915", "___FxUserBuyAnalysis", "E20110511tmp", "GMLog_UserDynamic", "LEVEL", "Log_Dynamic", "lucciproblem", "UserLadder00"
                    ]
        if tableName_ori in noCreateNameArray:
            pass
        else:
            for gameWorldArr in gameWorldArrMap:
                if tableName[:-2] in specialFilter:
                    tableFullName = schmeaNameInit + tableName[:-2]
                    filePathStr = filePathInit.replace("[:DBName]", gameWorldArr[1]).replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:TableName]", tableName_ori[:-2] + gameWorldArr[0])
                else:
                    tableFullName = schmeaNameInit + tableName
                    filePathStr = filePathInit.replace("[:DBName]", gameWorldArr[1]).replace("[:DateNoLine]",makeTime.strftime("%Y%m%d")).replace("[:TableName]", tableName_ori)

                partitionedStr = partitionedInit.replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:WorldNumber]",gameWorldArr[0])
                alterPartitionCode = alterPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr).replace("[:FilePath]", filePathStr)
                dropPartitionCode = dropPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr)

                taskDropSQLStrArr.append(dropPartitionCode)
                taskAlterSQLStrArr.append(alterPartitionCode)

    startRunTime = time.time()
    extracthCtrl.runsql(hiveCtrl, taskDropSQLStrArr)
    extracthCtrl.runsql(hiveCtrl, taskAlterSQLStrArr)
    print("Created", makeTime.strftime("%Y-%m-%d"), "Partitions Total Used", time.time() - startRunTime, "seconds.")

#----------------------------------------------------------------------------------------------

def CheckGamePartitionCreated(makeTime, databaseInfo):
    inputXlsName = databaseInfo["inputXlsName"]
    gameWorldArrMap = databaseInfo["gameWorldArrMap"]
    tableFilterArray = databaseInfo["tableFilterArray"]
    schmeaNameInit = databaseInfo["schmeaNameInit"]
    filePathInit = databaseInfo["filePathInit"]
    partitionedInit = databaseInfo["partitionedInit"]
    if databaseInfo["specialFilter"] is not None:
        specialFilter = databaseInfo["specialFilter"]
    else:
        specialFilter = []
    tableDataFrame = pandas.read_csv("file/" + inputXlsName + "_table.csv")
    dataFrameTableFilter = tableDataFrame
    print("{} Partition Created Check : ".format(makeTime.strftime("%Y-%m-%d")))
    for tableFilter in tableFilterArray:
        dataFrameTableMask = dataFrameTableFilter[tableFilter[0]].str.contains(tableFilter[1])
        if tableFilter[2] == True:
            dataFrameTableFilter = dataFrameTableFilter[dataFrameTableMask]
        else:
            dataFrameTableFilter = dataFrameTableFilter[~dataFrameTableMask]

    for tableIndex, tableRow in dataFrameTableFilter.iterrows():
        tableName_ori = tableRow["TABLE_NAME"]
        tableName = tableRow["TABLE_NAME"].lower()
        for gameWorldArr in gameWorldArrMap:
            if tableName[:-2] in specialFilter:
                tableFullName = schmeaNameInit + tableName[:-2]
            else:
                tableFullName = schmeaNameInit + tableName
            partitionedStr = partitionedInit.replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:WorldNumber]",gameWorldArr[0])
            filePathStr = filePathInit.replace("[:DBName]", gameWorldArr[1]).replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:TableName]", tableName_ori)

            # 確認Partition是否建立
            checkPartitionCodeInit = "SHOW PARTITIONS [:TableName] PARTITION ( [:Partitioned] ) "
            checkPartitionCode = checkPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr)

            if hiveCtrl.searchSQL(checkPartitionCode).count()['partition'] == 0:
                # 排除只有部份DB有的Table
                noCheckNameArray  = [
                        "aa", "tmp_userinfodata_080403", "tset", "FxUserAccount_tmp", "FxUserAccount_tmp_0807", "tmp_newer_account_event",
                        "aaa", "FxTranx_backup", "FxTranxOld_bak", "FxUserWishItem", "Lovecountry", "temp_201805", "tmp_error_Item02", "Sendreward1",
                        "couple0915", "___FxUserBuyAnalysis", "E20110511tmp", "GMLog_UserDynamic", "LEVEL", "Log_Dynamic", "lucciproblem", "UserLadder00"
                    ]
                if tableName_ori in noCheckNameArray:
                    pass
                else:
                    message = u'\U0000203C' + " Info: " + tableFullName + " Table Partition " + partitionedStr + " create fail. Please check " + filePathStr + " is exists."
                    print(message)
                    # 送出Telegrame告警
                    try:
                        telegramCtrl.sendMessageByUrl(url=os.getenv("BDA_TELEGRAM_URL"), userid=os.getenv("BDA_TELEGRAM_USERID"), massage=message)
                    except Exception as e:
                        print(e)
                        pass

if __name__ == "__main__":
    print('start 02_MakeTableAndPartition.py')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))