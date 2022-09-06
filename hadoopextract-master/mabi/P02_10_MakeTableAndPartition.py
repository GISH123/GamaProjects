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

load_dotenv(dotenv_path="env/hdfs.env")
load_dotenv(dotenv_path="env/hive.env")
sshCtrl_hdfs = SSHCtrl(env="env/ETL05_SSH.env")
extracthCtrl = ExtracthCtrl()

hdfsCtrl = HDFSCtrl(
    url=os.getenv("HDFS_HOST")
    , user=os.getenv("HDFS_USER")
    , password=os.getenv("HDFS_PASSWD")
    , filePath=os.getenv("HDFS_PATH")
)

hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

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

taskSQLStrArr = []

#----------------------------------------------------------------------------------------------------

def main(parametersData = {}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    gameName = ""
    dbName = ""
    runALL = None
    runRestore = None

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
        if key == "dbname":
            dbName = parametersData[key][0]
        if key == "runall":
            runALL = True if parametersData[key][0] == "True" else False
        if key == "runrestore":
            runRestore = True if parametersData[key][0] == "True" else False

    if makeDateStrArr == [] :
        startDateStr = (nowZeroTime - datetime.timedelta(days=4)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameName = "mabi" if gameName == "" else gameName
    dbName = "DB" if dbName == "" else dbName
    runALL = True if runALL == None else runALL
    runRestore = True if runRestore == None else runRestore

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
        print("Run [MakeOriPartition] to {}".format(makeTime.strftime("%Y-%m-%d")))
        MakeGamePartitionMain(makeTime,gameName,dbName,runALL,runRestore)


#----------------------------------------------------------------------------------------------------

def MakeGamePartitionMain(makeTime,gameName,dbName,runALL=True,runRestore=True):
    databaseInfo = {
        "inputXlsName": "mabi_authdb"
        , "gameWorldArrMap": [
            ["","authdb"]
        ]
        , "schmeaNameInit": "mabi_all.authdb_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit" : "/user/hive/warehouse/mabi.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "runALL": runALL
        , "runRestore": runRestore
    }

    MakeGamePartitionDetail(makeTime,databaseInfo)

    databaseInfo = {
        "inputXlsName": "mabi_itemshop"
        , "gameWorldArrMap": [
            ["", "itemshop"]
        ]
        , "schmeaNameInit": "mabi_all.itemshop_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/mabi.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "runALL": runALL
        , "runRestore": runRestore
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "mabi_mabitaiwanshop"
        , "gameWorldArrMap": [
            ["", "MABI_TAIWAN_SHOP"]
        ]
        , "schmeaNameInit": "mabi_all.mabitaiwanshop_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/mabi.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "runALL": runALL
        , "runRestore": runRestore
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "mabi_mabiguild2"
        , "gameWorldArrMap": [
            ["", "mabi_guild2"]
        ]
        , "schmeaNameInit": "mabi_all.mabiguild2_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/mabi.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "runALL": runALL
        , "runRestore": runRestore
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "mabi_messengerDB"
        , "gameWorldArrMap": [
            ["", "messengerDB"]
        ]
        , "schmeaNameInit": "mabi_all.messengerDB_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/mabi.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "runALL": runALL
        , "runRestore": runRestore
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "mabi_mabitwmabinogi"
        , "gameWorldArrMap": [
            ["01", "mabitw1_mabinogi"]
            , ["02", "mabitw2_mabinogi"]
            , ["03", "mabitw3_mabinogi"]
            , ["04", "mabitw4_mabinogi"]
            , ["05", "mabitw5_mabinogi"]
        ]
        , "schmeaNameInit": "mabi_all.mabitwmabinogi_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/mabi.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]', world='[:WorldNumber]'"
        , "runALL": runALL
        , "runRestore": runRestore
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "mabi_mabitwauction"
        , "gameWorldArrMap": [
            ["01", "mabitw1_auction"]
            , ["02", "mabitw2_auction"]
            , ["03", "mabitw3_auction"]
            , ["04", "mabitw4_auction"]
            , ["05", "mabitw5_auction"]
        ]
        , "schmeaNameInit": "mabi_all.mabitwauction_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/mabi.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]', world='[:WorldNumber]'"
        , "runALL": runALL
        , "runRestore": runRestore
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "mabi_log"
        , "gameWorldArrMap": [
            ["", "log"]
        ]
        , "schmeaNameInit": "mabi_all.mabi_log_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hdfs/mabi/[:TableName]/date=[:DateNoLine]"
        , "partitionedInit": "date='[:DateNoLine]'"
        , "runALL": runALL
        , "runRestore": runRestore
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)

#----------------------------------------------------------------------------------------------------

def MakeGamePartitionDetail(makeTime,databaseInfo):
    sqlStrsInit = """
            ALTER TABLE [:TableName] DROP IF EXISTS PARTITION ( [:Partitioned] ) ;
            ALTER TABLE [:TableName] ADD IF NOT EXISTS PARTITION ( [:Partitioned] ) LOCATION '[:FilePath]' ;
        """

    inputXlsName = databaseInfo["inputXlsName"]
    gameWorldArrMap = databaseInfo["gameWorldArrMap"]
    tableFilterArray = databaseInfo["tableFilterArray"]
    schmeaNameInit = databaseInfo["schmeaNameInit"]
    filePathInit = databaseInfo["filePathInit"]
    partitionedInit = databaseInfo["partitionedInit"]
    makeTableNameArr = databaseInfo["makeTableNameArr"] if "makeTableNameArr" in databaseInfo.keys() else None

    tableDataFrame = pandas.read_csv("file/" + inputXlsName + "_table.csv")
    dataFrameTableFilter = tableDataFrame
    for tableFilter in tableFilterArray:
        dataFrameTableMask = dataFrameTableFilter[tableFilter[0]].str.contains(tableFilter[1])
        if tableFilter[2] == True:
            dataFrameTableFilter = dataFrameTableFilter[dataFrameTableMask]
        else:
            dataFrameTableFilter = dataFrameTableFilter[~dataFrameTableMask]

    taskSQLStrArr = []
    for tableIndex, tableRow in dataFrameTableFilter.iterrows():
        tableName_ori = tableRow["TABLE_NAME"]
        tableName = tableRow["TABLE_NAME"].lower()
        # 排除只有部份DB有的Table
        noCreateNameArray = [
                    # mabitwmabinogi
                    "accadb","allitem","aname","anewplayer","asset_now","asset_old","AssetRank_diff","bak_farmErrorList","bak_farmErrorList_1",
                    "bankitem","charitem","fateitem_20151119","houseitem","icea","icen1","item_a","lifeA","lifeAskill","musica","rankTable_after",
                    "rankTable_before","rankTable_diff","temp_farm_a","temp_test","tmp_asset20131025","tmp_assetrank_20180301","tmp_old_20110712",
                    "tmpCharacterMaxlevel_20190221","tmphouseitemhuge_20160721_2","tmpitemquestdel_20150528_1","tmpitemquestiddel_20150528_1",
                    "tmpMailboxitem_20151029_1","tmpMailboxitem_20160721_2","tmpquestdel_20150528_1","totalassetrank","vipitem_a","tmp_bot_id",
                    "bak_bugWeddingMailItem", "bak_bugWeddingHouseItem", "oldplayer", "bak_bugWeddingBankItem", "tmpMailboxitem_20150806",
                    "bak_bugWeddingCharItem", "tmphouseitemlarge_20150806", "tmpSoulMate_2014", "tmpCharactersumlevel_20190221", "tmpcharacter_metaMCESXT",
                    "tmpMailboxitem_20160324_1", "tmp_questG1G3", "backup_MateMailBoxReceive", "tmpitemquestiddel_20160324_1", "backup_MateMailBoxItem",
                    "tmpitemquestdel_20160324_1", "backup_MateHouseItem", "tmpquestdel_20160324_1", "backup_MateBankItem"
                ]
        if tableName_ori in noCreateNameArray:
            pass
        else:
            if makeTableNameArr != None and tableName not in makeTableNameArr:
                continue

            for gameWorldArr in gameWorldArrMap:
                tableFullName = schmeaNameInit + tableName
                partitionedStr = partitionedInit.replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:WorldNumber]", gameWorldArr[0])
                filePathStr = filePathInit.replace("[:DBName]", gameWorldArr[1]).replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:TableName]", tableName_ori)
                sqlStrs = sqlStrsInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr).replace("[:FilePath]", filePathStr)
                taskSQLStrArr.append(sqlStrs)

    if databaseInfo["runALL"] == True:
        extracthCtrl.runsql(hiveCtrl, taskSQLStrArr, printError=True)

#----------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    #MakeGameTableMain()
    main()

