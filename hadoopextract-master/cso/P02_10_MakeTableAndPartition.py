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
    , ["timestamp", "timestamp"]
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
    gameName = "cso" if gameName == "" else gameName
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
        "inputXlsName": "cso_cso"
        , "gameWorldArrMap": [
            ["", "cso"]
        ]
        , "schmeaNameInit": "cso_all.cso_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit" : "/user/hive/warehouse/cso.db/ALL/cso/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "runALL": runALL
        , "runRestore": runRestore
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName" : "cso_db_shop"
        , "gameWorldArrMap" : [
            ["", "db_shop"]
        ]
        , "schmeaNameInit": "cso_all.db_shop_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit" : "/user/hive/warehouse/cso.db/ALL/db_shop/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit" : "dt='[:DateNoLine]'"
        , "runALL": runALL
        , "runRestore": runRestore
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName" : "cso_NX_Messenger00"
        , "gameWorldArrMap" : [
            ["", "nx_messenger00"]
        ]
        , "schmeaNameInit": "cso_all.nx_messenger00_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit" : "/user/hive/warehouse/cso.db/ALL/NX_Messenger00/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit" : "dt='[:DateNoLine]'"
        , "runALL": runALL
        , "runRestore": runRestore
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "cso_log"
        , "gameWorldArrMap": [
            ["", "cso_log"]
        ]
        , "schmeaNameInit": "cso_all.cso_log_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/cso.db/ALL/CSO_Log/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
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
            "auction_restrictedItem", "temp170717", "temp170718items", "account", "id", "report_old2", "temp_clan_member_info",
            "temp_erased_clan_info", "temp_erased_clan_list", "temp_expeled_members", "character_temp", "temp_backup_member",
            "minigame_whole_data", "d20120223_character_create", "d20120223_character_update", "reward_recieved_131229",
            "clan_zombie_temp", "member_zombie_temp", "clan_donate_temp", "clan_union_temp", "evt200820_settlement_support_users",
            "d20080716_location", "d20080716_character", "d20080716_location3", "d20080716_location2", "teminven0212",
            "teminven0212m", "20121207intimacy_edge", "d20080717_character", "temp0401", "temp0401_2"
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

