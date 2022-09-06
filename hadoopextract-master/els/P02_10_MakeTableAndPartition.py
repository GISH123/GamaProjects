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
sshCtrl_hdfs = SSHCtrl(env="env/ETL01_SSH_HDFS.env")
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

tableFilterArray = [
    ["TABLE_NAME", r"^[A-Za-z0-9_]+$", True]
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
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameName = "els" if gameName == "" else gameName
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
        "inputXlsName": "els_Game01"
        , "gameWorldArrMap": [
            ["", "Game01"]
        ]
        , "schmeaNameInit": "els_all.game01_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/els.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "runALL" : runALL
        , "runRestore" : runRestore
    }
    if dbName == 'DB' or dbName == 'game01':
        MakeGamePartitionDetail(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "els_Account"
        , "gameWorldArrMap": [
            ["", "Account"]
        ]
        , "schmeaNameInit": "els_all.account_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/els.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "runALL" : runALL
        , "runRestore" : runRestore
    }
    if dbName == 'DB' or dbName == 'account':
        MakeGamePartitionDetail(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "els_ConvertCenter"
        , "gameWorldArrMap": [
            ["", "ConvertCenter"]
        ]
        , "schmeaNameInit": "els_all.convertcenter_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/els.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "runALL": runALL
        , "runRestore": runRestore
    }
    if dbName == 'DB' or dbName == 'convertcenter':
        MakeGamePartitionDetail(makeTime, databaseInfo)

    databaseInfo = {
        "inputXlsName": "els_Statistics"
        , "gameWorldArrMap": [
            ["", "Statistics"]
        ]
        , "schmeaNameInit": "els_all.statistics_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/els.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        , "runALL": runALL
        , "runRestore": runRestore
    }
    if dbName == 'DB' or dbName == 'statistics':
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

    taskSQLStrArr = []
    for gameWorldArr in gameWorldArrMap:
        hadoopStr = "hadoop dfs -du -s /user/hive/warehouse/els.db/Restore/{}/dt={}/*".format(gameWorldArr[1], makeTime.strftime("%Y%m%d"))
        execRetrunStr = sshCtrl_hdfs.ssh_exec_cmd_return(hadoopStr)
        retrunStrArr = execRetrunStr.split("\r\n")
        mainDataList = []
        for retrunStr in retrunStrArr:
            if retrunStr == "DEPRECATED: Use of this script to execute hdfs command is deprecated.":
                continue
            elif retrunStr == "Instead use the hdfs command for it.":
                continue
            elif retrunStr == "":
                continue
            elif retrunStr.find("No such file or directory") >= 0:
                continue
            else:
                def not_empty(s):
                    return s and s.strip()

                pathSizeData = list(filter(not_empty, retrunStr.split(" ")))
                pathData = list(filter(not_empty, pathSizeData[2].split("/")))
                tableName = pathData[7].lower()
                tableFullName = schmeaNameInit + tableName
                partitionedStr = partitionedInit.replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:WorldNumber]", gameWorldArr[0])
                sqlStrs = sqlStrsInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr).replace("[:FilePath]", pathSizeData[2])
                taskSQLStrArr.append(sqlStrs)

    if databaseInfo["runRestore"] == True:
        extracthCtrl.runsql(hiveCtrl, taskSQLStrArr,printError=True)

#----------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    main()

