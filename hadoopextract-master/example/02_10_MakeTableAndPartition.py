import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from dotenv import load_dotenv
import os , sys , re , datetime
import random , pandas
from package.common.inputCtrl import inputCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl

load_dotenv(dotenv_path="env/hdfs.env")
load_dotenv(dotenv_path="env/hive.env")

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
]

taskSQLStrArr = []

#----------------------------------------------------------------------------------------------------

def main():
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeTimeArr = [
        nowZeroTime - datetime.timedelta(days=7)
        , nowZeroTime - datetime.timedelta(days=3)
        , nowZeroTime - datetime.timedelta(days=2)
        , nowZeroTime - datetime.timedelta(days=1)
    ]
    for makeTime in makeTimeArr:
        MakeGamePartitionMain(makeTime)
        makeTime = makeTime + datetime.timedelta(days=1)

#----------------------------------------------------------------------------------------------------

def MakeGameTableMain():
    databaseInfo = {
        "outputSQLFile": "file/Create_TDN_DNWorld.sql"
        , "inputXlsName": "tdn_DNWorld"
        , "tableFilterArray" : tableFilterArray
        , "schmeaNameInit": "tdn_all.dnworld_"
        , "filePathInit": "/user/hive/warehouse/tdn.db/ALL/Table"
        , "partitionedInit": "dt string , world string"
    }
    MakeGameTableDetail(databaseInfo)

    databaseInfo = {
        "outputSQLFile" : "file/Create_TDN_DNMembership.sql"
        , "inputXlsName" : "tdn_DNMembership"
        , "tableFilterArray" : tableFilterArray
        , "schmeaNameInit" : "tdn_all.dnmembership_"
        , "filePathInit" : "/user/hive/warehouse/tdn.db/ALL/Table"
        , "partitionedInit" : "dt string "
    }
    MakeGameTableDetail(databaseInfo)

    databaseInfo = {
        "outputSQLFile": "file/Create_TDN_DNStaging.sql"
        , "inputXlsName": "tdn_DNStaging_New"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit" : "tdn_all.dnstaging_"
        , "filePathInit": "/user/hive/warehouse/tdn.db/ALL/Table"
        , "partitionedInit": "dt string"
    }
    MakeGameTableDetail(databaseInfo)

def MakeGamePartitionMain(makeTime):
    databaseInfo = {
        "inputXlsName": "tdn_DNWorld"
        , "gameWorldArrMap": [
            ["01", "DNWorld"]
            , ["02", "DNWorld2"]
        ]
        , "schmeaNameInit": "tdn_all.dnworld_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit" : "/user/hive/warehouse/tdn.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]', world='[:WorldNumber]'"
    }
    MakeGamePartitionDetail(makeTime,databaseInfo)

    databaseInfo = {
        "inputXlsName" : "tdn_DNMembership"
        , "gameWorldArrMap" : [
            ["", "DNMembership"]
        ]
        , "schmeaNameInit": "tdn_all.dnmembership_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit" : "/user/hive/warehouse/tdn.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit" : "dt='[:DateNoLine]'"
    }
    MakeGamePartitionDetail(makeTime,databaseInfo)

    databaseInfo = {
        "inputXlsName": "tdn_DNStaging_New"
        , "gameWorldArrMap": [
            ["", "DNStaging"]
        ]
        , "schmeaNameInit": "tdn_all.dnstaging_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/tdn.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)

#----------------------------------------------------------------------------------------------------

def MakeGameTableDetail (databaseInfo) :
    outputSQLFile = databaseInfo["outputSQLFile"]
    inputXlsName = databaseInfo["inputXlsName"]
    tableFilterArray = databaseInfo["tableFilterArray"]
    schmeaNameInit = databaseInfo["schmeaNameInit"]
    filePathInit = databaseInfo["filePathInit"]
    partitionedInit = databaseInfo["partitionedInit"]
    tableDataFrame = pandas.read_csv("file/" + inputXlsName + "_table.csv")
    columnDataFrame = pandas.read_csv("file/" + inputXlsName + "_column.csv")
    createCodeStr = ""
    dataFrameTableFilter = tableDataFrame
    for tableFilter in tableFilterArray :
        dataFrameTableMask = dataFrameTableFilter[tableFilter[0]].str.contains(tableFilter[1])
        if tableFilter[2] == True :
            dataFrameTableFilter = dataFrameTableFilter[dataFrameTableMask]
        else :
            dataFrameTableFilter = dataFrameTableFilter[~dataFrameTableMask]

    createStrInit = "CREATE External TABLE [:tableName] ( \n" \
                + "    [:coulnmName] \n) " \
                + "PARTITIONED BY ( [:partitioned] ) \n" \
                + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' \n" \
                + "LOCATION '[:filePath]'  \n"

    for tableIndex, tableRow in dataFrameTableFilter.iterrows():


        dataFrameColumnMask_tablename = columnDataFrame["TABLE_NAME"] == tableRow["TABLE_NAME"]
        dataFrameColumnFilter = columnDataFrame[dataFrameColumnMask_tablename]
        dataFrameColumnFilter = dataFrameColumnFilter.sort_values(by="ORDINAL_POSITION",ascending=True)
        tableName = tableRow["TABLE_NAME"].lower()

        tableFullName = schmeaNameInit + tableName
        coulnmNameStr = ""
        partitionedStr = partitionedInit
        fileFullInit = filePathInit

        for columnIndex, columnRow in dataFrameColumnFilter.iterrows():
            columnName = str(columnRow["COLUMN_NAME"]).lower().replace(" ","")

            if bool(re.search(r"^[A-Za-z0-9_]+$",columnName)) == False :
                columnName = "column" + str(random.randint(1, 49999))

            if columnRow["ORDINAL_POSITION"] >= 2:
                coulnmNameStr = coulnmNameStr + "\n    , "

            for cloumnString in cloumnStringArr :
                if columnName == cloumnString[0] :
                    columnName = cloumnString[1]

            for dataType in dataTypeArr :
                if columnRow["DATA_TYPE"] == dataType[0] :
                    coulnmNameStr = coulnmNameStr + columnName + " " + dataType[1]

        createStr = createStrInit.replace("[:tableName]",tableFullName).replace("[:coulnmName]",coulnmNameStr).replace("[:partitioned]",partitionedStr).replace("[:filePath]",fileFullInit)

        try :
            #print("DROP TABLE IF EXISTS " + tableFullName)
            #hiveCtrl.executeSQL("DROP TABLE IF EXISTS " + tableFullName)
            print(createStr)
            hiveCtrl.executeSQL(createStr)
        except:
            print(createStr)
            print("table is exist")

            createCodeStr = createCodeStr + createStr + "\n\n"
    shfileMake = open(outputSQLFile, "w", encoding="utf-8")
    shfileMake.writelines(createCodeStr)

def MakeGamePartitionDetail(makeTime,databaseInfo):
    taskSQLStrArr = []
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

    for tableIndex, tableRow in dataFrameTableFilter.iterrows():
        tableName_ori = tableRow["TABLE_NAME"]
        tableName = tableRow["TABLE_NAME"].lower()
        if makeTableNameArr != None and tableName not in makeTableNameArr :
            continue

        for gameWorldArr in gameWorldArrMap:
            tableFullName = schmeaNameInit + tableName
            partitionedStr = partitionedInit.replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:WorldNumber]",gameWorldArr[0])
            filePathStr = filePathInit.replace("[:DBName]", gameWorldArr[1]).replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:TableName]", tableName_ori)
            sqlStrs = sqlStrsInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr).replace("[:FilePath]", filePathStr)
            print(sqlStrs)
            taskSQLStrArr.append(sqlStrs)

    extracthCtrl.runsql(hiveCtrl, taskSQLStrArr)

#----------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    #MakeGameTableMain()
    main()

