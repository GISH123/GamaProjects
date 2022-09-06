import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hiveCtrl_2 import HiveSingleCtrl
# from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.telegramCtrl import TelegramCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl
from dotenv import load_dotenv
import random, pandas, time, re, datetime
# import sys
# from package.common.inputCtrl import inputCtrl

# load_dotenv(dotenv_path="env/hdfs.env")
load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/Telegram_GAA.env")

'''hdfsCtrl = HDFSCtrl(
    url=os.getenv("HDFS_HOST")
    , user=os.getenv("HDFS_USER")
    , password=os.getenv("HDFS_PASSWD")
    , filePath=os.getenv("HDFS_PATH")
)'''

hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST2")
    , port=int(os.getenv("HIVE_PORT2"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

hiveSingleCtrl = HiveSingleCtrl(
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

#----------------------------------------------------------------------------------------------------


def main():
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    '''startDateStr = (nowZeroTime - datetime.timedelta(days=3)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    # startDateStr = "2020-06-23"
    # endDateStr = "2020-06-27"
    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime

    while makeTime <= endDateTime:
        hiveSingleCtrl.startConnect()
        MakeGameTableMain(makeTime)
        MakeGamePartitionMain(makeTime)
        makeTime = makeTime + datetime.timedelta(days=1)
        hiveSingleCtrl.closeConnect()
    '''
    makeTimeArr = [
        nowZeroTime - datetime.timedelta(days=4)
        , nowZeroTime - datetime.timedelta(days=1)
        , nowZeroTime - datetime.timedelta(days=0)
    ]
    for makeTime in makeTimeArr:
        # hiveSingleCtrl.startConnect()
        # MakeGameTableMain(makeTime)
        MakeGamePartitionMain(makeTime)
        # hiveSingleCtrl.closeConnect()

    print("Total Used", time.time() - startTime, "seconds.")


def MakeGameTableMain(makeTime):
    databaseInfo = {
        "outputSQLFile": "file/Create_beanfun.sql"
        , "inputXlsName": "beanfun"
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bf_all.beanfundb_"
        , "filePathInit": "/user/hive/warehouse/bf.db/ALL/Table"
        , "partitionedInit": "dt string "
    }
    MakeGameTableDetail(makeTime, databaseInfo)


def MakeGamePartitionMain(makeTime):
    databaseInfo = {
        "inputXlsName": "beanfun"
        , "gameWorldArrMap": [
            ["", "beanfunDB"]
        ]
        , "schmeaNameInit": "bf_all.beanfundb_"
        , "tableFilterArray": tableFilterArray
        , "filePathInit": "/user/hive/warehouse/bf.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
    }
    MakeGamePartitionDetail(makeTime, databaseInfo)
    CheckGamePartitionCreated(makeTime, databaseInfo)

# ----------------------------------------------------------------------------------------------------


def MakeGameTableDetail (makeTime, databaseInfo):
    outputSQLFile = databaseInfo["outputSQLFile"]
    inputXlsName = databaseInfo["inputXlsName"]
    tableFilterArray = databaseInfo["tableFilterArray"]
    schmeaNameInit = databaseInfo["schmeaNameInit"]
    filePathInit = databaseInfo["filePathInit"]
    partitionedInit = databaseInfo["partitionedInit"]
    tableDataFrame = pandas.read_csv("file/" + inputXlsName + "_table.csv")
    columnDataFrame = pandas.read_csv("file/" + inputXlsName + "_column.csv")
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
                + "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' \n" \
                + "LOCATION '[:filePath]'  \n ;"

    print("Start Create", databaseInfo["inputXlsName"], makeTime.strftime("%Y-%m-%d"), "Tables.")
    taskCreateSQLStrArr = []
    for tableIndex, tableRow in dataFrameTableFilter.iterrows():
        dataFrameColumnMask_tablename = columnDataFrame["TABLE_NAME"] == tableRow["TABLE_NAME"]
        dataFrameColumnFilter = columnDataFrame[dataFrameColumnMask_tablename]
        dataFrameColumnFilter = dataFrameColumnFilter.sort_values(by="ORDINAL_POSITION", ascending=True)
        tableName = tableRow["TABLE_NAME"].lower()

        #if tableName != "gevent_timespace_point":
        #    continue

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

        createStr = createStrInit.replace("[:tableName]", tableFullName).replace("[:coulnmName]", coulnmNameStr).replace("[:partitioned]", partitionedStr).replace("[:filePath]", fileFullInit)

        # 若Table不存在則建立Table
        checkCreateTableCodeInit = "SHOW TABLES IN [:DBName] '[:TableName]'"
        checkCreateTableCode = checkCreateTableCodeInit.replace("[:DBName]", tableFullName.split('.')[0]).replace("[:TableName]", tableFullName.split('.')[1])
        if hiveSingleCtrl.searchSQL_TCByCount(checkCreateTableCode, 3).count()['tab_name'] == 0:
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
            ""
        ]
        if tableName_ori in noCreateNameArray:
            pass
        else:
            for gameWorldArr in gameWorldArrMap:
                tableFullName = schmeaNameInit + tableName
                partitionedStr = partitionedInit.replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:WorldNumber]",gameWorldArr[0])
                filePathStr = filePathInit.replace("[:DBName]", gameWorldArr[1]).replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:TableName]", tableName_ori)
                alterPartitionCode = alterPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr).replace("[:FilePath]", filePathStr)
                dropPartitionCode = dropPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr)

                taskDropSQLStrArr.append(dropPartitionCode)
                taskAlterSQLStrArr.append(alterPartitionCode)

    startRunTime = time.time()
    extracthCtrl.runsql(hiveCtrl, taskDropSQLStrArr)
    extracthCtrl.runsql(hiveCtrl, taskAlterSQLStrArr)
    print("Created", makeTime.strftime("%Y-%m-%d"), "Partitions Total Used", time.time() - startRunTime, "seconds.")


def CheckGamePartitionCreated(makeTime, databaseInfo):
    hiveSingleCtrl.startConnect()
    inputXlsName = databaseInfo["inputXlsName"]
    gameWorldArrMap = databaseInfo["gameWorldArrMap"]
    tableFilterArray = databaseInfo["tableFilterArray"]
    schmeaNameInit = databaseInfo["schmeaNameInit"]
    filePathInit = databaseInfo["filePathInit"]
    partitionedInit = databaseInfo["partitionedInit"]
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
            tableFullName = schmeaNameInit + tableName
            partitionedStr = partitionedInit.replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:WorldNumber]",gameWorldArr[0])
            filePathStr = filePathInit.replace("[:DBName]", gameWorldArr[1]).replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:TableName]", tableName_ori)

            # 確認Partition是否建立
            checkPartitionCodeInit = "SHOW PARTITIONS [:TableName] PARTITION ( [:Partitioned] ) "
            checkPartitionCode = checkPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr)

            if hiveSingleCtrl.searchSQL_TCByCount(checkPartitionCode, 3).count()['partition'] == 0:
                # 排除只有部份DB有的Table
                noCheckNameArray = [
                    ""
                ]
                if tableName_ori in noCheckNameArray:
                    pass
                else:
                    message = u'\U0000203C' + " Info: " + tableFullName + " Table Partition " + partitionedStr + " create fail. Please check " + filePathStr + " is exists."
                    print(message)
                    if makeTime.strftime("%Y-%m-%d") == (datetime.datetime.now() - datetime.timedelta(days=0)).strftime("%Y-%m-%d"):
                        # 送出Telegrame告警
                        try:
                            telegramCtrl.sendMessageByUrl(url=os.getenv("BDA_TELEGRAM_URL"), userid=os.getenv("BDA_TELEGRAM_USERID"), massage=message)
                        except Exception as e:
                            print(e)
                            pass
    hiveSingleCtrl.closeConnect()


if __name__ == "__main__":
    print('start 02_MakeTableAndPartition')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
