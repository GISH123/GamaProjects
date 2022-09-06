import os, sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.inputCtrl import inputCtrl
from package.common.sshCtrl import SSHCtrl
from package.common.telegramCtrl import TelegramCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl
from package.common.database.mssqlCtrl import MSSQLCtrl
from dotenv import load_dotenv
import pandas
import datetime
import time
import random
import re

load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/GAME_BAK_INFO.env")
telegramCtrl = TelegramCtrl()
extracthCtrl = ExtracthCtrl()

sshCtrl_hdfs = SSHCtrl("10.10.99.133", 22, "hdfs", pkey="env/ALL_PKEY_HDFS")

hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST2")
    , port=int(os.getenv("HIVE_PORT2"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

mssqlCtrl = MSSQLCtrl(
    host="10.10.99.148"
    , port="1433"
    , user="bfbd"
    , password=os.getenv("MAIN_RESTORE_PASSWORD")
    , database="master"
)

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
    , ["TABLE_TYPE", r"VIEW", True]
]

#----------------------------------------------------------------------------------------------------


def main(parametersData = {}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    gameName = ""
    dataBaseArr = []

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

    if makeDateStrArr == []:
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameName = "bf" if gameName == "" else gameName
    dataBaseArr = ["GTWBF5020_bfEvent", "GTWBF5033_BeanFunEvent", "GTWBF5035_bfAPP"] if dataBaseArr == [] else dataBaseArr

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

    databaseInfo = {
        "gameName": gameName
        , "dataBaseArr": dataBaseArr
        , "tableFilterArray": tableFilterArray
        , "schmeaNameInit": "bf_all.[:dataBase]_"
        , "tableFilePathInit": "/user/hive/warehouse/bf.db/ALL/[:dataBase]"
        , "tablePartitionedInit": "dt string"
        , "filePathInit": "/user/hive/warehouse/bf.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
    }

    cteateTables(mssqlCtrl, databaseInfo)

    for makeTime in makeTimeArr:
        createPartitions(mssqlCtrl, databaseInfo, makeTime)


def cteateTables(mssqlCtrl, databaseInfo):
    dataBaseArr = databaseInfo["dataBaseArr"]
    tableFilterArray = databaseInfo["tableFilterArray"]
    schmeaNameInit = databaseInfo["schmeaNameInit"]
    filePathInit = databaseInfo["tableFilePathInit"]
    partitionedInit = databaseInfo["tablePartitionedInit"]

    for dataBase in dataBaseArr:
        # 取得Table Name
        dataFrameTableFilter = mssqlCtrl.searchSQL(f"SELECT * FROM {dataBase}.INFORMATION_SCHEMA.TABLES")
        # 取得Table的Column Name及資料類型
        columnDataFrame = mssqlCtrl.searchSQL(f"SELECT * FROM {dataBase}.INFORMATION_SCHEMA.COLUMNS")

        for tableFilter in tableFilterArray:
            dataFrameTableMask = dataFrameTableFilter[tableFilter[0]].str.contains(tableFilter[1])
            if tableFilter[2] == True:
                dataFrameTableFilter = dataFrameTableFilter[dataFrameTableMask]
            else:
                dataFrameTableFilter = dataFrameTableFilter[~dataFrameTableMask]

        createStrInit = "CREATE External TABLE IF NOT EXISTS [:tableName] ( \n" \
                    + "    [:coulnmName] \n) " \
                    + "PARTITIONED BY ( [:partitioned] ) \n" \
                    + "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' LINES TERMINATED BY '\\n' \n" \
                    + "LOCATION '[:filePath]'  \n ;"
        print("Start Create", dataBase, "Tables.")
        taskCreateSQLStrArr = []
        for tableIndex, tableRow in dataFrameTableFilter.iterrows():
            dataFrameColumnMask_tablename = columnDataFrame["TABLE_NAME"] == tableRow["TABLE_NAME"]
            dataFrameColumnFilter = columnDataFrame[dataFrameColumnMask_tablename]
            dataFrameColumnFilter = dataFrameColumnFilter.sort_values(by="ORDINAL_POSITION", ascending=True)
            tableName = tableRow["TABLE_NAME"].lower()
            tableFullName = schmeaNameInit.replace("[:dataBase]", dataBase.lower()) + tableName
            coulnmNameStr = ""
            partitionedStr = partitionedInit
            fileFullInit = filePathInit.replace("[:dataBase]", dataBase)

            for columnIndex, columnRow in dataFrameColumnFilter.iterrows():
                columnName = str(columnRow["COLUMN_NAME"]).lower().replace(" ", "")

                if bool(re.search(r"^[A-Za-z0-9_]+$", columnName)) == False:
                    columnName = "column" + str(random.randint(1, 49999))

                if columnRow["ORDINAL_POSITION"] >= 2:
                    coulnmNameStr = coulnmNameStr + "\n    , "

                for cloumnString in cloumnStringArr:
                    if columnName == cloumnString[0]:
                        columnName = cloumnString[1]

                for dataType in dataTypeArr:
                    if columnRow["DATA_TYPE"] == dataType[0]:
                        coulnmNameStr = coulnmNameStr + columnName + " " + dataType[1]

            createStr = createStrInit.replace("[:tableName]", tableFullName).replace("[:coulnmName]", coulnmNameStr)
            createStr = createStr.replace("[:partitioned]", partitionedStr).replace("[:filePath]", fileFullInit)
            taskCreateSQLStrArr.append(createStr)
            # print(createStr)

        startRunTime = time.time()
        extracthCtrl.runsql(hiveCtrl, taskCreateSQLStrArr)
        print(f"Created {dataBase} Totals Used", time.time() - startRunTime, "seconds.")


def createPartitions(mssqlCtrl, databaseInfo, makeTime):
    dataBaseArr = databaseInfo["dataBaseArr"]
    tableFilterArray = databaseInfo["tableFilterArray"]
    schmeaNameInit = databaseInfo["schmeaNameInit"]
    filePathInit = databaseInfo["filePathInit"]
    partitionedInit = databaseInfo["partitionedInit"]

    dropPartitionCodeInit = "ALTER TABLE [:TableName] DROP IF EXISTS PARTITION ( [:Partitioned] ) ;"
    alterPartitionCodeInit = "ALTER TABLE [:TableName] ADD IF NOT EXISTS PARTITION ( [:Partitioned] ) LOCATION '[:FilePath]' ;"

    for dataBase in dataBaseArr:
        # 取得Table Name
        dataFrameTableFilter = mssqlCtrl.searchSQL(f"SELECT * FROM {dataBase}.INFORMATION_SCHEMA.TABLES")

        for tableFilter in tableFilterArray:
            dataFrameTableMask = dataFrameTableFilter[tableFilter[0]].str.contains(tableFilter[1])
            if tableFilter[2] == True:
                dataFrameTableFilter = dataFrameTableFilter[dataFrameTableMask]
            else:
                dataFrameTableFilter = dataFrameTableFilter[~dataFrameTableMask]

        print(f"Start Create {dataBase} {makeTime.strftime('%Y-%m-%d')} Partitions.")
        taskDropSQLStrArr = []
        taskAlterSQLStrArr = []
        for tableIndex, tableRow in dataFrameTableFilter.iterrows():
            tableName_ori = tableRow["TABLE_NAME"]
            tableName = tableRow["TABLE_NAME"].lower()
            tableFullName = schmeaNameInit.replace("[:dataBase]", dataBase.lower()) + tableName
            partitionedStr = partitionedInit.replace("[:DateNoLine]", makeTime.strftime("%Y%m%d"))
            filePathStr = filePathInit.replace("[:DBName]", dataBase).replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:TableName]", tableName_ori)
            alterPartitionCode = alterPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr).replace("[:FilePath]", filePathStr)
            dropPartitionCode = dropPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr)

            taskDropSQLStrArr.append(dropPartitionCode)
            taskAlterSQLStrArr.append(alterPartitionCode)
            # print(alterPartitionCode)

        startRunTime = time.time()
        extracthCtrl.runsql(hiveCtrl, taskDropSQLStrArr)
        extracthCtrl.runsql(hiveCtrl, taskAlterSQLStrArr)
        print("Created", makeTime.strftime("%Y-%m-%d"), "Partitions Total Used", time.time() - startRunTime, "seconds.")


if __name__ == "__main__":
    print('start 02_01_MakeTableAndPartition')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStr = (nowZeroTime - datetime.timedelta(days=4)).strftime("%Y-%m-%d")
    main({"startdate": [makeDateStr], "enddate": [makeDateStr]})
    makeDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    main({"startdate": [makeDateStr], "enddate": [makeDateStr]})
    makeDateStr = (nowZeroTime - datetime.timedelta(days=0)).strftime("%Y-%m-%d")
    main({"startdate": [makeDateStr], "enddate": [makeDateStr]})
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
