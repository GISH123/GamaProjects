import os, sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.inputCtrl import inputCtrl
from package.common.sshCtrl import SSHCtrl
from package.common.telegramCtrl import TelegramCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl
from package.common.database.mssqlCtrl import MSSQLCtrl
from package.common.database.postgreCtrl import PostgresCtrl
from dotenv import load_dotenv
import pandas
import datetime
import time
import random
import re

load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/GAME_BAK_INFO.env")
load_dotenv(dotenv_path="env/PostgreSQL.env")
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
    host="10.10.99.145"
    , port="1433"
    , user="lineagebd"
    , password=os.getenv("MAIN_RESTORE_PASSWORD")
    , database="master"
)

postgresCtrl = PostgresCtrl(
    host=os.getenv("POSTGRES_HOST182")
    , port=os.getenv("POSTGRES_PORT")
    , user=os.getenv("POSTGRES_USER")
    , password=os.getenv("POSTGRES_PASSWORD")
    , database="Hadoop"
    , schema="originalinfo"
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
    ["table_name", r"^[A-Za-z0-9_]+$", True]
    , ["table_type", r"BASE TABLE", True]
    , ["table_name", r"temp_", False]
    , ["table_name", r"_temp", False]
    , ["table_name", r"TEMP_", False]
    , ["table_name", r"_TEMP", False]
    , ["table_name", r"_tmp", False]
    , ["table_name", r"tmp_", False]
    , ["table_name", r"_200", False]
    , ["table_name", r"_201", False]
    , ["table_name", r"_202", False]
    , ["table_name", r"_old", False]
    , ["table_name", r"_backup", False]
    , ["table_name", r"backup_", False]
]

#----------------------------------------------------------------------------------------------------


def main(parametersData = {}, checkCreate = True):
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
    gameName = "lineagef2p" if gameName == "" else gameName
    dataBaseArr1 = ["LinDBF2P", "PurchaseAgentDB"] if dataBaseArr == [] and gameWorldArr == [] else dataBaseArr
    gameWorldArr = [
        # [伺服器線編號, DBName, 關閉時間]
        ["01", "LINWORLD101", "2099-12-31"]
        , ["02", "LINWORLD102", "2099-12-31"]
        , ["03", "LINWORLD103", "2099-12-31"]
        , ["06", "LINWORLD106", "2099-12-31"]
        , ["23", "LINWORLD123", "2099-12-31"]
        ] if dataBaseArr == [] and gameWorldArr == [] else gameWorldArr

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
        , "dataBaseArr": dataBaseArr1
        , "gameWorldArr": gameWorldArr
        , "tableFilterArray": tableFilterArray
        # 建共用DB使用
        , "schmeaNameInit": "lineage_all.[:dataBase]_"
        , "tableFilePathInit": "/user/hive/warehouse/lineage.db/ALL/[:dataBase]"
        , "tablePartitionedInit": "dt string"
        # 共用DB建Partition使用
        , "filePathInit": "/user/hive/warehouse/lineage.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "partitionedInit": "dt='[:DateNoLine]'"
        # 建World DB使用
        , "worldSchmeaNameInit": "lineage_all.linworld_f2p_"
        , "worldTableFilePathInit": "/user/hive/warehouse/lineage.db/ALL/Table"
        , "tableWorldPartitionedInit": "dt string, world string"
        # 共用World DB建Partition使用
        , "worldFilePathInit": "/user/hive/warehouse/lineage.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
        , "worldPartitionedInit": "dt='[:DateNoLine]', world='[:WorldNumber]'"
    }

    # 確認是否有新的Table產生
    if checkCreate == True:
        for database in dataBaseArr1:
            getTableInfo(database, nowTime)
        for gameWorld in gameWorldArr:
            if datetime.datetime.now() <= datetime.datetime.strptime(gameWorld[2], "%Y-%m-%d"):
                getTableInfo(gameWorld[1], nowTime)
        cteateTables(databaseInfo)
        cteateWorldTables(databaseInfo)

    for makeTime in makeTimeArr:
        dateLine = makeTime.strftime("%Y-%m-%d")
        print(f"Create {gameName} DataBase {dateLine} Partitions")
        startRunTime = time.time()
        createPartitions(databaseInfo, makeTime)
        createWorldPartitions(databaseInfo, makeTime)
        print(f"Create DataBase {dateLine} Partition Total Used {time.time() - startRunTime} seconds.")


def getTableInfo(dataBase, nowTime):
    # 因Relay未還原新的備份，若使用當天的DB日期發生錯誤的話就用前一天的日期查詣
    try:
        dbnowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
        dbDateStr = (dbnowZeroTime - datetime.timedelta(days=1)).strftime("%Y%m%d")
        # 取得Table Name
        tableDataFrame = mssqlCtrl.searchSQL(f"SELECT '{dataBase}' as table_catalog ,table_schema ,table_name  ,table_type FROM {dataBase}_{dbDateStr}.INFORMATION_SCHEMA.TABLES")
        # 取得Table的Column Name及資料類型
        columnDataFrame = mssqlCtrl.searchSQL(f"SELECT '{dataBase}' as table_catalog ,table_schema ,table_name ,column_name ,ordinal_position ,data_type FROM {dataBase}_{dbDateStr}.INFORMATION_SCHEMA.COLUMNS")
    except:
        # 取得Table Name
        tableDataFrame = mssqlCtrl.searchSQL(f"SELECT '{dataBase}' as table_catalog ,table_schema ,table_name  ,table_type FROM {dataBase}_{time.strftime('%Y%m%d')}.INFORMATION_SCHEMA.TABLES")
        # 取得Table的Column Name及資料類型
        columnDataFrame = mssqlCtrl.searchSQL(f"SELECT '{dataBase}' as table_catalog ,table_schema ,table_name ,column_name ,ordinal_position ,data_type FROM {dataBase}_{time.strftime('%Y%m%d')}.INFORMATION_SCHEMA.COLUMNS")

    createTableInfo = postgresCtrl.searchSQL(f"SELECT * FROM originalinfo.createtableinfo WHERE table_catalog = '{dataBase}'")
    createColumnsInfo = postgresCtrl.searchSQL(f"SELECT * FROM originalinfo.createtablecolumnsinfo WHERE table_catalog = '{dataBase}'")

    if tableDataFrame.shape[0] > createTableInfo.shape[0]:
        #insert table data
        tableName = 'originalinfo.createtableinfo'
        tableInfoDF = postgresCtrl.getTableInfo(tableName)
        postgresCtrl.executeSQL(f"DELETE FROM {tableName} WHERE table_catalog = '{dataBase}'")
        postgresCtrl.insertData(tableName=tableName, insertTableInfoDF=tableInfoDF, insertDataDF=tableDataFrame)

    if columnDataFrame.shape[0] > createColumnsInfo.shape[0]:
        #insert columns data
        tableName = 'originalinfo.createtablecolumnsinfo'
        tableInfoDF = postgresCtrl.getTableInfo(tableName)
        postgresCtrl.executeSQL(f"DELETE FROM {tableName} WHERE table_catalog = '{dataBase}'")
        postgresCtrl.insertData(tableName=tableName, insertTableInfoDF=tableInfoDF, insertDataDF=columnDataFrame)


def cteateTables(databaseInfo):
    dataBaseArr = databaseInfo["dataBaseArr"]
    tableFilterArray = databaseInfo["tableFilterArray"]
    schmeaNameInit = databaseInfo["schmeaNameInit"]
    filePathInit = databaseInfo["tableFilePathInit"]
    partitionedInit = databaseInfo["tablePartitionedInit"]

    for dataBase in dataBaseArr:
        dataFrameTableFilter = postgresCtrl.searchSQL(f"SELECT * FROM originalinfo.createtableinfo WHERE table_catalog = '{dataBase}'")
        columnDataFrame = postgresCtrl.searchSQL(f"SELECT * FROM originalinfo.createtablecolumnsinfo WHERE table_catalog = '{dataBase}'")

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
        print(f"Start Create {dataBase} Tables.")
        taskCreateSQLStrArr = []
        for tableIndex, tableRow in dataFrameTableFilter.iterrows():
            dataFrameColumnMask_tablename = columnDataFrame["table_name"] == tableRow["table_name"]
            dataFrameColumnFilter = columnDataFrame[dataFrameColumnMask_tablename]
            dataFrameColumnFilter = dataFrameColumnFilter.sort_values(by="ordinal_position", ascending=True)
            tableName = tableRow["table_name"].lower()
            tableFullName = schmeaNameInit.replace("[:dataBase]", dataBase.lower()) + tableName
            coulnmNameStr = ""
            partitionedStr = partitionedInit
            fileFullInit = filePathInit.replace("[:dataBase]", dataBase)

            for columnIndex, columnRow in dataFrameColumnFilter.iterrows():
                columnName = str(columnRow["column_name"]).lower().replace(" ", "")

                if bool(re.search(r"^[A-Za-z0-9_]+$", columnName)) == False:
                    columnName = "column" + str(random.randint(1, 49999))

                if columnRow["ordinal_position"] >= 2:
                    coulnmNameStr = coulnmNameStr + "\n    , "

                for cloumnString in cloumnStringArr:
                    if columnName == cloumnString[0]:
                        columnName = cloumnString[1]

                for dataType in dataTypeArr:
                    if columnRow["data_type"] == dataType[0]:
                        coulnmNameStr = coulnmNameStr + columnName + " " + dataType[1]

            createStr = createStrInit.replace("[:tableName]", tableFullName).replace("[:coulnmName]", coulnmNameStr)
            createStr = createStr.replace("[:partitioned]", partitionedStr).replace("[:filePath]", fileFullInit)
            taskCreateSQLStrArr.append(createStr)
            # print(createStr)

        startRunTime = time.time()
        extracthCtrl.runsql(hiveCtrl, taskCreateSQLStrArr)
        print(f"Created {dataBase} Tables Totals Used", time.time() - startRunTime, "seconds.")


def createPartitions(databaseInfo, makeTime):
    dataBaseArr = databaseInfo["dataBaseArr"]
    tableFilterArray = databaseInfo["tableFilterArray"]
    schmeaNameInit = databaseInfo["schmeaNameInit"]
    filePathInit = databaseInfo["filePathInit"]
    partitionedInit = databaseInfo["partitionedInit"]
    dropPartitionCodeInit = "ALTER TABLE [:TableName] DROP IF EXISTS PARTITION ( [:Partitioned] ) ;"
    alterPartitionCodeInit = "ALTER TABLE [:TableName] ADD IF NOT EXISTS PARTITION ( [:Partitioned] ) LOCATION '[:FilePath]' ;"

    for dataBase in dataBaseArr:
        dataFrameTableFilter = postgresCtrl.searchSQL(f"SELECT * FROM originalinfo.createtableinfo WHERE table_catalog = '{dataBase}'")

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
            tableName_ori = tableRow["table_name"]
            tableName = tableRow["table_name"].lower()
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
        print("Created", dataBase, makeTime.strftime("%Y-%m-%d"), "Partitions Total Used", time.time() - startRunTime, "seconds.")


def cteateWorldTables(databaseInfo):
    gameWorldArr = databaseInfo["gameWorldArr"]
    tableFilterArray = databaseInfo["tableFilterArray"]
    schmeaNameInit = databaseInfo["worldSchmeaNameInit"]
    filePathInit = databaseInfo["worldTableFilePathInit"]
    partitionedInit = databaseInfo["tableWorldPartitionedInit"]

    for dataBase in gameWorldArr:
        dataFrameTableFilter = postgresCtrl.searchSQL(f"SELECT * FROM originalinfo.createtableinfo WHERE table_catalog = '{dataBase[1]}'")
        columnDataFrame = postgresCtrl.searchSQL(f"SELECT * FROM originalinfo.createtablecolumnsinfo WHERE table_catalog = '{dataBase[1]}'")

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
        print(f"Start Create {dataBase[1]} Tables.")
        taskCreateSQLStrArr = []
        for tableIndex, tableRow in dataFrameTableFilter.iterrows():
            dataFrameColumnMask_tablename = columnDataFrame["table_name"] == tableRow["table_name"]
            dataFrameColumnFilter = columnDataFrame[dataFrameColumnMask_tablename]
            dataFrameColumnFilter = dataFrameColumnFilter.sort_values(by="ordinal_position", ascending=True)
            tableName = tableRow["table_name"].lower()
            tableFullName = schmeaNameInit + tableName
            coulnmNameStr = ""
            partitionedStr = partitionedInit
            fileFullInit = filePathInit

            for columnIndex, columnRow in dataFrameColumnFilter.iterrows():
                columnName = str(columnRow["column_name"]).lower().replace(" ", "")

                if bool(re.search(r"^[A-Za-z0-9_]+$", columnName)) == False:
                    columnName = "column" + str(random.randint(1, 49999))

                if columnRow["ordinal_position"] >= 2:
                    coulnmNameStr = coulnmNameStr + "\n    , "

                for cloumnString in cloumnStringArr:
                    if columnName == cloumnString[0]:
                        columnName = cloumnString[1]

                for dataType in dataTypeArr:
                    if columnRow["data_type"] == dataType[0]:
                        coulnmNameStr = coulnmNameStr + columnName + " " + dataType[1]

            createStr = createStrInit.replace("[:tableName]", tableFullName).replace("[:coulnmName]", coulnmNameStr)
            createStr = createStr.replace("[:partitioned]", partitionedStr).replace("[:filePath]", fileFullInit)
            taskCreateSQLStrArr.append(createStr)
            # print(createStr)

        startRunTime = time.time()
        extracthCtrl.runsql(hiveCtrl, taskCreateSQLStrArr)
        print(f"Created {dataBase[1]} Totals Used", time.time() - startRunTime, "seconds.")


def createWorldPartitions(databaseInfo, makeTime):
    dataBaseArr = databaseInfo["gameWorldArr"]
    tableFilterArray = databaseInfo["tableFilterArray"]
    schmeaNameInit = databaseInfo["worldSchmeaNameInit"]
    filePathInit = databaseInfo["worldFilePathInit"]
    partitionedInit = databaseInfo["worldPartitionedInit"]
    dropPartitionCodeInit = "ALTER TABLE [:TableName] DROP IF EXISTS PARTITION ( [:Partitioned] ) ;"
    alterPartitionCodeInit = "ALTER TABLE [:TableName] ADD IF NOT EXISTS PARTITION ( [:Partitioned] ) LOCATION '[:FilePath]' ;"

    for dataBase in dataBaseArr:
        if makeTime <= datetime.datetime.strptime(dataBase[2], "%Y-%m-%d"):
            dataFrameTableFilter = postgresCtrl.searchSQL(f"SELECT * FROM originalinfo.createtableinfo WHERE table_catalog = '{dataBase[1]}'")

            for tableFilter in tableFilterArray:
                dataFrameTableMask = dataFrameTableFilter[tableFilter[0]].str.contains(tableFilter[1])
                if tableFilter[2] == True:
                    dataFrameTableFilter = dataFrameTableFilter[dataFrameTableMask]
                else:
                    dataFrameTableFilter = dataFrameTableFilter[~dataFrameTableMask]

            print(f"Start Create {dataBase[1]} {makeTime.strftime('%Y-%m-%d')} Partitions.")
            taskDropSQLStrArr = []
            taskAlterSQLStrArr = []
            for tableIndex, tableRow in dataFrameTableFilter.iterrows():
                tableName_ori = tableRow["table_name"]
                tableName = tableRow["table_name"].lower()
                tableFullName = schmeaNameInit + tableName
                partitionedStr = partitionedInit.replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:WorldNumber]", dataBase[0])
                filePathStr = filePathInit.replace("[:DBName]", dataBase[1]).replace("[:DateNoLine]", makeTime.strftime("%Y%m%d")).replace("[:TableName]", tableName_ori)
                alterPartitionCode = alterPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr).replace("[:FilePath]", filePathStr)
                dropPartitionCode = dropPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr)

                taskDropSQLStrArr.append(dropPartitionCode)
                taskAlterSQLStrArr.append(alterPartitionCode)
                # print(alterPartitionCode)

            startRunTime = time.time()
            extracthCtrl.runsql(hiveCtrl, taskDropSQLStrArr)
            extracthCtrl.runsql(hiveCtrl, taskAlterSQLStrArr)
            print("Created", dataBase[1], makeTime.strftime("%Y-%m-%d"), "Partitions Total Used", time.time() - startRunTime, "seconds.")


if __name__ == "__main__":
    print('start P10_MakeTableAndPartition')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStr = (nowZeroTime - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    main({"startdate": [makeDateStr], "enddate": [makeDateStr]})
    makeDateStr = (nowZeroTime - datetime.timedelta(days=3)).strftime("%Y-%m-%d")
    main({"startdate": [makeDateStr], "enddate": [makeDateStr]})
    makeDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    main({"startdate": [makeDateStr], "enddate": [makeDateStr]})
    makeDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    main({"startdate": [makeDateStr], "enddate": [makeDateStr]})
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
