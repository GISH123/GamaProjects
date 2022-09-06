import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.sshCtrl import SSHCtrl
from package.common.database.postgreCtrl import PostgresCtrl
from package.common.telegramCtrl import TelegramCtrl
from dotenv import load_dotenv
import sys, re , datetime
import random, pandas, time

sshCtrl_hdfs = SSHCtrl("10.10.99.133", 22, "hdfs", pkey="env/ALL_PKEY_HDFS")
sshCtrl = SSHCtrl(env="env/ETL03_SSH.env")
load_dotenv(dotenv_path="env/Telegram_GAA.env")
load_dotenv(dotenv_path="env/PostgreSQL.env")

postgresCtrl = PostgresCtrl(
    host=os.getenv("POSTGRES_HOST182")
    , port=os.getenv("POSTGRES_PORT")
    , user=os.getenv("POSTGRES_USER")
    , password=os.getenv("POSTGRES_PASSWORD")
    , database="Hadoop"
    , schema="sizecheck"
)

greenplumCtrl = PostgresCtrl(
    host=os.getenv("POSTGRES_HOST_Greenplum")
    , port=os.getenv("POSTGRES_PORT")
    , user=os.getenv("POSTGRES_USER_Greenplum")
    , password=os.getenv("POSTGRES_PASSWORD")
    , database="hadoopcheck"
    , schema="sizecheck"
)

telegramCtrl = TelegramCtrl()

pandas.set_option("display.max_columns", None)
pandas.set_option("display.max_rows", None)
pandas.set_option("display.width", 1000)


def main():
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    '''startDateStr = (nowZeroTime - datetime.timedelta(days=4)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=0)).strftime("%Y-%m-%d")
    # startDateStr = "2019-12-01"
    # endDateStr = "2020-07-28"
    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime
    while makeTime <= endDateTime:'''
    makeTimeArr = [
        nowZeroTime - datetime.timedelta(days=1)
        , nowZeroTime - datetime.timedelta(days=2)
        , nowZeroTime - datetime.timedelta(days=3)
        , nowZeroTime - datetime.timedelta(days=4)
        , nowZeroTime - datetime.timedelta(days=5)
        , nowZeroTime - datetime.timedelta(days=6)
        , nowZeroTime - datetime.timedelta(days=7)
        , nowZeroTime - datetime.timedelta(days=8)
        , nowZeroTime - datetime.timedelta(days=9)
        , nowZeroTime - datetime.timedelta(days=10)
        , nowZeroTime - datetime.timedelta(days=15)
        , nowZeroTime - datetime.timedelta(days=20)
        , nowZeroTime - datetime.timedelta(days=25)
        , nowZeroTime - datetime.timedelta(days=30)
    ]
    for makeTime in makeTimeArr:
        databaseInfo = {
            "gameNameArr": ["bf", "bnb", "cso", "els", "kr", "lineage", "mabi", "maple", "tdn", "gtwpd", "all", "root", "wod"]
            # "gameNameArr": ["maple"]
            , "dbName": "*"
        }

        checkSourceSize(makeTime, databaseInfo)
        checkExtractSize(makeTime, databaseInfo)
        checkExtractNoBakSize(makeTime, databaseInfo)
        getAllSize(makeTime, databaseInfo)
        toKibana(makeTime)
        makeTime = makeTime + datetime.timedelta(days=1)


def checkSourceSize(makeTime, databaseInfo):
    gameNameArr = databaseInfo["gameNameArr"]
    dbName = databaseInfo["dbName"]
    dateNoLine = str(makeTime.strftime("%Y%m%d"))
    dateLine = str(makeTime.strftime("%Y-%m-%d"))
    postgresTableName = "sizecheck.sizecheck"

    checkDataSQLInitCode = "SELECT SUM(1) as cnt FROM [:postgresTableName] WHERE datetime = '[:DateLine]' AND type = 'Source'"
    checkSourceSizeInitCode = "hdfs dfs -du -s /user/hive/warehouse/[:GameName].db/ALL/[:DBName]/dt=[:DateNoLine]/* | awk \'{SUM += $0} END {print SUM}\'"
    updateDataSQLInitCode = "UPDATE [:postgresTableName] SET size = [:SizeData] WHERE datetime = '[:DateLine]' AND type = 'Source' AND gamename = '[:GameName]'"
    insertDataList = []
    updateDataList = []

    checkDataSQL = checkDataSQLInitCode.replace("[:postgresTableName]", postgresTableName).replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
    chedkDataCnt = postgresCtrl.searchSQL(checkDataSQL).count()["cnt"]

    print(f"Run [checkSourceSize] Data")
    startRunTime = time.time()
    for gameName in gameNameArr:
        if gameName in ["all", "gtwpd", "root"]:
            continue
        else:
            checkSourceSize = checkSourceSizeInitCode.replace("[:GameName]", gameName).replace("[:DBName]", dbName).replace("[:DateNoLine]", dateNoLine)
            # zeroFloderData = sshCtrl_hdfs.ssh_exec_cmd_return('hdfs dfs -du -s -h /user/hive/warehouse/bf.db/ALL/beanfunDB/dt=202007??/* |grep "0  0"')
            SourceSize = sshCtrl_hdfs.ssh_exec_cmd_return(checkSourceSize)

        if chedkDataCnt == 0:
            insertData = {}
            insertData['gamename'] = gameName
            insertData['datetime'] = dateLine
            insertData['type'] = "Source"
            if "No such file or directory" in SourceSize.split("\r\n")[0] or "File does not exist" in SourceSize.split("\r\n")[0]:
                insertData['size'] = '0'
            elif SourceSize.split("\r\n")[0] == '':
                insertData['size'] = '0'
            else:
                insertData['size'] = SourceSize.split("\r\n")[0]
            insertData['kibana_datetime'] = dateNoLine + "T00:00:00,000Z"
            insertDataList.append(insertData)
        else:
            updateDataSQL = updateDataSQLInitCode.replace("[:postgresTableName]", postgresTableName)
            if "No such file or directory" in SourceSize.split("\r\n")[0] or "File does not exist" in SourceSize.split("\r\n")[0]:
                updateDataSQL = updateDataSQL.replace("[:SizeData]", str('0'))
            elif SourceSize.split("\r\n")[0] == '':
                updateDataSQL = updateDataSQL.replace("[:SizeData]", str('0'))
            else:
                updateDataSQL = updateDataSQL.replace("[:SizeData]", SourceSize.split("\r\n")[0])
            updateDataSQL = updateDataSQL.replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
            updateDataSQL = updateDataSQL.replace("[:GameName]", gameName)
            # print(updateDataSQL)
            updateDataList.append(updateDataSQL)

    if chedkDataCnt == 0:
        insertDataObjectDF = pandas.core.frame.DataFrame(insertDataList)
        tableInfoDF = postgresCtrl.getTableInfo(postgresTableName)
        postgresCtrl.insertData(tableName=postgresTableName, insertTableInfoDF=tableInfoDF, insertDataDF=insertDataObjectDF)
        tableInfoDF = greenplumCtrl.getTableInfo(postgresTableName)
        greenplumCtrl.insertData(tableName=postgresTableName, insertTableInfoDF=tableInfoDF,insertDataDF=insertDataObjectDF)
    else:
        for updateSQL in updateDataList:
            postgresCtrl.executeSQL(updateSQL)
            greenplumCtrl.executeSQL(updateSQL)
    print(f"Insert [checkSourceSize] Data Total Used {time.time() - startRunTime} seconds.")


def checkExtractSize(makeTime, databaseInfo):
    gameNameArr = databaseInfo["gameNameArr"]
    dbName = databaseInfo["dbName"]
    dateNoLine = str(makeTime.strftime("%Y%m%d"))
    dateLine = str(makeTime.strftime("%Y-%m-%d"))
    postgresTableName = "sizecheck.sizecheck"

    checkDataSQLInitCode = "SELECT SUM(1) as cnt FROM [:postgresTableName] WHERE datetime = '[:DateLine]' AND type = 'Extract'"
    checkExtractSizeInitCode = "hdfs dfs -du -s /user/hive/warehouse/[:GameName]_extract.db/[:DBName]/*/dt=[:DateNoLine]/* | awk \'{SUM += $0} END {print SUM}\'"
    updateDataSQLInitCode = "UPDATE [:postgresTableName] SET size = [:SizeData] WHERE datetime = '[:DateLine]' AND type = 'Extract' AND gamename = '[:GameName]'"
    insertDataList = []
    updateDataList = []

    checkDataSQL = checkDataSQLInitCode.replace("[:postgresTableName]", postgresTableName).replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
    chedkDataCnt = postgresCtrl.searchSQL(checkDataSQL).count()["cnt"]

    print(f"Run [checkExtractSize] Data")
    startRunTime = time.time()
    for gameName in gameNameArr:
        if gameName in ["all", "gtwpd", "root"]:
            continue
        else:
            checkExtractSize = checkExtractSizeInitCode.replace("[:GameName]", gameName).replace("[:DBName]", dbName).replace("[:DateNoLine]", dateNoLine)
            # zeroFloderData = sshCtrl_hdfs.ssh_exec_cmd_return('hdfs dfs -du -s -h /user/hive/warehouse/bf.db/ALL/beanfunDB/dt=202007??/* |grep "0  0"')
            if gameName == "maple" and makeTime.day == 1:
                MapleExtractBackupSize = sshCtrl_hdfs.ssh_exec_cmd_return(checkExtractSize)
                checkExtractSize = checkExtractSize.replace("/*/*/", "/*/")
            elif gameName == "maple":
                checkExtractSize = checkExtractSize.replace("/*/*/", "/*/")

        ExtractSize = sshCtrl_hdfs.ssh_exec_cmd_return(checkExtractSize)

        if chedkDataCnt == 0:
            insertData = {}
            insertData['gamename'] = gameName
            insertData['datetime'] = dateLine
            insertData['type'] = "Extract"
            if "No such file or directory" in ExtractSize.split("\r\n")[0] or "File does not exist" in ExtractSize.split("\r\n")[0]:
                insertData['size'] = '0'
            elif gameName == "maple" and makeTime.day == 1:
                if "No such file or directory" in MapleExtractBackupSize.split("\r\n")[0] or "File does not exist" in MapleExtractBackupSize.split("\r\n")[0]:
                    MapleExtractSize = int(ExtractSize.split("\r\n")[0]) + int(0)
                elif MapleExtractBackupSize.split("\r\n")[0] == '':
                    MapleExtractSize = int(ExtractSize.split("\r\n")[0]) + int(0)
                else:
                    MapleExtractSize = int(ExtractSize.split("\r\n")[0]) + int(MapleExtractBackupSize.split("\r\n")[0])
                insertData['size'] = str(MapleExtractSize)
            elif ExtractSize.split("\r\n")[0] == '':
                insertData['size'] = '0'
            else:
                insertData['size'] = ExtractSize.split("\r\n")[0]
            insertData['kibana_datetime'] = dateNoLine + "T00:00:00,000Z"
            # print(insertData)
            insertDataList.append(insertData)
        else:
            updateDataSQL = updateDataSQLInitCode.replace("[:postgresTableName]", postgresTableName)
            if "No such file or directory" in ExtractSize.split("\r\n")[0] or "File does not exist" in ExtractSize.split("\r\n")[0]:
                updateDataSQL = updateDataSQL.replace("[:SizeData]", str('0'))
            elif gameName == "maple" and makeTime.day == 1:
                if "No such file or directory" in MapleExtractBackupSize.split("\r\n")[0] or "File does not exist" in MapleExtractBackupSize.split("\r\n")[0]:
                    MapleExtractSize = int(ExtractSize.split("\r\n")[0]) + int(0)
                elif MapleExtractBackupSize.split("\r\n")[0] == '':
                    MapleExtractSize = int(ExtractSize.split("\r\n")[0]) + int(0)
                else:
                    MapleExtractSize = int(ExtractSize.split("\r\n")[0]) + int(MapleExtractBackupSize.split("\r\n")[0])
                updateDataSQL = updateDataSQL.replace("[:SizeData]", str(MapleExtractSize))
            elif ExtractSize.split("\r\n")[0] == '':
                updateDataSQL = updateDataSQL.replace("[:SizeData]", str('0'))
            else:
                updateDataSQL = updateDataSQL.replace("[:SizeData]", ExtractSize.split("\r\n")[0])
            updateDataSQL = updateDataSQL.replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
            updateDataSQL = updateDataSQL.replace("[:GameName]", gameName)
            # print(updateDataSQL)
            updateDataList.append(updateDataSQL)

    if chedkDataCnt == 0:
        insertDataObjectDF = pandas.core.frame.DataFrame(insertDataList)
        tableInfoDF = postgresCtrl.getTableInfo(postgresTableName)
        postgresCtrl.insertData(tableName=postgresTableName, insertTableInfoDF=tableInfoDF, insertDataDF=insertDataObjectDF)
        tableInfoDF = greenplumCtrl.getTableInfo(postgresTableName)
        greenplumCtrl.insertData(tableName=postgresTableName, insertTableInfoDF=tableInfoDF, insertDataDF=insertDataObjectDF)
    else:
        for updateSQL in updateDataList:
            postgresCtrl.executeSQL(updateSQL)
            greenplumCtrl.executeSQL(updateSQL)
    print(f"Insert [checkExtractSize] Data Total Used {time.time() - startRunTime} seconds.")


def checkExtractNoBakSize(makeTime, databaseInfo):
    gameNameArr = databaseInfo["gameNameArr"]
    dbName = databaseInfo["dbName"]
    dateNoLine = str(makeTime.strftime("%Y%m%d"))
    dateLine = str(makeTime.strftime("%Y-%m-%d"))
    postgresTableName = "sizecheck.extractsize"

    checkDataSQLInitCode = "SELECT SUM(1) as cnt FROM [:postgresTableName] WHERE datetime = '[:DateLine]' AND type = 'ExtractNoBak'"
    checkExtractSizeInitCode = "hadoop fs -du -s /user/hive/warehouse/[:GameName]_extract.db/[:DBName]/*/dt=[:DateNoLine]/ | grep -v /all | awk \'{SUM += $0} END {print SUM}\'"
    updateDataSQLInitCode = "UPDATE [:postgresTableName] SET size = [:SizeData] WHERE datetime = '[:DateLine]' AND type = 'ExtractNoBak' AND gamename = '[:GameName]'"
    insertDataList = []
    updateDataList = []

    checkDataSQL = checkDataSQLInitCode.replace("[:postgresTableName]", postgresTableName).replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
    chedkDataCnt = postgresCtrl.searchSQL(checkDataSQL).count()["cnt"]

    print(f"Run [checkExtractNoBakSize] Data")
    startRunTime = time.time()
    for gameName in gameNameArr:
        if gameName in ["all", "gtwpd", "root"]:
            continue
        else:
            checkExtractSize = checkExtractSizeInitCode.replace("[:GameName]", gameName).replace("[:DBName]", dbName).replace("[:DateNoLine]", dateNoLine)
            if gameName == "maple":
                checkExtractSize = checkExtractSize.replace("/*/*/", "/*/")

        ExtractSize = sshCtrl_hdfs.ssh_exec_cmd_return(checkExtractSize)
        if chedkDataCnt == 0:
            insertData = {}
            insertData['gamename'] = gameName
            insertData['datetime'] = dateLine
            insertData['type'] = "ExtractNoBak"
            if "No such file or directory" in ExtractSize.split("\r\n")[0] or "File does not exist" in ExtractSize.split("\r\n")[0]:
                insertData['size'] = '0'
            elif ExtractSize.split("\r\n")[0] == '':
                insertData['size'] = '0'
            else:
                insertData['size'] = ExtractSize.split("\r\n")[0]
            insertData['kibana_datetime'] = dateNoLine + "T00:00:00,000Z"
            # print(insertData)
            insertDataList.append(insertData)
        else:
            updateDataSQL = updateDataSQLInitCode.replace("[:postgresTableName]", postgresTableName)
            if "No such file or directory" in ExtractSize.split("\r\n")[0] or "File does not exist" in ExtractSize.split("\r\n")[0]:
                updateDataSQL = updateDataSQL.replace("[:SizeData]", str('0'))
            elif ExtractSize.split("\r\n")[0] == '':
                updateDataSQL = updateDataSQL.replace("[:SizeData]", str('0'))
            else:
                updateDataSQL = updateDataSQL.replace("[:SizeData]", ExtractSize.split("\r\n")[0])
            updateDataSQL = updateDataSQL.replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
            updateDataSQL = updateDataSQL.replace("[:GameName]", gameName)
            # print(updateDataSQL)
            updateDataList.append(updateDataSQL)

    if chedkDataCnt == 0:
        insertDataObjectDF = pandas.core.frame.DataFrame(insertDataList)
        tableInfoDF = postgresCtrl.getTableInfo(postgresTableName)
        postgresCtrl.insertData(tableName=postgresTableName, insertTableInfoDF=tableInfoDF, insertDataDF=insertDataObjectDF)
        tableInfoDF = greenplumCtrl.getTableInfo(postgresTableName)
        greenplumCtrl.insertData(tableName=postgresTableName, insertTableInfoDF=tableInfoDF,insertDataDF=insertDataObjectDF)
    else:
        for updateSQL in updateDataList:
            postgresCtrl.executeSQL(updateSQL)
            greenplumCtrl.executeSQL(updateSQL)
    print(f"Insert [checkExtractNoBakSize] Data Total Used {time.time() - startRunTime} seconds.")


def getAllSize(makeTime, databaseInfo):
    gameNameArr = databaseInfo["gameNameArr"]
    dbName = databaseInfo["dbName"]
    dateNoLine = str(makeTime.strftime("%Y%m%d"))
    dateLine = str(makeTime.strftime("%Y-%m-%d"))
    postgresTableName = "sizecheck.sizecheck"

    checkDataSQLInitCode = "SELECT SUM(1) as cnt FROM [:postgresTableName] WHERE datetime = '[:DateLine]' AND type = 'All'"
    checkGameAllSizeInitCode = "hdfs dfs -du -s /user/hive/warehouse/[:GameName]*.db/ | awk \'{SUM += $0} END {print SUM}\'"
    updateDataSQLInitCode = "UPDATE [:postgresTableName] SET size = [:SizeData] WHERE datetime = '[:DateLine]' AND type = 'All' AND gamename = '[:GameName]'"
    insertDataList = []
    updateDataList = []

    checkDataSQL = checkDataSQLInitCode.replace("[:postgresTableName]", postgresTableName).replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
    chedkDataCnt = postgresCtrl.searchSQL(checkDataSQL).count()["cnt"]

    print(f"Run [getAllSize] Data")
    startRunTime = time.time()
    if makeTime.strftime("%Y-%m-%d") == (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y-%m-%d"):
        for gameName in gameNameArr:
            if gameName == "all":
                checkAllSize = "hdfs dfs -du -s /user/hive/warehouse/ | awk \'{SUM += $0} END {print SUM}\'"
                GameAllSize = sshCtrl_hdfs.ssh_exec_cmd_return(checkAllSize)
            elif gameName == "root":
                checkAllSize = "hdfs dfs -du -s / | awk \'{SUM += $0} END {print SUM}\'"
                GameAllSize = sshCtrl_hdfs.ssh_exec_cmd_return(checkAllSize)
            else:
                checkGameAllSize = checkGameAllSizeInitCode.replace("[:GameName]", gameName)
                # zeroFloderData = sshCtrl_hdfs.ssh_exec_cmd_return('hdfs dfs -du -s -h /user/hive/warehouse/bf.db/ALL/beanfunDB/dt=202007??/* |grep "0  0"')
                GameAllSize = sshCtrl_hdfs.ssh_exec_cmd_return(checkGameAllSize)

            if chedkDataCnt == 0:
                insertData = {}
                insertData['gamename'] = gameName
                insertData['datetime'] = dateLine
                insertData['type'] = "All"
                if "No such file or directory" in GameAllSize.split("\r\n")[0] or "File does not exist" in GameAllSize.split("\r\n")[0]:
                    insertData['size'] = '0'
                elif GameAllSize.split("\r\n")[0] == '':
                    insertData['size'] = '0'
                else:
                    insertData['size'] = GameAllSize.split("\r\n")[0]
                insertData['kibana_datetime'] = dateNoLine + "T00:00:00,000Z"
                # print(insertData)
                insertDataList.append(insertData)
            else:
                updateDataSQL = updateDataSQLInitCode.replace("[:postgresTableName]", postgresTableName)
                if "No such file or directory" in GameAllSize.split("\r\n")[0] or "File does not exist" in GameAllSize.split("\r\n")[0]:
                    updateDataSQL = updateDataSQL.replace("[:SizeData]", str('0'))
                elif GameAllSize.split("\r\n")[0] == '':
                    updateDataSQL = updateDataSQL.replace("[:SizeData]", str('0'))
                else:
                    updateDataSQL = updateDataSQL.replace("[:SizeData]", GameAllSize.split("\r\n")[0])
                updateDataSQL = updateDataSQL.replace("[:DateLine]", makeTime.strftime("%Y-%m-%d"))
                updateDataSQL = updateDataSQL.replace("[:GameName]", gameName)
                # print(updateDataSQL)
                updateDataList.append(updateDataSQL)

        if chedkDataCnt == 0:
            insertDataObjectDF = pandas.core.frame.DataFrame(insertDataList)
            tableInfoDF = postgresCtrl.getTableInfo(postgresTableName)
            postgresCtrl.insertData(tableName=postgresTableName, insertTableInfoDF=tableInfoDF, insertDataDF=insertDataObjectDF)
            tableInfoDF = greenplumCtrl.getTableInfo(postgresTableName)
            greenplumCtrl.insertData(tableName=postgresTableName, insertTableInfoDF=tableInfoDF, insertDataDF=insertDataObjectDF)
        else:
            for updateSQL in updateDataList:
                postgresCtrl.executeSQL(updateSQL)
                greenplumCtrl.executeSQL(updateSQL)
    print(f"Insert [getAllSize] Data Total Used {time.time() - startRunTime} seconds.")


def toKibana(makeTime):
    querySQLInitCode = "SELECT * FROM [:postgresTableName] WHERE datetime = '[:DateLine]'"
    dateNoLine = str(makeTime.strftime("%Y%m%d"))
    dateLine = str(makeTime.strftime("%Y-%m-%d"))
    postgresTableName = "sizecheck.sizecheck"
    datetime_str =dateLine + "T00:00:00,000Z"

    querySQL = querySQLInitCode.replace("[:postgresTableName]", postgresTableName).replace("[:DateLine]", dateLine)
    queryData = postgresCtrl.searchSQL(querySQL)
    # print(queryData)
    # Linux用
    queryData.to_csv('../../HadoopDataSizeLog/HadoopDataSize_' + dateNoLine + '.log', index=False, encoding="utf_8_sig", sep=" ")
    # PyCharm用
    # queryData.to_csv('./HadoopDataSize_' + dateNoLine + '.log', index=False, encoding="utf_8_sig", sep=" ")
    # print(datetime_str)
    # print("curl -XPOST \"http://10.10.99.192:9200/hadoop_datasize/_delete_by_query\" -H 'Content-Type:application/json' -d '{\"query\": {\"match\": {\"datatime\":\"'" + dateLine + "'\"}}}'")
    sshCtrl.ssh_exec_cmd("curl -XPOST \"http://10.10.99.192:9200/hadoop_datasize/_delete_by_query\" -H 'Content-Type:application/json' -d '{\"query\": {\"match\": {\"datatime\":\"'" + dateLine + "'\"}}}'")
    sshCtrl.ssh_exec_cmd("nc 10.10.99.192 5068 < /dfs01/Docker/PythonBD/Volumes/Data/HadoopDataSizeLog/HadoopDataSize_" + dateNoLine + ".log")


if __name__ == "__main__":
    print('start CheckHadoopDataSize')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
