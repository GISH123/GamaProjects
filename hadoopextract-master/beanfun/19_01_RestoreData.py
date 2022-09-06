import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.inputCtrl import inputCtrl
from package.common.sshCtrl import SSHCtrl
# from beanfun.P25_MakeModelDataSQL import MakeModelSQLScript
from package.common.telegramCtrl import TelegramCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl
from dotenv import load_dotenv
import pandas
import datetime
import time

load_dotenv(dotenv_path="env/hive.env")
# makeModelSQLScript = MakeModelSQLScript()
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

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

def main(parametersData = {}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    dbName = ""
    tableName = ""
    gameName = ""

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
        if key == "dbName":
            gameName = parametersData[key][0]
        if key == "tableName":
            gameName = parametersData[key][0]

    if makeDateStrArr == []:
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameName = "bf" if gameName == "" else gameName
    dbName = "beanfunDB" if dbName == "" else dbName
    tableName = "History_Play" if tableName == "" else tableName

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
        , "dbName": dbName
        , "tableName": tableName
    }

    insertDataSQL = ""
    # 建立restore資料
    for makeTime in makeTimeArr:
        gameName = databaseInfo["gameName"]
        DateLine = makeTime.strftime("%Y-%m-%d")

        # 確認Table是否存在，若不存在則建立
        month1stDay = datetime.datetime.strptime(str(makeTime.strftime("%Y-%m")) + "-01", "%Y-%m-%d")
        if makeTime == month1stDay:
            chkDataTableCreated(makeTime, databaseInfo)

        # 組建InsertSQL
        insertDataSQL += insertRestoreDataSQL(makeTime, databaseInfo)

    # gameName = databaseInfo["gameName"]
    tableNameLower = str.lower(tableName)
    dateMonth = startDateTime.strftime("%Y%m")
    restoreTableName = f"{gameName}_extract.restore_{tableNameLower}_{dateMonth}"

    restoreDataSQLInitCoed = """ 
        FROM (SELECT * FROM [:restoreTableName]) tmp """
    restoreDataSQL = restoreDataSQLInitCoed.replace("[:restoreTableName]", restoreTableName)
    restoreDataSQL += insertDataSQL

    # 開始執行資料寫入
    print(f"Restore {gameName} {tableName} Data")
    startRunTime = time.time()
    # Model資料寫入
    hiveCtrl.executeSQL_TCByCount(restoreDataSQL, 3)
    # 確認資料是否正常寫入
    checkInsertData(startDateTime, databaseInfo)
    print(f"Restore {gameName} {tableName} Data Total Used {time.time() - startRunTime} seconds.")


def chkDataTableCreated(makeTime, databaseInfo):
    gameName = databaseInfo["gameName"]
    dbName = databaseInfo["dbName"]
    dbNameLower = str.lower(databaseInfo["dbName"])
    tableName = databaseInfo["tableName"]
    tableNameLower = str.lower(databaseInfo["tableName"])
    dateMonth = str(makeTime.strftime("%Y%m"))

    fullTableName = f"{gameName}_extract.restore_{tableNameLower}_{dateMonth}"
    likeTableName = f"{gameName}_extract.{dbNameLower}_{tableNameLower}"
    locationPath = f"/user/hive/warehouse/{gameName}_extract.db/all/{dbName}"
    dateNoLine = f"{dateMonth}04"
    partitionPath = f"{locationPath}/dt={dateNoLine}/{tableName}"

    try:
        # 確認Table是否存在
        hiveCtrl.executeSQL(f"show columns in {gameName}_extract.restore_{tableNameLower}_{dateMonth}")
    except:
        createSQLInitCode = "CREATE External TABLE IF NOT EXISTS [:fullTableName] \n" \
                    "LIKE [:likeTableName]  \n" \
                    "LOCATION '[:locationPath]' \n"
        # 建立Table
        createSQL = createSQLInitCode.replace('[:fullTableName]', fullTableName)
        createSQL = createSQL.replace("[:likeTableName]", likeTableName).replace("[:locationPath]", locationPath)
        hiveCtrl.executeSQL_TCByCount(createSQL, 3)

    # 建立Partition
    checkPartitionCodeInit = "SHOW PARTITIONS [:fullTableName] PARTITION ( dt=[:dateNoLine] ) "
    alterPartitionCodeInit = "ALTER TABLE [:fullTableName] ADD IF NOT EXISTS PARTITION (dt='[:dateNoLine]') LOCATION '[:partitionPath]'"

    # 確認Partition是否建立
    checkPartition = checkPartitionCodeInit.replace("[:fullTableName]", fullTableName).replace("[:dateNoLine]", dateNoLine)
    if hiveCtrl.searchSQL_TCByCount(checkPartition, 3).count()['partition'] == 0:
        alterPartition = alterPartitionCodeInit.replace("[:fullTableName]", fullTableName).replace("[:dateNoLine]",dateNoLine)
        alterPartition = alterPartition.replace("[:partitionPath]", partitionPath)
        hiveCtrl.executeSQL_TCByCount(alterPartition, 3)


def checkInsertData(makeTime, databaseInfo):
    gameName = databaseInfo["gameName"]
    dbNameLower = str.lower(databaseInfo["dbName"])
    tableNameLower = str.lower(databaseInfo["tableName"])
    dateMonth = str(makeTime.strftime("%Y%m"))

    checkFloderSizeInitCode = "hadoop fs -du -s -h /user/hive/warehouse/[:gameName]_extract.db/[:dbNameLower]/[:tableNameLower]/dt=[:dateMonth]?? |grep \'0  0\'"
    checkFloderSize = checkFloderSizeInitCode.replace("[:gameName]", gameName).replace("[:dbNameLower]", dbNameLower)
    checkFloderSize = checkFloderSize.replace("[:tableNameLower]", tableNameLower).replace("[:dateMonth]", dateMonth)
    zeroFloderData = sshCtrl_hdfs.ssh_exec_cmd_return(checkFloderSize)

    # 取得空資料夾的路徑
    zeroFloder = [folder.split('  ')[-1] for folder in zeroFloderData.split("\r\n")]
    if zeroFloder[0] == '':
        # print(f"Import LoginData {datenoline} Data Success.")
        pass
    else:
        for folder in zeroFloder:
            if folder != '':
                message = u'\U0000203C' + f"[{gameName}] Info: {str(folder)} is empty, please check."
                print(message)
                # 送出Telegrame告警
                # telegramSend(message)


def telegramSend(message):
    try:
        telegramCtrl.sendMessageByUrl(url=os.getenv("BDA_TELEGRAM_URL"), userid=os.getenv("BDA_TELEGRAM_USERID"), massage=message)
        pass
    except Exception as e:
        print(e)
        pass


def insertRestoreDataSQL(makeTime, databaseInfo):
    insertDataSQLInitCoed = """
        INSERT OVERWRITE TABLE [:toTableName] PARTITION(dt='[:dateNoLine]')
        SELECT guid,serverindex,mainaccountid,servicecode,serviceregion,serviceaccountid,logintime,logouttime
        ,chargerule,chargepoints,chargeservicepoints,ipaddress,memo,createtime,transactionid,ticketid
        WHERE logouttime >= DATE_ADD('[:dateLine]',0)
        AND   logouttime < DATE_ADD('[:dateLine]',1) """

    gameName = databaseInfo["gameName"]
    dbNameLower = str.lower(databaseInfo["dbName"])
    tableNameLower = str.lower(databaseInfo["tableName"])
    dateMonth = str(makeTime.strftime("%Y%m"))

    restoreTableName = f"{gameName}_extract.restore_{tableNameLower}_{dateMonth}"
    toTableName = f"{gameName}_extract.{dbNameLower}_{tableNameLower}"
    dateNoLine = makeTime.strftime("%Y%m%d")
    dateLine = makeTime.strftime("%Y-%m-%d")

    insertDataSQL = insertDataSQLInitCoed.replace("[:toTableName]", toTableName)
    insertDataSQL = insertDataSQL.replace("[:dateNoLine]", dateNoLine).replace("[:dateLine]", dateLine)

    return insertDataSQL

if __name__ == "__main__":
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    # 只能一個月一個月的執行，月份暫無法判斷
    # main({"startdate": ["2020-05-01"], "enddate": ["2020-05-31"]})
    main({"startdate": ["2020-06-01"], "enddate": ["2020-06-30"]})
    main({"startdate": ["2020-07-01"], "enddate": ["2020-07-31"]})
    main({"startdate": ["2020-08-01"], "enddate": ["2020-08-31"]})
    main({"startdate": ["2020-09-01"], "enddate": ["2020-09-30"]})
    main({"startdate": ["2020-10-01"], "enddate": ["2020-10-31"]})
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))

