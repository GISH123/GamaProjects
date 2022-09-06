import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.sshCtrl import SSHCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hiveCtrl_2 import HiveSingleCtrl
from package.common.telegramCtrl import TelegramCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl
from dotenv import load_dotenv
import pandas, datetime, time, asyncio
import re, random

load_dotenv(dotenv_path="env/hdfs.env")
load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/Telegram_GAA.env")

sshCtrl_hdfs = SSHCtrl("10.10.99.133", 22, "hdfs", pkey="env/ALL_PKEY_HDFS")

hdfsCtrl = HDFSCtrl(
    url=os.getenv("HDFS_HOST")
    , user=os.getenv("HDFS_USER")
    , password=os.getenv("HDFS_PASSWD")
    , filePath=os.getenv("HDFS_PATH")
)

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

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

start_time = time.time()


def main():
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - datetime.timedelta(days=4)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    # 開始時間
    startDateStr = "2020-07-01"
    # 結束時間
    endDateStr = "2020-07-01"
    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime

    while makeTime <= endDateTime:
        deleteForderData = {
            "deletePath": "/user/GTW_PD/DB/BUSINESSUNIT/bureport/game=lineager/dt=[:dateNoLine]"
            , "deleteFolderArr": [
                "world=1",
                "world=2",
                "world=3",
                "world=5",
                "world=39",
                "world=101",
                "world=102",
                "world=103",
                "world=106",
                "world=123",
            ]
        }

        chkDropPartitionData = {
            "chkDefaultPath": "/user/GTW_PD/DB/BUSINESSUNIT/bureport/"
            # "chkDefaultPath": "/user/hive/warehouse/[:gameName].db/[:dbName]/[:tableName]/"
            , "gameName": "gtwpd"
            , "dbName": "businessunit"
            , "tableName": "bureport"
            , "tableFullName": "gtwpd.businessunit_bureport"
            , "showPartitionInitCode": "show partitions [:tableFullName] partition(game='[:partitionName]',dt='[:dateNoLine]')"
            , "dropPartitionInitCode": "ALTER TABLE [:tableFullName] DROP IF EXISTS PARTITION ( [:partitioned] ) ;"
            , "partitionNameArr": [
                "lineager"
            ]
        }

        # 大量刪除資料夾
        # deleteFolder(makeTime, deleteForderData)
        dropNoDataPartition(makeTime, chkDropPartitionData)

        makeTime = makeTime + datetime.timedelta(days=1)
    pass


def deleteFolder(makeTime, deleteForderData):
    dateNoLine = makeTime.strftime("%Y%m%d")
    deletePath = deleteForderData["deletePath"]
    deleteFolderArr = deleteForderData["deleteFolderArr"]
    # deletePath = f"/user/GTW_PD/DB/BUSINESSUNIT/bureport/game=lineager/dt={dateNoLine}"

    deletePath = deletePath.replace("[:dateNoLine]", dateNoLine)
    for deleteFolder in deleteFolderArr:
        deleteFolderCode = f"hdfs dfs -rm -r -f {deletePath}/{deleteFolder}"
        print(deleteFolderCode)
        # sshCtrl_hdfs.ssh_exec_cmd(deleteFolderCode)


def dropNoDataPartition(makeTime, chkDropPartitionData):
    dateNoLine = makeTime.strftime("%Y%m%d")
    chkDefaultPath = chkDropPartitionData["chkDefaultPath"]
    tableFullName = chkDropPartitionData["tableFullName"]
    partitionNameArr = chkDropPartitionData["partitionNameArr"]
    showPartitionInitCode = chkDropPartitionData["showPartitionInitCode"]
    dropPartitionInitCode = chkDropPartitionData["dropPartitionInitCode"]
    checkFloderSizeInitCode = "hdfs dfs -du -s -h [:chkPath]"

    dropPartitionSQLArr = []
    for partitionName in partitionNameArr:
        showPartitionSQL = showPartitionInitCode.replace("[:tableFullName]", tableFullName)
        showPartitionSQL = showPartitionSQL.replace("[:dateNoLine]", dateNoLine).replace("[:partitionName]", partitionName)
        # 取得已建立的Partition資訊
        chkPartitionData = hiveCtrl.searchSQL_TCByCount(showPartitionSQL, 3)
        # print(chkPartitionData)
        for pf in chkPartitionData['partition']:
            # 取得Partition的路徑
            chkPath = f"{chkDefaultPath}{pf}"
            checkFloderSize = checkFloderSizeInitCode.replace("[:chkPath]", chkPath)
            checkFloder = sshCtrl_hdfs.ssh_exec_cmd_return(checkFloderSize)
            # 若資料夾不存在則Drop Partition
            if "No such file or directory" in checkFloder:
                partitionArr = pf.split("/")
                # print(partitionArr)
                partitioned = ""
                for partition in partitionArr:
                    gn = partition.split("=")[0]
                    pn = partition.split("=")[1]
                    partitioned += f"{gn}='{pn}'"
                    if partition != partitionArr[-1]:
                        partitioned += ","
                dropPartition = dropPartitionInitCode.replace("[:tableFullName]", tableFullName)
                dropPartition = dropPartition.replace("[:partitioned]", partitioned)
            # print(dropPartition)
            dropPartitionSQLArr.append(dropPartition)
        # print(dropPartitionSQLArr)
        extracthCtrl.runsql(hiveCtrl, dropPartitionSQLArr)



if __name__ == "__main__":
    print('start DeleteFolder')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))