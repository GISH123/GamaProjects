import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.sshCtrl import SSHCtrl
from package.common.telegramCtrl import TelegramCtrl
from dotenv import load_dotenv
import sys , re , datetime
import random , pandas , time

sshCtrl_hdfs = SSHCtrl("10.10.99.133", 22, "hdfs", pkey="env/ALL_PKEY_HDFS")
load_dotenv(dotenv_path="env/Telegram_GAA.env")

telegramCtrl = TelegramCtrl()

pandas.set_option("display.max_columns", None)
pandas.set_option("display.max_rows", None)
pandas.set_option("display.width", 1000)


def main():
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=0)).strftime("%Y-%m-%d")
    # startDateStr = "2020-09-15"
    # endDateStr = "2020-09-18"
    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime
    while makeTime <= endDateTime:
        databaseInfo = {
            "gameName": "mabi"
            , "dbName": "*"
        }
        checkImportData(makeTime, databaseInfo)
        # checkExtractData(makeTime, databaseInfo)
        makeTime = makeTime + datetime.timedelta(days=1)


def checkImportData(makeTime, databaseInfo):
    gameName = databaseInfo["gameName"]
    dbName = databaseInfo["dbName"]
    datenoline = str(makeTime.strftime("%Y%m%d"))
    checkFloderSizeInitCode = "hdfs dfs -du -s -h /user/hdfs/mabi/[:DBName]/date=[:DateNoLine]/* |grep \'0  0\'"
    checkFloderSize = checkFloderSizeInitCode.replace("[:GameName]", gameName).replace("[:DBName]", dbName).replace("[:DateNoLine]", datenoline)
    # zeroFloderData = sshCtrl_hdfs.ssh_exec_cmd_return('hdfs dfs -du -s -h /user/hive/warehouse/bf.db/ALL/beanfunDB/dt=202007??/* |grep "0  0"')
    zeroFloderData = sshCtrl_hdfs.ssh_exec_cmd_return(checkFloderSize)

    # 取得空資料夾的路徑
    zeroFloder = [floder.split('  ')[-1] for floder in zeroFloderData.split("\r\n")]
    if zeroFloder[0] == '':
        print(makeTime.strftime("%Y-%m-%d"), "All Source Import Data Success.")
        pass
    else:
        for floder in zeroFloder:
            if floder != '':
                message = u'\U0000203C' + f"[{gameName}] Info: {str(floder)} is empty, please check."
                print(message)
                # 送出Telegrame告警
                try:
                    telegramCtrl.sendMessageByUrl(url=os.getenv("BDA_TELEGRAM_URL"), userid=os.getenv("BDA_TELEGRAM_USERID"), massage=message)
                except Exception as e:
                    print(e)
                    pass



def checkExtractData(makeTime, databaseInfo):
    gameName = databaseInfo["gameName"]
    dbName = databaseInfo["dbName"]
    datenoline = str(makeTime.strftime("%Y%m%d"))
    checkFloderSizeInitCode = "hdfs dfs -du -s -h /user/hive/warehouse/[:GameName]_extract.db/[:DBName]/*/dt=[:DateNoLine]/* |grep \'0  0\'"
    checkFloderSize = checkFloderSizeInitCode.replace("[:GameName]", gameName).replace("[:DBName]", dbName).replace("[:DateNoLine]", datenoline)
    # zeroFloderData = sshCtrl_hdfs.ssh_exec_cmd_return('hdfs dfs -du -s -h /user/hive/warehouse/lineage_extract.db/*/*/dt=20200715/* |grep "0  0"')
    zeroFloderData = sshCtrl_hdfs.ssh_exec_cmd_return(checkFloderSize)

    # 取得空資料夾的路徑
    zeroFloder = [floder.split('  ')[-1] for floder in zeroFloderData.split("\r\n")]
    if zeroFloder[0] == '':
        print("All Import Extract Data Success.")
        pass
    else:
        for floder in zeroFloder:
            if floder != '':
                message = u'\U0000203C' + f"[{gameName}] Info: {str(floder)} is empty, please check."
                print(message)
                # 送出Telegrame告警
                try:
                    telegramCtrl.sendMessageByUrl(url=os.getenv("BDA_TELEGRAM_URL"), userid=os.getenv("BDA_TELEGRAM_USERID"), massage=message)
                except Exception as e:
                    print(e)
                    pass


if __name__ == "__main__":
    print('start 02_CheckImportDataSize')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
