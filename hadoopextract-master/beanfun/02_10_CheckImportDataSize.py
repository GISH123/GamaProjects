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

folderFilterArray = ['temp_', '_temp', 'TEMP_', '_TEMP', '_tmp', 'tmp_', '_200', '_201', '_202', '_old', '_backup', 'backup_']

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
    dataBaseArr = ["*"] if dataBaseArr == [] else dataBaseArr

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
        , "folderFilterArray": folderFilterArray
    }

    for makeTime in makeTimeArr:
        dateLine = makeTime.strftime("%Y-%m-%d")
        print(f"Start Check {gameName} Source {dateLine} Import Data")
        startRunTime = time.time()
        checkImportData(makeTime, databaseInfo)
        print(f"Check {dateLine} Import Data Total Used {time.time() - startRunTime} seconds.")


def checkImportData(makeTime, databaseInfo):
    gameName = databaseInfo["gameName"]
    dataBaseArr = databaseInfo["dataBaseArr"]
    folderFilterArray = databaseInfo["folderFilterArray"]
    datenoline = str(makeTime.strftime("%Y%m%d"))
    datenolineRow = str(time.strftime('%Y%m%d'))
    checkFloderSizeInitCode = "hadoop fs -du -s -h /user/hive/warehouse/[:gameName].db/ALL/[:dbName]/dt=[:dateNoLine]/* |grep \'0  0\'"
    # 共用DB
    for dbName in dataBaseArr:
        checkFloderSize = checkFloderSizeInitCode.replace("[:gameName]", gameName).replace("[:dbName]", dbName)
        checkFloderSize = checkFloderSize.replace("[:dateNoLine]", datenoline)

        zeroFloderData = sshCtrl_hdfs.ssh_exec_cmd_return(checkFloderSize)
        # 取得空資料夾的路徑
        zeroFloder = [folder.split('  ')[-1] for folder in zeroFloderData.split("\r\n")]
        if zeroFloder[0] != '':
            for folder in zeroFloder:
                if folder != '' and containsAny(folder, folderFilterArray) == False:
                    message = u'\U0000203C' + f"[{gameName}] Info: {str(folder)} is empty, please check."
                    print(message)
                    # 送出Telegrame告警
                    # telegramSend(message)


def checkExtractData(makeTime, databaseInfo):
    gameName = databaseInfo["gameName"]
    dbName = databaseInfo["dbName"]
    datenoline = str(makeTime.strftime("%Y%m%d"))
    checkFloderSizeInitCode = "hadoop fs -du -s -h /user/hive/warehouse/[:GameName]_extract.db/[:DBName]/*/dt=[:DateNoLine]/* |grep \'0  0\'"
    checkFloderSize = checkFloderSizeInitCode.replace("[:GameName]", gameName).replace("[:DBName]", dbName).replace("[:DateNoLine]", datenoline)
    zeroFloderData = sshCtrl_hdfs.ssh_exec_cmd_return(checkFloderSize)

    # 取得空資料夾的路徑
    zeroFloder = [floder.split('  ')[-1] for floder in zeroFloderData.split("\r\n")]
    if zeroFloder[0] != '':
        for floder in zeroFloder:
            if floder != '':
                message = u'\U0000203C' + f"[{gameName}] Info: {str(floder)} is empty, please check."
                print(message)
                # 送出Telegrame告警
                telegramSend(message)


def containsAny(seq, aset):
    return True if any(i in seq for i in aset) else False


def telegramSend(message):
    try:
        telegramCtrl.sendMessageByUrl(url=os.getenv("BDA_TELEGRAM_URL"), userid=os.getenv("BDA_TELEGRAM_USERID"), massage=message)
        pass
    except Exception as e:
        print(e)
        pass


if __name__ == "__main__":
    print('start 02_10_CheckImportDataSize')
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
