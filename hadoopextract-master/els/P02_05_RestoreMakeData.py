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

sshCtrl_hdfs = SSHCtrl(env="env/ETL01_SSH_HDFS.env")

pandas.set_option("display.max_columns", None)
pandas.set_option("display.max_rows", None)
pandas.set_option("display.width", 1000)

#----------------------------------------------------------------------------------------------------

def main(parametersData={}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    gameName = ""
    dbName = ""

    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "makedate":
            makeDateStrArr = parametersData[key]
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]

    if makeDateStrArr == [] :
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameName = "els" if gameName == "" else gameName
    dbName = "DB" if dbName == "" else dbName

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
        print("Run [MakeRestoreData] to {}".format(makeTime.strftime("%Y-%m-%d")))
        MakeRestoreData(makeTime)

#----------------------------------------------------------------------------------------------------

def MakeRestoreData(makeTime):
    sqlFilePath = "els/file_restore"
    sqlFilePathNames = os.listdir(sqlFilePath)
    for fileName in sqlFilePathNames:
        if "D{}_".format(makeTime.strftime("%Y%m%d")) not in fileName:
            continue
        if "_HDFS_" not in fileName:
            continue

        shfile = open("{}/{}".format(sqlFilePath,fileName), "r", encoding="utf8")
        shStrs = "".join(shfile.readlines())
        shStrArr = shStrs.split("\n")
        for shStr in shStrArr :
            sshCtrl_hdfs.ssh_exec_cmd(shStr)


#----------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    #MakeGameTableMain()
    main()

