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
load_dotenv(dotenv_path="env/hive.env")

hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

sshCtrl_hdfs = SSHCtrl("10.10.99.135",22,"hdfs",pkey="env/ALL_PKEY_HDFS")
extracthCtrl = ExtracthCtrl()

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

tableFilterArray = [
    ["TABLE_NAME", r"^[A-Za-z0-9_]+$", True]
]

taskSQLStrArr = []

def main(parametersData = {}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""

    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "makedate":
            makeDateStrArr = parametersData[key]
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]

    if makeDateStrArr == []:
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr

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
        if makeTime.day in [3, 8]:
            monthFirstDay = datetime.datetime(makeTime.year, makeTime.month, 1, 0, 0, 0, 0)
            copyALLMonthFirst(monthFirstDay)

def copyALLMonthFirst (makeTime) :
    shReplaceArr = [
        ["[:DateLine]", makeTime.strftime("%Y-%m-%d")]
        , ["[:DateNoLine]", makeTime.strftime("%Y%m%d")]
    ]

    for shHadoopStr in ["hdfs dfs -du -s /user/hive/warehouse/wod.db/ALL/*/dt=[:DateNoLine]"]:
        for shReplace in shReplaceArr:
            shHadoopStr = shHadoopStr.replace(shReplace[0], shReplace[1])
        print(shHadoopStr)
        execRetrunStr = sshCtrl_hdfs.ssh_exec_cmd_return(shHadoopStr)
        retrunStrArr = execRetrunStr.split("\r\n")

        for retrunStr in retrunStrArr:

            if retrunStr == "DEPRECATED: Use of this script to execute hdfs command is deprecated.":
                continue
            elif retrunStr == "Instead use the hdfs command for it.":
                continue
            elif retrunStr == "":
                continue
            elif retrunStr.find("No such file or directory") >= 0:
                continue
            else:
                def not_empty(s):
                    return s and s.strip()

                pathSizeData = list(filter(not_empty, retrunStr.split(" ")))
                oldPathName = pathSizeData[2]
                newPathName = pathSizeData[2].replace("/wod.db/ALL", "/wod_extract.db/all")

                sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r {}".format(newPathName))
                sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p {}".format(newPathName))
                sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p {} {}".format(oldPathName, newPathName))

if __name__ == "__main__":
    main()





