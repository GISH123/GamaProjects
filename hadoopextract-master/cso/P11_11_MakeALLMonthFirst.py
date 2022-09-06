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
        startDateStr = (nowZeroTime - datetime.timedelta(days=7)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=5)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr

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
            monthFirstDayNoLine = monthFirstDay.strftime("%Y%m%d")

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/cso_extract.db/all/db_shop/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/cso_extract.db/all/db_shop/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/cso.db/ALL/db_shop/dt={}/* /user/hive/warehouse/cso_extract.db/all/db_shop/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/cso_extract.db/all/cso/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/cso_extract.db/all/cso/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/cso.db/ALL/cso/dt={}/* /user/hive/warehouse/cso_extract.db/all/cso/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/cso_extract.db/all/nx_messenger00/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/cso_extract.db/all/nx_messenger00/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/cso.db/ALL/NX_Messenger00/dt={}/* /user/hive/warehouse/cso_extract.db/all/nx_messenger00/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/cso_extract.db/all/cso_log/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/cso_extract.db/all/cso_log/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/cso.db/ALL/cso_log/dt={}/* /user/hive/warehouse/cso_extract.db/all/cso_log/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))


if __name__ == "__main__":
    main()





