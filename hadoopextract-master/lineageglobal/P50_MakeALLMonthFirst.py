import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.sshCtrl import SSHCtrl
import pandas, datetime, time
# import re, random, asyncio

hiveCtrl = HiveCtrl(host="10.10.99.124", port=10000, user="GTW_PD", password="haveagoodtime", database="default")
sshCtrl_hdfs = SSHCtrl("10.10.99.133", 22, "hdfs", pkey="env/ALL_PKEY_HDFS")

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

tableFilterArray = [
    ["TABLE_NAME", r"^[A-Za-z0-9_]+$", True]
]

taskSQLStrArr = []

def main():
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    # startDateStr = "2019-12-01"
    # endDateStr = "2020-06-30"
    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime
    while makeTime <= endDateTime:
        print(makeTime)
        if makeTime.day in [3, 8]:
            monthFirstDay = datetime.datetime(makeTime.year, makeTime.month, 1, 0, 0, 0, 0)
            monthFirstDayNoLine = monthFirstDay.strftime("%Y%m%d")

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/lineage_extract.db/all/LinDBGlobal/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/lineage_extract.db/all/LinDBGlobal/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/lineage.db/ALL/LinDBGlobal/dt={}/* /user/hive/warehouse/lineage_extract.db/all/LinDBGlobal/dt={}".format(monthFirstDayNoLine,monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/lineage_extract.db/all/LINWORLD1_Global/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/lineage_extract.db/all/LINWORLD1_Global/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/lineage.db/ALL/LINWORLD1_Global/dt={}/* /user/hive/warehouse/lineage_extract.db/all/LINWORLD1_Global/dt={}".format(monthFirstDayNoLine,monthFirstDayNoLine))

            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/lineage_extract.db/all/LINWORLD2_Global/dt={} ".format(monthFirstDayNoLine))
            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/lineage_extract.db/all/LINWORLD2_Global/dt={} ".format(monthFirstDayNoLine))
            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/lineage.db/ALL/LINWORLD2_Global/dt={}/* /user/hive/warehouse/lineage_extract.db/all/LINWORLD2_Global/dt={}".format(monthFirstDayNoLine,monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/lineage_extract.db/all/LINWORLD4_Global/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/lineage_extract.db/all/LINWORLD4_Global/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/lineage.db/ALL/LINWORLD4_Global/dt={}/* /user/hive/warehouse/lineage_extract.db/all/LINWORLD4_Global/dt={}".format(monthFirstDayNoLine,monthFirstDayNoLine))

            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/lineage_extract.db/all/LINWORLD5_Global/dt={} ".format(monthFirstDayNoLine))
            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/lineage_extract.db/all/LINWORLD5_Global/dt={} ".format(monthFirstDayNoLine))
            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/lineage.db/ALL/LINWORLD5_Global/dt={}/* /user/hive/warehouse/lineage_extract.db/all/LINWORLD5_Global/dt={}".format(monthFirstDayNoLine,monthFirstDayNoLine))

            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/lineage_extract.db/all/LINWORLD6_Global/dt={} ".format(monthFirstDayNoLine))
            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/lineage_extract.db/all/LINWORLD6_Global/dt={} ".format(monthFirstDayNoLine))
            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/lineage.db/ALL/LINWORLD6_Global/dt={}/* /user/hive/warehouse/lineage_extract.db/all/LINWORLD6_Global/dt={}".format(monthFirstDayNoLine,monthFirstDayNoLine))

            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/lineage_extract.db/all/LINWORLD7_Global/dt={} ".format(monthFirstDayNoLine))
            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/lineage_extract.db/all/LINWORLD7_Global/dt={} ".format(monthFirstDayNoLine))
            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/lineage.db/ALL/LINWORLD7_Global/dt={}/* /user/hive/warehouse/lineage_extract.db/all/LINWORLD7_Global/dt={}".format(monthFirstDayNoLine,monthFirstDayNoLine))

            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/lineage_extract.db/all/LINWORLD9_Global/dt={} ".format(monthFirstDayNoLine))
            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/lineage_extract.db/all/LINWORLD9_Global/dt={} ".format(monthFirstDayNoLine))
            # sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/lineage.db/ALL/LINWORLD9_Global/dt={}/* /user/hive/warehouse/lineage_extract.db/all/LINWORLD9_Global/dt={}".format(monthFirstDayNoLine,monthFirstDayNoLine))

        makeTime = makeTime + datetime.timedelta(days=1)

if __name__ == "__main__":
    print('start 11_11_MakeALLMonthFirst')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))





