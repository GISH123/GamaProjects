import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.sshCtrl import SSHCtrl
import pandas, datetime, time

sshCtrl_hdfs = SSHCtrl("10.10.99.133", 22, "hdfs", pkey="env/ALL_PKEY_HDFS")

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

tableFilterArray = [
    ["TABLE_NAME", r"^[A-Za-z0-9_]+$", True]
]

taskSQLStrArr = []

def Main():
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime
    while makeTime <= endDateTime:
        print(makeTime)
        if makeTime.day in [3, 8]:
            monthFirstDay = datetime.datetime(makeTime.year, makeTime.month, 1, 0, 0, 0, 0)
            monthFirstDayNoLine = monthFirstDay.strftime("%Y%m%d")

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bf_extract.db/all/beanfunDB/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bf_extract.db/all/beanfunDB/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bf.db/ALL/beanfunDB/dt={}/* /user/hive/warehouse/bf_extract.db/all/beanfunDB/dt={}".format(monthFirstDayNoLine,monthFirstDayNoLine))

        makeTime = makeTime + datetime.timedelta(days=1)


if __name__ == "__main__":
    print('start 11_11_MakeALLMonthFirst')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    Main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))





