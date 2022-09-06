import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.sshCtrl import SSHCtrl
import pandas, datetime, time
# import re, random, asyncio


sshCtrl_hdfs = SSHCtrl("10.10.99.133", 22, "hdfs", pkey="env/ALL_PKEY_HDFS")

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

tableFilterArray = [
    ["TABLE_NAME", r"^[A-Za-z0-9_]+$", True]
]

taskSQLStrArr = []

dbArr = ['GameWorld00','GameWorld01','GameWorld02','GameWorld03','GameWorld04','GameWorld06','GameWorld45','GlobalAccount',
'Auction00','Auction01','Auction02','Auction03','Auction04','Auction06']


def main():
    print('running: P50_MakeALLMonthFirst.py')
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
        # if makeTime.day in [3]:
            monthFirstDay = datetime.datetime(makeTime.year, makeTime.month, 1, 0, 0, 0, 0)
            monthFirstDayNoLine = monthFirstDay.strftime("%Y%m%d")
            for db in dbArr:
                sshCtrl_hdfs.ssh_exec_cmd(f"hdfs dfs -rm -r /user/hive/warehouse/maple_extract.db/all/{db}/dt={monthFirstDayNoLine} ")
                sshCtrl_hdfs.ssh_exec_cmd(f"hdfs dfs -mkdir -p /user/hive/warehouse/maple_extract.db/all/{db}/dt={monthFirstDayNoLine} ")
                sshCtrl_hdfs.ssh_exec_cmd(f"hdfs dfs -cp -p /user/hive/warehouse/maple.db/ALL/{db}/dt={monthFirstDayNoLine}/* /user/hive/warehouse/maple_extract.db/all/{db}/dt={monthFirstDayNoLine}")

        if makeTime.day in [3, 8]:
            shHadoopStr = "hdfs dfs -du -s /user/hive/warehouse/maple.db/ALL/Log/*/dt=[:DateNoLine]"
            shReplaceArr = [
                ["[:DateNoLine]", makeTime.strftime("%Y%m%d")]
            ]
            for shReplace in shReplaceArr:
                shHadoopStr = shHadoopStr.replace(shReplace[0], shReplace[1])
            try:

                execRetrunStr = sshCtrl_hdfs.ssh_exec_cmd_return(shHadoopStr)
                # print(execRetrunStr)q
                if execRetrunStr is not None:
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
                            pathData = list(filter(not_empty, pathSizeData[2].split("/")))

                            oldPath = pathSizeData[2]
                            newPath = pathSizeData[2].replace("maple.db/ALL", "/maple_extract.db/all")

                            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r {}".format(newPath))
                            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p {}".format(newPath))
                            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p {}/* {}".format(oldPath, newPath))

            except Exception as e:
                print(e)
                pass


        makeTime = makeTime + datetime.timedelta(days=1)

if __name__ == "__main__":
    print('start 11_11_MakeALLMonthFirst')
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))





