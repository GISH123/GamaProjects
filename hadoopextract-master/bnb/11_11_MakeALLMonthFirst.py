import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.sshCtrl import SSHCtrl
import pandas
import re
import datetime
import random
import time , asyncio

hiveCtrl = HiveCtrl(host="10.10.99.124", port=10000, user="GTW_PD", password="haveagoodtime", database="default")
sshCtrl_hdfs = SSHCtrl("10.10.99.134", 22, "hdfs", pkey="env/ALL_PKEY_HDFS")

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

loop = asyncio.get_event_loop()
start_time = time.time()
#八小時

tableFilterArray = [
    ["TABLE_NAME", r"^[A-Za-z0-9_]+$", True]
]

taskSQLStrArr = []

def Main():
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    # startDateStr = "2020-08-01"
    # endDateStr = "2020-08-03"
    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime
    while makeTime <= endDateTime:
        print(makeTime)
        #if makeTime.day in [1,8]:
        if makeTime.day in [1]:
            monthFirstDay = datetime.datetime(makeTime.year, makeTime.month, 1, 0, 0, 0, 0)
            monthFirstDayNoLine = monthFirstDay.strftime("%Y%m%d")

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserInfo00/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserInfo00/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserInfo00/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserInfo00/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserInfo01/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserInfo01/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserInfo01/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserInfo01/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserInfo02/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserInfo02/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserInfo02/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserInfo02/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserInfo03/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserInfo03/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserInfo03/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserInfo03/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserInfo04/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserInfo04/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserInfo04/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserInfo04/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserInfo05/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserInfo05/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserInfo05/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserInfo05/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserInventory00/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserInventory00/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserInventory00/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserInventory00/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserInventory01/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserInventory01/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserInventory01/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserInventory01/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserInventory02/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserInventory02/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserInventory02/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserInventory02/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserInventory03/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserInventory03/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserInventory03/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserInventory03/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserInventory04/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserInventory04/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserInventory04/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserInventory04/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserInventory05/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserInventory05/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserInventory05/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserInventory05/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/Userdynamic00/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/Userdynamic00/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/Userdynamic00/dt={}/* /user/hive/warehouse/bnb_extract.db/all/Userdynamic00/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/Userdynamic01/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/Userdynamic01/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/Userdynamic01/dt={}/* /user/hive/warehouse/bnb_extract.db/all/Userdynamic01/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/Userdynamic02/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/Userdynamic02/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/Userdynamic02/dt={}/* /user/hive/warehouse/bnb_extract.db/all/Userdynamic02/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/Userdynamic03/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/Userdynamic03/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/Userdynamic03/dt={}/* /user/hive/warehouse/bnb_extract.db/all/Userdynamic03/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/Userdynamic04/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/Userdynamic04/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/Userdynamic04/dt={}/* /user/hive/warehouse/bnb_extract.db/all/Userdynamic04/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/Userdynamic05/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/Userdynamic05/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/Userdynamic05/dt={}/* /user/hive/warehouse/bnb_extract.db/all/Userdynamic05/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/CAConfig/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/CAConfig/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/CAConfig/dt={}/* /user/hive/warehouse/bnb_extract.db/all/CAConfig/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/CAGuild/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/CAGuild/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/CAGuild/dt={}/* /user/hive/warehouse/bnb_extract.db/all/CAGuild/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/CAItem/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/CAItem/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/CAItem/dt={}/* /user/hive/warehouse/bnb_extract.db/all/CAItem/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/CAMarket/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/CAMarket/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/CAMarket/dt={}/* /user/hive/warehouse/bnb_extract.db/all/CAMarket/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/CARankey/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/CARankey/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/CARankey/dt={}/* /user/hive/warehouse/bnb_extract.db/all/CARankey/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/CASchool/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/CASchool/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/CASchool/dt={}/* /user/hive/warehouse/bnb_extract.db/all/CASchool/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/CATaiwanStat/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/CATaiwanStat/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/CATaiwanStat/dt={}/* /user/hive/warehouse/bnb_extract.db/all/CATaiwanStat/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/CAUtils/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/CAUtils/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/CAUtils/dt={}/* /user/hive/warehouse/bnb_extract.db/all/CAUtils/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/FxMessage/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/FxMessage/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/FxMessage/dt={}/* /user/hive/warehouse/bnb_extract.db/all/FxMessage/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/LadderInfo/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/LadderInfo/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/LadderInfo/dt={}/* /user/hive/warehouse/bnb_extract.db/all/LadderInfo/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/LadderRank/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/LadderRank/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/LadderRank/dt={}/* /user/hive/warehouse/bnb_extract.db/all/LadderRank/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/RPGDynamic/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/RPGDynamic/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/RPGDynamic/dt={}/* /user/hive/warehouse/bnb_extract.db/all/RPGDynamic/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/RPGInventory/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/RPGInventory/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/RPGInventory/dt={}/* /user/hive/warehouse/bnb_extract.db/all/RPGInventory/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/ServerLogDB/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/ServerLogDB/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/ServerLogDB/dt={}/* /user/hive/warehouse/bnb_extract.db/all/ServerLogDB/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/TranxLog/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/TranxLog/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/TranxLog/dt={}/* /user/hive/warehouse/bnb_extract.db/all/TranxLog/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserContent/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserContent/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserContent/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserContent/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))

            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -rm -r /user/hive/warehouse/bnb_extract.db/all/UserEditLog/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -mkdir -p /user/hive/warehouse/bnb_extract.db/all/UserEditLog/dt={} ".format(monthFirstDayNoLine))
            sshCtrl_hdfs.ssh_exec_cmd("hdfs dfs -cp -p /user/hive/warehouse/bnb.db/ALL/UserEditLog/dt={}/* /user/hive/warehouse/bnb_extract.db/all/UserEditLog/dt={}".format(monthFirstDayNoLine, monthFirstDayNoLine))


        makeTime = makeTime + datetime.timedelta(days=1)

if __name__ == "__main__":
    Main()





