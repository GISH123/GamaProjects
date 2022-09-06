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
load_dotenv(dotenv_path="env/PostgreSQL.env")

sshCtrl_hdfs = SSHCtrl(
    host="10.10.99.131"
    , port=22
    , usr="hdfs"
    , pkey="env/ALL_PKEY_HDFS"
)

sqlTool = SqlTool()
extracthCtrl = ExtracthCtrl()
hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)



pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)


def main():
    for worldName in ["01","02"] :
        hadoopStr = "hadoop dfs -du -s /user/GTW_PD/DB/BUSINESSUNIT/bureport/game=tdn/dt=*/world={}/tablenumber=80013".format(worldName)
        print(hadoopStr)
        execRetrunStr = sshCtrl_hdfs.ssh_exec_cmd_return(hadoopStr)
        retrunStrArr = execRetrunStr.split("\r\n")
        for retrunStr in retrunStrArr:
            print(retrunStr)
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
                hadoopRMStr = "hadoop dfs -rm -r -f {}".format(pathSizeData[2])
                sshCtrl_hdfs.ssh_exec_cmd(hadoopRMStr)

if __name__ == "__main__":
    main()

