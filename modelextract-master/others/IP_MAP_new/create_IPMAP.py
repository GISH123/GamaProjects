import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.sqlTool import SqlTool
from dotenv import load_dotenv
import pandas as pd
import datetime as dt

load_dotenv(dotenv_path="env/hive.env")
hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)
load_dotenv(dotenv_path="env/hdfs.env")
hdfsCtrl = HDFSCtrl(
      url=os.getenv("HDFS_HOST")
    , user=os.getenv("HDFS_USER")
    , password=os.getenv("HDFS_PASSWD")
    , filePath=os.getenv("HDFS_PATH")
)


def main(parametersData = {}):

    # 上傳
    #hdfsUpload('/user/GTW_PD/DB/TEMP/temp_ipmap/asp/','asp.csv')
    #hdfsUpload('/user/GTW_PD/DB/TEMP/temp_ipmap/map_data/', 'map_data.csv')
    d = dt.datetime.now()
    replacArr = [['[:DateNoLine]',d.strftime('%Y%m%d')]]
    hiveStrArr = SqlTool().loadfile(f"runASN.sql").makeSQLStrs(replacArr).makeSQLArr().getSQLArr()
    hiveStrArr += SqlTool().loadfile(f"runMAP.sql").makeSQLStrs(replacArr).makeSQLArr().getSQLArr()


    for sqlStr in hiveStrArr :
         #hiveCtrl.executeSQL_TCByCount(sqlStr,3)
         print(sqlStr)
         hiveCtrl.executeSQL(sqlStr)

if __name__ == "__main__":
    main()
