import os , datetime
from package.common.document.documentCtrl import DocumentCtrl as DocumentCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.hiveCtrl import HiveCtrl
from dotenv import load_dotenv
import pandas

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)
# 撈取相關
load_dotenv(dotenv_path="env/hdfs.env")
load_dotenv(dotenv_path="env/hive.env")
documentCtrl = DocumentCtrl()

hdfsCtrl = HDFSCtrl(os.getenv("HDFS_HOST"), os.getenv("HDFS_USER"), os.getenv("HDFS_PASSWD"), os.getenv("HDFS_PATH"))
hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

makeInfo = {}
gameName = "lineage"
tableName = "gtwpd.modelextract_modelextract"
dataTime = "20220701"
tableNumber = "6007"

eval(f"exec('from info.{gameName}.tableinfo.TableInfoMain import TableInfoMain as {gameName}TableInfoMain')")
tableInfoMain = eval(f" {gameName}TableInfoMain()")
tableInfoMap = tableInfoMain.getInitInfoMap()
"""
1802
"""
sql = documentCtrl.MakeDataSQL(tableInfoMain , makeInfo , tableName, gameName ,tableNumber, dataTime)
sql = sql.replace("Limit 10","Limit 10")
print(sql)
print(hiveCtrl.searchSQL(sql))

