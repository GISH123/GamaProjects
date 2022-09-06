import os , datetime
from info.wf.tableinfo.TableInfoMain import TableInfoMain as TableInfoMain
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
tableInfoMain = TableInfoMain()
tableInfoMap = tableInfoMain.getInitInfoMap()
hdfsCtrl = HDFSCtrl(os.getenv("HDFS_HOST"), os.getenv("HDFS_USER"), os.getenv("HDFS_PASSWD"), os.getenv("HDFS_PATH"))
hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

makeInfo = {
    "tableauReportName" : ""
    , "gameName" :"wf"
    , "gameCHName" : ""
    , "schemaName" : ""
    , "serverName": ""
    , "serverPort": ""
    , "dbName": ""
    , "userName": ""
}
gameName = "wf"
tableName = "gtwpd.modelextract_modelextract"
dataTime = "20220613"
tableNumber = "6001"
"""
1802
"""

sql = documentCtrl.MakeDataSQL(tableInfoMain , makeInfo , tableName, gameName , tableNumber, dataTime)
print(sql)
print(hiveCtrl.searchSQL(sql))