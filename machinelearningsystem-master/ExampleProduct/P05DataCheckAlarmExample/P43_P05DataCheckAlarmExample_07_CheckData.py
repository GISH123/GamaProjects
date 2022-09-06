import os , datetime
from ExampleProduct.P05DataCheckAlarmExample.info.RawDataInfo_P05DataCheckAlarmExample import RawDataInfo_P05DataCheckAlarmExample
from ExampleProduct.P05DataCheckAlarmExample.info.PreProcessInfo_P05DataCheckAlarmExample import PreProcessInfo_P05DataCheckAlarmExample
from ExampleProduct.P05DataCheckAlarmExample.info.UseModelInfo_P05DataCheckAlarmExample import UseModelInfo_P05DataCheckAlarmExample
from ExampleProduct.P05DataCheckAlarmExample.info.ModelResultInfo_P05DataCheckAlarmExample import ModelResultInfo_P05DataCheckAlarmExample
from ExampleProduct.P05DataCheckAlarmExample.info.ModelScoreInfo_P05DataCheckAlarmExample import ModelScoreInfo_P05DataCheckAlarmExample
from package.modeldocument.documentCtrl import DocumentCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.hiveCtrl import HiveCtrl
from dotenv import load_dotenv
import pandas

#
#   請參考 P04DataCheckInfoExample
#

class P05DataCheckAlarmExampleInfoMain(RawDataInfo_P05DataCheckAlarmExample
                       , PreProcessInfo_P05DataCheckAlarmExample
                       , UseModelInfo_P05DataCheckAlarmExample
                       , ModelResultInfo_P05DataCheckAlarmExample
                       , ModelScoreInfo_P05DataCheckAlarmExample ):
    pass

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

load_dotenv(dotenv_path="env/hdfs.env")
load_dotenv(dotenv_path="env/hive.env")

documentCtrl = DocumentCtrl()
projectInfoMain = P05DataCheckAlarmExampleInfoMain()
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


tableName = "gtwpd.model_usedata"

product = "ExampleProduct"
project = "P05DataCheckAlarmExample"
step = "RawData"
version = "R1_0_3"
dataTime = "20220101"

sql = documentCtrl.MakeDataSQL(projectInfoMain , makeInfo , tableName , product, project ,step, version, dataTime)
sql = sql.replace("Limit 10","Limit 10")
print(sql)
print(hiveCtrl.searchSQL(sql))




