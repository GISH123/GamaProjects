import os , datetime
from ExampleProduct.P04DataCheckInfoExample.info.RawDataInfo_P04DataCheckInfoExample import RawDataInfo_P04DataCheckInfoExample
from ExampleProduct.P04DataCheckInfoExample.info.PreProcessInfo_P04DataCheckInfoExample import PreProcessInfo_P04DataCheckInfoExample
from ExampleProduct.P04DataCheckInfoExample.info.UseModelInfo_P04DataCheckInfoExample import UseModelInfo_P04DataCheckInfoExample
from ExampleProduct.P04DataCheckInfoExample.info.ModelResultInfo_P04DataCheckInfoExample import ModelResultInfo_P04DataCheckInfoExample
from ExampleProduct.P04DataCheckInfoExample.info.ModelScoreInfo_P04DataCheckInfoExample import ModelScoreInfo_P04DataCheckInfoExample
from package.modeldocument.documentCtrl import DocumentCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.hiveCtrl import HiveCtrl
from dotenv import load_dotenv
import pandas

#   可以使用該範例做快速手動資料檢查與SQL產生
#   請記得要設定 class P04DataCheckInfoExampleInfoMain 的繼承內容，以確保可以撈取到所有東西
#   設定完後只要設定 撈取的表名稱 產品 專案 步驟 版本 時間 就可以撈取相關的資料與產生相關的SQL
#   tableName = "gtwpd.model_usedata"
#
#   product = "ExampleProduct"
#   project = "P04DataCheckInfoExample"
#   step = "RawData"
#   version = "R1_0_3"
#   dataTime = "20220101"

class P04DataCheckInfoExampleInfoMain(RawDataInfo_P04DataCheckInfoExample
                       , PreProcessInfo_P04DataCheckInfoExample
                       , UseModelInfo_P04DataCheckInfoExample
                       , ModelResultInfo_P04DataCheckInfoExample
                       , ModelScoreInfo_P04DataCheckInfoExample ):
    pass

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

load_dotenv(dotenv_path="env/hdfs.env")
load_dotenv(dotenv_path="env/hive.env")

documentCtrl = DocumentCtrl()
projectInfoMain = P04DataCheckInfoExampleInfoMain()
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
project = "P04DataCheckInfoExample"
step = "RawData"
version = "R1_0_3"
dataTime = "20220101"

sql = documentCtrl.MakeDataSQL(projectInfoMain , makeInfo , tableName , product, project ,step, version, dataTime)
sql = sql.replace("Limit 10","Limit 10")
print(sql)
print(hiveCtrl.searchSQL(sql))




