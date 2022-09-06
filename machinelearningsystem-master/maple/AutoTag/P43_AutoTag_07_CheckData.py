import os , datetime
from maple.AutoTag.info.RawDataInfo_AutoTag import RawDataInfo_AutoTag
from maple.AutoTag.info.PreProcessInfo_AutoTag import PreProcessInfo_AutoTag
from maple.AutoTag.info.UseModelInfo_AutoTag import UseModelInfo_AutoTag
from maple.AutoTag.info.ModelResultInfo_AutoTag import ModelResultInfo_AutoTag
from maple.AutoTag.info.ModelScoreInfo_AutoTag import ModelScoreInfo_AutoTag
from package.modeldocument.documentCtrl import DocumentCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.hiveCtrl import HiveCtrl
from dotenv import load_dotenv
import pandas

class AutoTagInfoMain(
                    RawDataInfo_AutoTag
                    , PreProcessInfo_AutoTag
                    , UseModelInfo_AutoTag
                    , ModelResultInfo_AutoTag
                    , ModelScoreInfo_AutoTag
                ):
    pass

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

load_dotenv(dotenv_path="env/hdfs.env")
load_dotenv(dotenv_path="env/hive.env")

documentCtrl = DocumentCtrl()
projectInfoMain = AutoTagInfoMain()
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

product = "maple"
project = "AutoTag"
step = "RawData"
version = "R0_1_2"
dataTime = "20220101"

sql = documentCtrl.MakeDataSQL(projectInfoMain , makeInfo , tableName , product, project ,step, version, dataTime)
sql = sql.replace("Limit 10","Limit 10")
print(sql)
print(hiveCtrl.searchSQL(sql))




