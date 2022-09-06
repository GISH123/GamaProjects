import os , datetime
from maple.LoginTag.info.RawDataInfo_LoginTag import RawDataInfo_LoginTag
from maple.LoginTag.info.PreProcessInfo_LoginTag import PreProcessInfo_LoginTag
from maple.LoginTag.info.UseModelInfo_LoginTag import UseModelInfo_LoginTag
from maple.LoginTag.info.ModelResultInfo_LoginTag import ModelResultInfo_LoginTag
from maple.LoginTag.info.ModelScoreInfo_LoginTag import ModelScoreInfo_LoginTag
from package.modeldocument.documentCtrl import DocumentCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.hiveCtrl import HiveCtrl
from dotenv import load_dotenv
import pandas

class LoginTagInfoMain(
                    RawDataInfo_LoginTag
                    , PreProcessInfo_LoginTag
                    , UseModelInfo_LoginTag
                    , ModelResultInfo_LoginTag
                    , ModelScoreInfo_LoginTag
                ):
    pass

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

load_dotenv(dotenv_path="env/hdfs.env")
load_dotenv(dotenv_path="env/hive.env")

documentCtrl = DocumentCtrl()
projectInfoMain = LoginTagInfoMain()
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
project = "LoginTag"
step = "RawData"
version = "R0_1_2"
dataTime = "20220101"

sql = documentCtrl.MakeDataSQL(projectInfoMain , makeInfo , tableName , product, project ,step, version, dataTime)
sql = sql.replace("Limit 10","Limit 10")
print(sql)
print(hiveCtrl.searchSQL(sql))




