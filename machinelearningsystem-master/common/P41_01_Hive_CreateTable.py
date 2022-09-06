import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.sqlTool import SqlTool
from package.common.database.hiveCtrl import HiveCtrl
from dotenv import load_dotenv

load_dotenv(dotenv_path="env/hive.env")

hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'

)


sqlStrArr = SqlTool().loadfile("common/common/file/init/P41_01_Hive_CreateTable.sql").makeSQLStrs().makeSQLArr().getSQLArr()

for sqlStr in sqlStrArr :
    print(sqlStr)
    hiveCtrl.executeSQL(sqlStr)