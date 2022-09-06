import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.sqlTool import SqlTool
from package.common.database.postgreCtrl import PostgresCtrl
from dotenv import load_dotenv
import time

load_dotenv(dotenv_path="env/greenplus.env")
greenplumCtrl = PostgresCtrl(
    host=os.getenv("GREENPLUS_HOST")
    , port=int(os.getenv("GREENPLUS_PORT"))
    , user=os.getenv("GREENPLUS_USER")
    , password=os.getenv("GREENPLUS_PASSWORD")
    , database="mlops"
    , schema="public"
)

# sqlStrArr = SqlTool().loadfile("common/common/file/init/P41_02_GP_CreateTable_modelmanagement_modelversion.sql").makeSQLStrs().makeSQLArr().getSQLArr()
# for sqlStr in sqlStrArr :
#     time.sleep(0.5)
#     greenplumCtrl.executeSQL(sqlStr)

# sqlStrArr = SqlTool().loadfile("common/common/file/init/P41_02_GP_CreateTable_modeldatacheck_modeldatacheckinfo.sql").makeSQLStrs().makeSQLArr().getSQLArr()
# for sqlStr in sqlStrArr :
#     time.sleep(0.5)
#     greenplumCtrl.executeSQL(sqlStr)

# sqlStrArr = SqlTool().loadfile("common/common/file/init/P41_02_GP_CreateTable_modeldatacheck_modeldataalarmdetail.sql").makeSQLStrs().makeSQLArr().getSQLArr()
# for sqlStr in sqlStrArr :
#     time.sleep(0.5)
#     greenplumCtrl.executeSQL(sqlStr)

# sqlStrArr = SqlTool().loadfile("common/common/file/init/P41_02_GP_CreateTable_modeldatacheck_modeldataalarmoverview.sql").makeSQLStrs().makeSQLArr().getSQLArr()
# for sqlStr in sqlStrArr :
#     time.sleep(0.5)
#     greenplumCtrl.executeSQL(sqlStr)
