import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl_2_0_0 import ExtracthCtrl
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
hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

sshCtrl_hdfs = SSHCtrl("10.10.99.135",22,"hdfs",pkey="env/ALL_PKEY_HDFS")
extracthCtrl = ExtracthCtrl()

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

tableFilterArray = [
    ["TABLE_NAME", r"^[A-Za-z0-9_]+$", True]
]

taskSQLStrArr = []

def main(parametersData = {}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    gameName = ""
    dbName = ""
    typeArray = []
    tableNameArray = []

    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "makedate":
            makeDateStrArr = parametersData[key]
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]
        if key == "gamename":
            gameName = parametersData[key][0]
        if key == "dbname":
            dbName = parametersData[key][0]
        if key == "typeArray":
            typeArray = parametersData[key]
        if key == "tableNameArray":
            tableNameArray  =parametersData[key]

    if makeDateStrArr == []:
        startDateStr = (nowZeroTime - datetime.timedelta(days=4)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=1)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameName = "mabi" if gameName == "" else gameName
    dbName = "DB" if dbName == "" else dbName
    typeArray = [] if typeArray == [] else typeArray
    tableNameArray = [] if dbName == [] else tableNameArray

    makeTimeArr = []
    if makeDateStrArr != []:
        for makeDateStr in makeDateStrArr:
            makeTimeArr.append(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d"))
    else:
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeTimeArr.append(makeDatetime)
            makeDatetime = makeDatetime + datetime.timedelta(days=1)

    for makeTime in makeTimeArr:
        print("Run [MakeALL] to {}".format(makeTime.strftime("%Y-%m-%d")))
        MakeWorldDataMain(makeTime, gameName, dbName , typeArray , tableNameArray)


def MakeWorldDataMain(makeTime, gameName, dbName, typeArray , tableNameArray):
        databaseInfo = {
            "gameName": "mabi"
            , "dbName": "authdb"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/mabi_authdb_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/mabi.db/ALL/authdb"
            , "extractTableType" : "File"
            , "typeArray": typeArray  # all grammar log
            , "tableNameArray": tableNameArray  # characters
        }
        extracthCtrl.MakeWorldDataDetail(makeTime,hiveCtrl, databaseInfo,printError=True)

        databaseInfo = {
            "gameName": "mabi"
            , "dbName": "itemshop"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/mabi_itemshop_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/mabi.db/ALL/itemshop"
            , "extractTableType" : "File"
            , "typeArray": typeArray  # all grammar log
            , "tableNameArray": tableNameArray  # characters
        }
        extracthCtrl.MakeWorldDataDetail(makeTime,hiveCtrl, databaseInfo,printError=True)

        databaseInfo = {
            "gameName": "mabi"
            , "dbName": "mabitaiwanshop"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/mabi_mabitaiwanshop_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/mabi.db/ALL/MABI_TAIWAN_SHOP"
            , "extractTableType" : "File"
            , "typeArray": typeArray  # all grammar log
            , "tableNameArray": tableNameArray  # characters
        }
        extracthCtrl.MakeWorldDataDetail(makeTime,hiveCtrl, databaseInfo,printError=True)

        databaseInfo = {
            "gameName": "mabi"
            , "dbName": "mabiguild2"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/mabi_mabiguild2_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/mabi.db/ALL/mabi_guild2"
            , "extractTableType" : "File"
            , "typeArray": typeArray  # all grammar log
            , "tableNameArray": tableNameArray  # characters
        }
        extracthCtrl.MakeWorldDataDetail(makeTime,hiveCtrl, databaseInfo,printError=True)

        databaseInfo = {
            "gameName": "mabi"
            , "dbName": "messengerDB"
            , "worldNameArr": [""]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]'"
            , "partitionedPathInit": "dt=[:DateNoLine]"
            , "partitionedSQLInit": "dt='[:DateNoLine]'"
            , "csvfile": "file/mabi_messengerDB_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/mabi.db/ALL/messengerDB"
            , "extractTableType" : "File"
            , "typeArray": typeArray  # all grammar log
            , "tableNameArray": tableNameArray  # characters
        }
        extracthCtrl.MakeWorldDataDetail(makeTime,hiveCtrl, databaseInfo,printError=True)

        databaseInfo = {
            "gameName": "mabi"
            , "dbName": "mabi_log"
            , "worldNameArr": [""]
            , "partitionedInit": "date string"
            , "partitionedAlterInit": "date='[:DateNoLine]'"
            , "partitionedPathInit": "date=[:DateNoLine]"
            , "partitionedSQLInit": "date = DATE_FORMAT(DATE_ADD('[:DateLine]',1),'yyyyMMdd')"
            , "csvfile": "file/mabi_mabi_log_extracttable.csv"
            , "filePathInit": "/user/hdfs/mabi"
            , "extractTableType" : "File"
            , "typeArray": typeArray  # all grammar log
            , "tableNameArray": tableNameArray  # characters
        }
        extracthCtrl.MakeWorldDataDetail(makeTime,hiveCtrl, databaseInfo,printError=True)

        databaseInfo = {
            "gameName": "mabi"
            , "dbName": "mabitwauction"
            , "worldNameArr": ["01", "02", "03", "04", "05"]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]' , world='[:World]'"
            , "partitionedPathInit": "dt=[:DateNoLine]/world=[:World]"
            , "partitionedSQLInit": "dt='[:DateNoLine]' AND world= '[:World]'"
            , "csvfile": "file/mabi_mabitwauction_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/mabi.db/ALL/mabitw1_auction"
            , "extractTableType" : "File"
            , "typeArray": typeArray  # all grammar log
            , "tableNameArray": tableNameArray  # characters
        }
        extracthCtrl.MakeWorldDataDetail(makeTime,hiveCtrl, databaseInfo,printError=True)

        databaseInfo = {
            "gameName": "mabi"
            , "dbName": "mabitwmabinogi"
            , "worldNameArr": ["01", "02", "03", "04", "05"]
            , "partitionedInit": "dt string"
            , "partitionedAlterInit": "dt='[:DateNoLine]' , world='[:World]'"
            , "partitionedPathInit": "dt=[:DateNoLine]/world=[:World]"
            , "partitionedSQLInit": "dt='[:DateNoLine]' AND world= '[:World]'"
            , "csvfile": "file/mabi_mabitwmabinogi_extracttable.csv"
            , "filePathInit": "/user/hive/warehouse/mabi.db/ALL/mabitw1_mabinogi"
            , "extractTableType" : "File"
            , "typeArray": typeArray  # all grammar log
            , "tableNameArray": tableNameArray  # characters
        }
        extracthCtrl.MakeWorldDataDetail(makeTime,hiveCtrl, databaseInfo,printError=True)

if __name__ == "__main__":
    main()

