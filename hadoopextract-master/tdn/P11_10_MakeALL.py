import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl_2_1_0 import ExtracthCtrl
from package.common.database.postgreCtrl import PostgresCtrl
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.sqlTool import SqlTool
from package.common.inputCtrl import inputCtrl
from package.common.sshCtrl import SSHCtrl
from dotenv import load_dotenv
import pandas
import datetime
load_dotenv(dotenv_path="env/PostgreSQL.env")
load_dotenv(dotenv_path="env/hive.env")
hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

postgresCtrl = PostgresCtrl(
    host=os.getenv("POSTGRES_HOST")
    , port=os.getenv("POSTGRES_PORT")
    , user=os.getenv("POSTGRES_USER")
    , password=os.getenv("POSTGRES_PASSWORD")
    , database="Hadoop"
    , schema="extractinfo"
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
    gameWorldArr = []
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
        if key == "gameWorldArr":
            gameWorldArr = parametersData[key]
        if key == "typeArray":
            typeArray = parametersData[key]
        if key == "tableNameArray":
            tableNameArray  =parametersData[key]

    if makeDateStrArr == []:
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameName = "tdn" if gameName == "" else gameName
    dbName = "DB" if dbName == "" else dbName
    typeArray = [] if typeArray == [] else typeArray
    tableNameArray = [] if dbName == [] else tableNameArray
    alldate = {
        "dnworld": ["01", "02"]
        , "dnstaging": [""]
        , "dnmembership": [""]
    }
    makedata = {}
    if dbName == "DB" and gameWorldArr == [] :
        makedata = alldate
    elif dbName != "DB" and gameWorldArr == [] :
        makedata[dbName] = alldate[dbName]
    elif dbName == "DB" and gameWorldArr != []:
        for dataName in alldate.keys():
            if alldate[dataName] != [""] :
                makedata[dataName] = gameWorldArr
    elif dbName != "DB" and gameWorldArr != [] :
        makedata[dbName] = gameWorldArr

    print(makedata)

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
        for dataBaseName in makedata.keys():
            dateLine = makeTime.strftime("%Y-%m-%d")
            extracttableInitCode = """
                SELECT * 
                FROM extractinfo.extractmakeinfo 
                WHERE 1 = 1 
                    AND gamename = '[:gameName]' 
                    AND dbname = '[:dataBase]'
            """
            extracttableSQL = extracttableInitCode.replace("[:gameName]", gameName.lower()).replace("[:dataBase]", dataBaseName.lower())
            extracttable = postgresCtrl.searchSQL(extracttableSQL)
            databaseInfo = {
                "gameName": gameName.lower()
                , "dbName": dataBaseName.lower()
                , "worldNameArr": makedata[dataBaseName]
                , "csvfile": ""
                , "extracttable": extracttable
                , "filePathInit": "/user/hive/warehouse/{}_extract.db/{}/[:tableName]".format(gameName.lower(),dataBaseName.lower())
                , "typeArray": typeArray  # all grammar log
                , "tableNameArray": tableNameArray  # characters
            }
            if makedata[dataBaseName] == [""] :
                databaseInfo["partitionedInit"] = "dt string"
                databaseInfo["partitionedAlterInit"] = "dt='[:DateNoLine]'"
                databaseInfo["partitionedPathInit"] = "dt=[:DateNoLine]"
                databaseInfo["partitionedSQLInit"] = "dt='[:DateNoLine]'"
            else :
                databaseInfo["partitionedInit"]= "dt string , world string"
                databaseInfo["partitionedAlterInit"]= "dt='[:DateNoLine]' , world='[:World]'"
                databaseInfo["partitionedPathInit"]= "dt=[:DateNoLine]/world=[:World]"
                databaseInfo["partitionedSQLInit"]= "dt='[:DateNoLine]' AND world= '[:World]'"

            print("Run [MakeALL] to {}".format(makeTime.strftime("%Y-%m-%d")))
            extracthCtrl.MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo,printError=True)

if __name__ == "__main__":
    main()