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
        "dnmembership": [""]
        , "dnstaging": [""]
        , "dnworld": ["01", "02"]
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
            print(extracttableSQL)
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
                databaseInfo["partitionedSQLInit"] = "dt='[:DateNoLine]"
            else :
                databaseInfo["partitionedInit"]= "dt string , world string"
                databaseInfo["partitionedAlterInit"]= "dt='[:DateNoLine]' , world='[:World]'"
                databaseInfo["partitionedPathInit"]= "dt=[:DateNoLine]/world=[:World]"
                databaseInfo["partitionedSQLInit"]= "dt='[:DateNoLine]' AND world= '[:World]'"

        print("Run [MakeALL] to {}".format(makeTime.strftime("%Y-%m-%d")))
        MakeWorldDataDetail(makeTime, hiveCtrl, databaseInfo,printError=True)

def MakeWorldDataDetail (makeTime, hiveCtrl, databaseInfo, printError=False):
        gameName = databaseInfo["gameName"]
        dbName = databaseInfo["dbName"]
        worldNameArr = databaseInfo["worldNameArr"]
        partitionedAlterInit = databaseInfo["partitionedAlterInit"]
        partitionedPathInit = databaseInfo["partitionedPathInit"]
        partitionedSQLParentInit = databaseInfo["partitionedSQLInit"]
        partitionedSQLInit = databaseInfo["partitionedSQLInit"]
        typeArray = databaseInfo["typeArray"] if "typeArray" in databaseInfo.keys() else []
        csvfile = databaseInfo["csvfile"] if "csvfile" in databaseInfo.keys() else ""
        tableNameArray = databaseInfo["tableNameArray"] if "tableNameArray" in databaseInfo.keys() else []



        if csvfile == "":
            tableDataFrame = databaseInfo["extracttable"]
        else:
            tableDataFrame = pandas.read_csv(databaseInfo["csvfile"], encoding="utf_8_sig")

        #利用排序決定執行順序
        tableDataFrame = tableDataFrame.sort_values(by=["layers", "type"])
        print("Start Insert", dbName, makeTime.strftime("%Y-%m-%d"), "Data.")
        exLayer = 0
        while exLayer <= 3:
            taskAlterSQLStrArr = []

            if printError == True:
                print("{} exLayer : {}".format(makeTime.strftime("%Y-%m-%d"), str(exLayer)))
            exLayerTableDataFrame = tableDataFrame[tableDataFrame["layers"].isin([exLayer])]

            if len(exLayerTableDataFrame) == 0:
                exLayer = exLayer + 1
                continue

            for tableIndex, tableRow in exLayerTableDataFrame.iterrows():
                if typeArray != [] and tableRow["type"] not in typeArray:
                    continue

                if tableNameArray != [] and tableRow["tablename"] not in tableNameArray:
                    continue

                oriColumnsAllStr = ""
                oriShowColumnsSQL = "show columns in {}_{}.{}_{}".format(str(tableRow["gamename"]), str(tableRow["orihivedb"]), str(tableRow["oridbname"]), str(tableRow["oritablename"]))
                oriColumnsDataFrame = hiveCtrl.searchSQL_TCByCount(oriShowColumnsSQL,3)
                oriColumnsDataFrame = oriColumnsDataFrame[~oriColumnsDataFrame["field"].isin(["dt", "world"])]
                for columnsIndex, columnsRow in oriColumnsDataFrame.iterrows():
                    oriColumnsAllStr = ("ORI." + columnsRow["field"]) if oriColumnsAllStr == "" else (oriColumnsAllStr + ",ORI." + columnsRow["field"])

                showColumnsSQL = "show columns in {}_{}.{}_{}".format(str(tableRow["gamename"]), str(tableRow["hivedb"]), str(tableRow["dbname"]), str(tableRow["tablename"]))

                try:
                    #createStrInit = "CREATE External TABLE IF NOT EXISTS [:tableFullName] like [:OriTableFullName] Location '[:FilePath]' ;"
                    alterStrInit = "Alter Table [:tableFullName] Set Location '[:FilePath]' ;"
                    OriTableFullName = "{}_{}.{}_{}".format(tableRow["gamename"], tableRow["orihivedb"], tableRow["oridbname"], tableRow["oritablename"])
                    tableFullName = "{}_{}.{}_{}".format(tableRow["gamename"], tableRow["hivedb"], tableRow["dbname"], tableRow["tablename"])
                    alterStr = alterStrInit.replace("[:OriTableFullName]", OriTableFullName).replace("[:tableFullName]", tableFullName)
                    alterStr = alterStr.replace("[:FilePath]", databaseInfo["filePathInit"]).replace("[:tableName]", tableRow["tablename"])
                    print(alterStr)
                    taskAlterSQLStrArr.append(alterStr)
                except:
                    pass

            for taskAlterSQLStr in taskAlterSQLStrArr :
                print(taskAlterSQLStr)

            extracthCtrl.runsql(hiveCtrl, taskAlterSQLStrArr, printError)
            exLayer = exLayer + 1



if __name__ == "__main__":
    #main({"startdate":["2021-01-10"],"enddate":["2021-01-10"],"dbname":["dnmembership"],"tableNameArray":["characters"]})
    main({"startdate":["2021-01-10"],"enddate":["2021-01-10"],"dbname":["dnmembership"]})
    main({"startdate":["2021-01-10"],"enddate":["2021-01-10"],"dbname":["dnstaging"]})

