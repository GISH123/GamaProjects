import os, sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.telegramCtrl import TelegramCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl
from package.common.database.postgreCtrl import PostgresCtrl
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.sqlTool import SqlTool
from package.common.inputCtrl import inputCtrl
from package.common.sshCtrl import SSHCtrl
from dotenv import load_dotenv
import pandas
import datetime

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

load_dotenv(dotenv_path="env/Telegram.env")
load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/PostgreSQL.env")

sqlTool = SqlTool()
extracthCtrl = ExtracthCtrl()

sshCtrl_hdfs = SSHCtrl(
    host="10.10.99.131"
    , port=22
    , usr="hdfs"
    , pkey="env/ALL_PKEY_HDFS"
)

hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

greenplumCtrl = PostgresCtrl(
    host="10.10.99.151"
    , port=int(os.getenv("POSTGRES_PORT"))
    , user="gpadmin"
    , password="!QAZ2wsx"
    , database="hadoopcheck"
    , schema="public"
)

telegramCtrl = TelegramCtrl()

def main(parametersData={}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    gameNameArr = []

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
            gameNameArr = parametersData[key]


    if makeDateStrArr == []:
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=30)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameNameArr = ["bf","maple","tdn","els","mabi","lineage","cso","bnb","kr"] if gameNameArr == [] else gameNameArr

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
        print("Run [HadoopExtractCheck] to {}".format(makeTime.strftime("%Y-%m-%d")))
        makeHadoopDataRestore(makeTime, gameNameArr)

#------------------------------------------------------------------------------------------

def makeHadoopDataRestore(makeTime, gameNameArr):
    for gameName in gameNameArr :
        if gameName in ["tdn"] :
            selectSqlDBNameStr = """
                SELECT 
                    datatime
                    , gamename
                    , dbname
                FROM hadoopextract.datacheckdetail AAA
                WHERE 1 = 1 
                    AND AAA.datatime >= '[:StartDate]' 
                    AND AAA.datatime < '[:EndDate]'
                    AND AAA.gamename = '[:GameName]'
                GROUP BY 
                    datatime
                    , gamename
                    , dbname
            """
            selectSqlDBNameStr = selectSqlDBNameStr.replace("[:StartDate]", makeTime.strftime("%Y-%m-%d"))
            selectSqlDBNameStr = selectSqlDBNameStr.replace("[:EndDate]", (makeTime + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
            selectSqlDBNameStr = selectSqlDBNameStr.replace("[:GameName]", gameName)
            checkDetailDF = greenplumCtrl.searchSQL(selectSqlDBNameStr)
            for index, row in checkDetailDF.iterrows():
                selectSqlDetailStr = """
                    SELECT 
                        datatime
                        , gamename
                        , dbname
                        , tablename
                    FROM hadoopextract.datacheckdetail AAA
                    WHERE 1 = 1 
                        AND AAA.datatime >= '[:StartDate]' 
                        AND AAA.datatime < '[:EndDate]'
                        AND AAA.gamename = '[:GameName]'
                        AND AAA.dbname = '[:DBName]'
                    GROUP BY 
                        datatime
                        , gamename
                        , dbname
                        , tablename
                """
                selectSqlDetailStr = selectSqlDetailStr.replace("[:StartDate]", makeTime.strftime("%Y-%m-%d"))
                selectSqlDetailStr = selectSqlDetailStr.replace("[:EndDate]", (makeTime + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
                selectSqlDetailStr = selectSqlDetailStr.replace("[:GameName]", gameName)
                selectSqlDetailStr = selectSqlDetailStr.replace("[:DBName]", row['dbname'])
                checkDetailAllDF = greenplumCtrl.searchSQL(selectSqlDetailStr)
                tableNameArr = []
                for index, row in checkDetailAllDF.iterrows():
                    tableNameArr.append(row["tablename"])
                evalRunStrArr = makeAllEvalStrArr(makeTime.strftime("%Y-%m-%d"), row['gamename'], row['dbname'], tableNameArr)
                for evalRunStr in evalRunStrArr :
                    print(evalRunStr)
                    eval(evalRunStr)

#------------------------------------------------------------------------------------------

def makeAllEvalStrArr(makeDataStr, gameNameStr, dbNameStr,tableNameArr):
    importStr = """exec("import [:GameName].P11_10_MakeALL as makeAllCtrl")"""
    runStr = """makeAllCtrl.main({"makedate": [[:MakeDataStr]],"gamename":[[:GameNameStr]],"dbname":[[:DBName]],"tableNameArray":[[:TableNameArray]]})"""

    makeDataStrs = "\"{}\"".format(makeDataStr)
    gameNameStrs = "\"{}\"".format(gameNameStr)
    dbNameStrs = "\"{}\"".format(dbNameStr)
    tableNameStrs = ""
    for tableName in tableNameArr:
        tableNameStrs = "\"{}\"".format(tableName) if tableNameStrs == "" else tableNameStrs + ",\"{}\"".format(tableName)

    return [runStr.replace("[:MakeDataStr]", makeDataStrs)
                  .replace("[:GameName]", gameNameStr)
                  .replace("[:GameNameStr]", gameNameStrs)
                  .replace("[:DBName]", dbNameStrs)
                  .replace("[:TableNameArray]", tableNameStrs)]

if __name__ == "__main__":
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - datetime.timedelta(days=15)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=10)).strftime("%Y-%m-%d")
    main({"startdate": [startDateStr], "enddate": [endDateStr]})

