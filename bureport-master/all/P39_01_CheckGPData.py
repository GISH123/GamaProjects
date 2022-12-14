import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.inputCtrl import inputCtrl
from package.common.bureport.gpBUReportCtrl import GPBUReportCtrl
from sql.wod.bureport.ReportInfo import ReportInfo
from dotenv import load_dotenv
import time
import datetime
import pandas as pd
import calendar
from package.common.database.postgreCtrl import PostgresCtrl

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)

load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/PostgreSQL.env")
load_dotenv(dotenv_path="env/GreenPlum.env")
gpBUReportCtrl = GPBUReportCtrl()
reportInfo= ReportInfo()


def main(parametersData = {}) :
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = ""
    endDateStr = ""
    gameName = ""
    tableNumberArr = []
    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]
        if key == "gamename":
            gameName = parametersData[key][0]
        if key == "tableNumberArr":
            tableNumberArr = parametersData[key]

    startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
    endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    tableNumberArr = reportInfo.tableNumberArr if tableNumberArr == [] else tableNumberArr
    gameName = "wod" if gameName == "" else gameName

    makeInfo = {
        "gameName" : gameName
        , "schemaName" : gameName
        , "tableNumberArr": tableNumberArr
        , "startDateLine": startDateStr
        , "endDateLine": endDateStr
        , "hiveTableName" : "business_bureport"
        , "gpTableStartName" : "bu"

    }
    print("Run [MakeDataSize] {} {} to {}".format(gameName, startDateStr, endDateStr))
    makeDataSize(makeInfo)
    print("Run [MakeDataCheckDetail] {} {} to {}".format(gameName, startDateStr, endDateStr))
    #makeDataCheckDetailSize(makeInfo)


def makeDataSize(makeInfo) :
    gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else "bf"
    schemaName = makeInfo["schemaName"] if "schemaName" in makeInfo.keys() else "bf"
    tableNumberArr = makeInfo["tableNumberArr"] if "tableNumberArr" in makeInfo.keys() else []
    startDateLine = makeInfo["startDateLine"]
    endDateLine = makeInfo["endDateLine"]
    gpTableStartName = makeInfo["gpTableStartName"] if "gpTableStartName" in makeInfo.keys() else "bu"

    for tableNumber in tableNumberArr :
        gpBUReportCtrl = PostgresCtrl(
            host=os.getenv("GREENPLUS_HOST")
            , port=int(os.getenv("GREENPLUS_PORT"))
            , user=os.getenv("GREENPLUS_USER")
            , password=os.getenv("GREENPLUS_PASSWORD")
            , database="bureport"
            , schema=schemaName
        )

        searchSqlStr = """
            SELECT 
                dt  as datatime
                , '[:GameName]' AS gamename 
                , 'gpbureport' AS dbname
                , world as world
                , '[:GPTableStartName][:TableNumber]' AS tablename
                , count(*) AS datasize 
            FROM [:SchemaName].[:GPTableStartName][:TableNumber]
            WHERE 1 = 1 
                AND dt >= '[:StartDateLine]' 
                AND dt <= '[:EndDateLine]'
            GROUP BY 	
                dt , world
            ORDER BY 	
                dt
        """

        sqlReplaceArr = [
            ['[:SchemaName]', schemaName]
            , ['[:GameName]', gameName]
            , ['[:TableNumber]', tableNumber]
            , ['[:TableName]', tableNumber]
            , ['[:GPTableStartName]', gpTableStartName]
            , ['[:StartDateLine]', startDateLine]
            , ['[:EndDateLine]', endDateLine]
        ]

        for sqlReplace in sqlReplaceArr :
            searchSqlStr = searchSqlStr.replace(sqlReplace[0],sqlReplace[1])

        searchDF = gpBUReportCtrl.searchSQL(searchSqlStr)

        gpHadoopCheckCtrl = PostgresCtrl(
            host=os.getenv("GREENPLUS_HOST")
            , port=int(os.getenv("GREENPLUS_PORT"))
            , user=os.getenv("GREENPLUS_USER")
            , password=os.getenv("GREENPLUS_PASSWORD")
            , database="hadoopcheck"
            , schema="public"
        )

        deleteSqlStr = """
           DELETE FROM gpbureport.datasize
           WHERE 1 = 1
               AND datatime >= '[:StartDateLine]' 
               AND datatime < '[:EndDateLine]'
               AND tablename = '[:GPTableStartName][:TableNumber]'
               AND gamename = '[:GameName]'
        """

        sqlReplaceArr = [
            ['[:SchemaName]', schemaName]
            , ['[:GameName]', gameName]
            , ['[:TableNumber]', tableNumber]
            , ['[:TableName]', tableNumber]
            , ['[:GPTableStartName]', gpTableStartName]
            , ['[:StartDateLine]', startDateLine]
            , ['[:EndDateLine]', endDateLine]
        ]

        for sqlReplace in sqlReplaceArr :
            deleteSqlStr = deleteSqlStr.replace(sqlReplace[0],sqlReplace[1])

        gpHadoopCheckCtrl.executeSQL(deleteSqlStr)

        tableName = "gpbureport.datasize"
        tableInfoDF = gpHadoopCheckCtrl.getTableInfo(tableName)
        gpHadoopCheckCtrl.insertData(tableName=tableName, insertTableInfoDF=tableInfoDF, insertDataDF=searchDF)

def makeDataCheckDetailSize(makeInfo):
    gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else "bf"
    startDateLine = makeInfo["startDateLine"]
    endDateLine = makeInfo["endDateLine"]

    startDateTime = datetime.datetime.strptime(startDateLine, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateLine, "%Y-%m-%d")
    makeDatetime = startDateTime
    while makeDatetime <= endDateTime:
        gpHadoopCheckCtrl = PostgresCtrl(
            host=os.getenv("GREENPLUS_HOST")
            , port=int(os.getenv("GREENPLUS_PORT"))
            , user=os.getenv("GREENPLUS_USER")
            , password=os.getenv("GREENPLUS_PASSWORD")
            , database="hadoopcheck"
            , schema="public"
        )

        makeSqlStr = """
            INSERT INTO gpbureport.datacheckdetail 
            SELECT 
                AAAA.datatime 
                , AAAA.gamename 
                , AAAA.dbname
                , AAAA.world
                , AAAA.tablename
                , 'Data Size Error'
                , 1 AS errorcount
                , last6daycount - checkdaycount AS errorsize
                , '???????????? ' || checkdaycount || ' ??????????????????????????? ' || last6daycount || ' ???' AS message
                , NULL AS reruntype 
                , NULL AS rerunmassage
            FROM (
                SELECT 
                    '[:DateLine]' ::timestamp AS datatime
                    , AAA.gamename 
                    , AAA.dbname
                    , AAA.world
                    , AAA.tablename
                    , sum(CASE WHEN datatime = '[:DateLine]' THEN datasize ELSE 0 END) AS checkdaycount
                    , sum(CASE 
                            WHEN datatime >= '[:DateLine]' ::timestamp - INTERVAL '6 Day'
                            AND datatime <= '[:DateLine]'::timestamp - INTERVAL '1 Day'
                            THEN round(datasize/6,0) ELSE 0 END) AS last6daycount
                FROM gpbureport.datasize AAA 
                WHERE 1 = 1 
                    AND datatime >= '[:DateLine]' ::timestamp - INTERVAL '6 Day'
                    AND datatime <= '[:DateLine]' 
                    AND gamename = '[:GameName]'
                GROUP BY
                    AAA.gamename 
                    , AAA.dbname
                    , AAA.world
                    , AAA.tablename
            ) AAAA
            WHERE 1 = 1 
                AND checkdaycount < last6daycount * 0.65
        """
        sqlReplaceArr = [
            ['[:GameName]', gameName]
            , ['[:DateLine]', makeDatetime.strftime("%Y-%m-%d")]
        ]

        for sqlReplace in sqlReplaceArr:
            makeSqlStr = makeSqlStr.replace(sqlReplace[0], sqlReplace[1])
        gpHadoopCheckCtrl.executeSQL(makeSqlStr)
        makeDatetime = makeDatetime + datetime.timedelta(days=1)

if __name__ == "__main__":
    tableNumberArrLoginPlus_Login = [
        "1001", "1002", "1003"
        , "1101", "1102", "1103"
        , "1131", "1132", "1133", "1134", "1135", "1136", "1137"
    ]
    tableNumberArrLoginPlus = [
        "1001", "1002", "1003"
        , "1101", "1102", "1103"
        , "1131", "1132", "1133", "1134", "1135", "1136", "1137"
        , "1804"
        , "6002", "6011", "6012", "6019"
    ]
    tableNumberArrAllData = [
        "1001", "1002", "1003"
        , "1101", "1102", "1103"
        , "1131", "1132", "1133", "1134", "1135", "1136", "1137"
        , "1804"
        , "2001", "2002"
        , "2101", "2102", "2103", "2111", "2112", "2113"
        , "2802"
        , "3001", "3002"
        , "5001", "5101"
        , "6002", "6011", "6012", "6019"
        , "6509"
        , "6609"
    ]
    main({"startdate": ["2019-12-01"],"enddate": ["2020-12-31"],"gamename":["maple"],"tableNumberArr":tableNumberArrAllData})
    main({"startdate": ["2019-12-01"],"enddate": ["2020-12-31"],"gamename":["tdn"],"tableNumberArr":tableNumberArrLoginPlus})
    main({"startdate": ["2019-12-01"],"enddate": ["2020-12-31"],"gamename":["wod"],"tableNumberArr":tableNumberArrLoginPlus_Login})

    main({"startdate": ["2019-12-01"],"enddate": ["2020-12-31"],"gamename":["cso"],"tableNumberArr":tableNumberArrLoginPlus})
    main({"startdate": ["2019-12-01"],"enddate": ["2020-12-31"],"gamename":["els"],"tableNumberArr":tableNumberArrLoginPlus})
    main({"startdate": ["2019-12-01"],"enddate": ["2020-12-31"],"gamename":["mabi"],"tableNumberArr":tableNumberArrLoginPlus})
    main({"startdate": ["2019-12-01"],"enddate": ["2020-12-31"],"gamename":["lineage"],"tableNumberArr":tableNumberArrLoginPlus})
    main({"startdate": ["2019-12-01"],"enddate": ["2020-12-31"],"gamename":["lineagef2p"],"tableNumberArr":tableNumberArrLoginPlus})
    main({"startdate": ["2019-12-01"],"enddate": ["2020-12-31"],"gamename":["lineager"],"tableNumberArr":tableNumberArrLoginPlus})
    main({"startdate": ["2019-12-01"],"enddate": ["2020-12-31"],"gamename":["lineageweb"],"tableNumberArr":tableNumberArrLoginPlus})





