import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
from package.common.database.postgreCtrl import PostgresCtrl
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.sqlTool import SqlTool
from package.common.inputCtrl import inputCtrl
from package.common.sshCtrl import SSHCtrl
from dotenv import load_dotenv
import pandas
import datetime

pandas.set_option("display.max_columns", None)
pandas.set_option("display.max_rows", None)
pandas.set_option("display.width", 2000)
pandas.set_option("display.float_format", lambda x: '%.2f' % x)

load_dotenv(dotenv_path="env/GAME_BAK_INFO.env")
load_dotenv(dotenv_path="env/PostgreSQL.env")

postgresCtrl = PostgresCtrl(
    host=os.getenv("POSTGRES_HOST")
    , port=int(os.getenv("POSTGRES_PORT"))
    , user=os.getenv("POSTGRES_USER")
    , password=os.getenv("POSTGRES_PASSWORD")
    , database="Hadoop"
    , schema="public"
)

"""
    06-00 「檢查數據資料」與「產生相關BAK檢查檔」

    1. 確認相關參數 startdate enddate gamename dbname
    2. 執行 06-01 檢查大數據資料是否正確
    3. 執行 06-02 發送相關訊息讓SE撈取BAK檔案
"""

def main(parametersData={}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    gameName = "mabi"
    dbName = "DB"

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

    if makeDateStrArr == [] :
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameName = "mabi" if gameName == "" else gameName
    dbName = "DB" if dbName == "" else dbName

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
        print("Run [MakeDataCheck] to {}".format(makeTime.strftime("%Y-%m-%d")))
        checkHadoopData(makeTime, gameName, dbName)

"""
    06-01 檢查大數據資料是否正確

    1.撈取 file/06_DBCompare/datacheck.sql 檔案並取代相關字元
    2.執行SQL，此SQL大約會執行下列動作
        1.刪除datacheck.datacheck與datacheck.datacheckdetail
        2.根據datacheck.oridatainfo與datacheck.datasize產生datacheck.datacheckdetail細項檔，相關規則如下
            1.某一表格的資料必須高於前七天資料的百分之八十
            2.排除201X，202X，Log等相關每日紀錄的檔案
            3.假如低於八十，又非排除檔案，會比對前七天DB列數資料，必須不差於百分之十
        3.根據datacheck.datacheckdetail產生datacheck.datacheck主檔
"""

def checkHadoopData(makeZeroTime, gameName, dbName):

    sqlReplaceArr = [
        ['[:SelectDate]', makeZeroTime.strftime("%Y-%m-%d")]
        , ['[:GameName]', gameName]
        , ['[:DBName]', dbName]
    ]

    sqlStrs = getSQLStrs()
    for sqlReplace in sqlReplaceArr:
        sqlStrs = sqlStrs.replace(sqlReplace[0], sqlReplace[1])

    sqlStrArr = sqlStrs.split(";")[:-1]

    for sqlStr in sqlStrArr:
        print(sqlStr)
        postgresCtrl.executeSQL(sqlStr)

def getSQLStrs() :
    return """
DELETE FROM datacheck.datacheck
WHERE 1 = 1
    AND datatime >= '[:SelectDate]'::timestamp
    AND datatime < '[:SelectDate]'::timestamp + INTERVAL '1 Day'
    AND ('Game' = '[:GameName]' OR gamename = '[:GameName]')
    AND ('DB' = '[:DBName]' OR dbname = '[:DBName]');

DELETE FROM datacheck.datacheckDetail
WHERE 1 = 1
    AND datatime >= '[:SelectDate]'::timestamp
    AND datatime < '[:SelectDate]'::timestamp + INTERVAL '1 Day'
    AND ('Game' = '[:GameName]' OR gamename = '[:GameName]')
    AND ('DB' = '[:DBName]' OR dbname = '[:DBName]');


INSERT INTO datacheck.datacheckdetail
WITH BASIC_DATA as (
	SELECT
		AA.gamename
		, AA.dbname
		, AA.tablename
		, SUM(CASE WHEN datatime = '[:SelectDate]' THEN datasize ELSE 0 END) as datanowday
		, Round(
			SUM(
				CASE
					WHEN 1 = 1
					AND datatime <= '[:SelectDate]'::timestamp - INTERVAL '1 Day'
					AND datatime >= '[:SelectDate]'::timestamp - INTERVAL '6 Day'
				THEN datasize
				ELSE 0
				END
			)/6
		) as data7bmean
		, 0 as dbnowday
		, 0 as db7bmean
	FROM datacheck.datasize AA
	WHERE 1 = 1
		AND AA.datatime >= '[:SelectDate]'::timestamp - INTERVAL '6 Day'
		AND AA.datatime < '[:SelectDate]'::timestamp + INTERVAL '1 Day'
        AND ('Game' = '[:GameName]' OR AA.gamename = '[:GameName]')
        AND ('DB' = '[:DBName]' OR AA.dbname = '[:DBName]')
	group by
		AA.gamename
		, AA.dbname
		, AA.tablename
)
SELECT
	'[:SelectDate]'::timestamp as datatime
	, AAAA.gamename
	, AAAA.dbname
	, AAAA.tablename
	, 1 as errorcount
	, AAAA.data7bmean - AAAA.datanowday as errorsize
	, 'nowday:'||AAAA.datanowday||',bmean:'||AAAA.data7bmean||',datapar:'||AAAA.datapar as message
	, AAAA.datanowday
FROM (
	SELECT
		AAA.gamename
		, AAA.dbname
		, AAA.tablename
		, SUM(AAA.datanowday) as datanowday
		, SUM(AAA.data7bmean) as data7bmean
		, case when SUM(AAA.data7bmean) = 0 then 0 else SUM(AAA.datanowday) / SUM(AAA.data7bmean) end as datapar
		, SUM(AAA.dbnowday) as dbnowday
		, SUM(AAA.db7bmean) as db7bmean
		,  case when SUM(AAA.db7bmean) = 0 then 0 else SUM(AAA.dbnowday) / SUM(AAA.db7bmean) end  as dbpar
	FROM BASIC_DATA AAA
	WHERE 1 = 1
	GROUP BY
		AAA.gamename
		, AAA.dbname
		, AAA.tablename
) AAAA
LEFT join gamedatainfo.datatablecheckin CCCC on 1 = 1
    AND AAAA.gamename = CCCC.gamename
    AND (AAAA.dbname = CCCC.dbname or CCCC.dbname is null)
    AND AAAA.tablename = CCCC.tablename
LEFT join gamedatainfo.datatablechecknoin DDDD on 1 = 1
    AND AAAA.gamename = DDDD.gamename
    AND (AAAA.dbname = DDDD.dbname or DDDD.dbname is null)
    AND AAAA.tablename = DDDD.tablename
WHERE 1 = 1
    AND AAAA.datapar < 0.20
    and ( 1 != 1
    	or ( 1 = 1
    		AND (AAAA.data7bmean > 10000 or ( AAAA.datanowday = 0 and AAAA.data7bmean > 10000 ))
		    AND AAAA.tablename not like '%201%'
		    AND AAAA.tablename not like '%202%'
		    AND AAAA.tablename not like '%temp%'
		    AND AAAA.tablename not like '%tmp%'
		    AND AAAA.tablename not like '%bak%'
		    AND AAAA.tablename not like '%old%'
    	)
    	or ( 1 = 1
    		AND AAAA.datanowday = 0
    		AND AAAA.data7bmean != 0
    		AND CCCC.tablename is not null
    	)
    )
    and ( 1 = 1
    	and DDDD.tablename is null ) ;

INSERT INTO datacheck.datacheck
SELECT
    AA.datatime
    , AA.gamename
    , AA.dbname
    , SUM(AA.errorcount) as errorcount
    , SUM(AA.errorsize) as errorsize
    , 'tableerror:' || count(*) as message
    , 0 as  status
FROM datacheck.datacheckdetail AA
WHERE 1 = 1
    AND datatime >= '[:SelectDate]'::timestamp
    AND datatime < '[:SelectDate]'::timestamp + INTERVAL '1 Day'
    AND ('Game' = '[:GameName]' OR AA.gamename = '[:GameName]')
    AND ('DB' = '[:DBName]' OR AA.dbname = '[:DBName]')
GROUP BY
    AA.datatime
    , AA.gamename
    , AA.dbname ;
"""

if __name__ == "__main__":
    main()