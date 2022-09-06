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

#----------------------------------------------------------------------------------------------------

def main(parametersData={}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    gameName = "els"
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
    gameName = "els" if gameName == "" else gameName
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
        print("Run [MakeRestoreSH] to {}".format(makeTime.strftime("%Y-%m-%d")))
        MakeRestoreFile(makeTime, gameName, dbName)

#----------------------------------------------------------------------------------------------------

def MakeRestoreFile(makeZeroTime, gameName, dbName):

    sqlReplaceArr = [
        ['[:SelectDate]', makeZeroTime.strftime("%Y-%m-%d")]
        , ['[:GameName]', gameName]
        , ['[:DBName]', dbName]
    ]
    sqlStrs = getSQLStrs()
    for sqlReplace in sqlReplaceArr:
        sqlStrs = sqlStrs.replace(sqlReplace[0], sqlReplace[1])
    sqlStrArr = sqlStrs.split(";")[:-1]

    dataCheckDF = postgresCtrl.searchSQL(sqlStrArr[0])

    for dataCheckIndex, dataCheckRow in dataCheckDF.iterrows():
        shCode = ""
        fileName = "D{}_Restore_HDFS_{}.txt".format(dataCheckRow["datatime"].strftime("%Y%m%d"),dataCheckRow["dbname"])
        startInitSH = "hadoop dfs -mkdir -p {} \n"
        mainInitSH = "hadoop dfs -rm -r -f {} \nhadoop dfs -cp -f {} {} \n"
        sqlReplaceArr = [
            ['[:SelectDate]', makeZeroTime.strftime("%Y-%m-%d")]
            , ['[:GameName]', dataCheckRow["gamename"]]
            , ['[:DBName]', dataCheckRow["dbname"]]
        ]
        startSH = startInitSH.format(dataCheckRow['initpath'])
        shCode = shCode + startSH
        detailSqlStrs = getDetailSQLStrs()
        for sqlReplace in sqlReplaceArr:
            detailSqlStrs = detailSqlStrs.replace(sqlReplace[0], sqlReplace[1])
        detailSqlStrArr = detailSqlStrs.split(";")[:-1]
        detailDataCheckDF = postgresCtrl.searchSQL(detailSqlStrArr[0])
        for detailDataCheckIndex, detailDataCheckRow in detailDataCheckDF.iterrows():
            mainSH = mainInitSH.format(detailDataCheckRow['restorepath'],detailDataCheckRow['oripath'],detailDataCheckRow['restorepath'])
            shCode = shCode + mainSH

        shfileMake = open("els/file_restore/{}".format(fileName), "w", encoding="utf-8")
        shfileMake.writelines(shCode)
        shfileMake.close()

def getSQLStrs() :
    return """
select 
 	AA.datatime 
 	, AA.gamename 
	, AA.dbname
	, '/user/hive/warehouse/els.db/Restore/' || AA.dbname || '/dt=' || to_char(AA.datatime,'yyyymmdd') as initpath
from datacheck.datacheckdetail AA
inner join restoreinfo.restoreinfo BB on 1 = 1 
	and BB.gamename = AA.gamename 
	and BB.dbname = AA.dbname
	and BB.tablename = AA.tablename
where 1 = 1 
	and AA.datatime = '[:SelectDate]'
	AND ('Game' = '[:GameName]' OR AA.gamename = '[:GameName]')
    AND ('DB' = '[:DBName]' OR AA.dbname = '[:DBName]')
group by
	AA.datatime 
 	, AA.gamename 
	, AA.dbname ; 
    """
def getDetailSQLStrs():
    return """
select 
 	AA.datatime 
	, BB.datatype
	, case 
		when BB.datatype = 'log_file'
			then '/user/hive/warehouse/els.db/' || BB.orihivedb || '/' || BB.oridbname || '/dt=' 
			|| 	to_char(AA.datatime + INTERVAL '16 Day','yyyymm') 
		 	|| 	case when to_char(AA.datatime + INTERVAL '16 Day','dd') <= '16' then '01' else '16' end
			||  '/' || BB.oritablename || '_' || to_char(AA.datatime - INTERVAL '1 Day','yyyymmdd') 
		when BB.datatype = 'log_all'
			then '/user/hive/warehouse/els.db/' || BB.orihivedb|| '/' || BB.oridbname || '/dt='
			|| 	to_char(AA.datatime + INTERVAL '16 Day','yyyymm') 
		 	|| 	case when to_char(AA.datatime + INTERVAL '16 Day','dd') <= '16' then '01' else '16' end
			||  '/' || BB.oritablename
		when BB.datatype = 'copy_new'
			then '/user/hive/warehouse/els.db/' || BB.orihivedb || '/' || BB.oridbname || '/dt=' || to_char(AA.datatime + INTERVAL '1 Day','yyyymmdd') || '/' || BB.oritablename
	  end as oripath
	, '/user/hive/warehouse/els.db/'|| BB.hivedb || '/' || BB.dbname || '/dt=' ||to_char(AA.datatime,'yyyymmdd') || '/' ||BB.tablename as restorepath	 
	, '/user/hive/warehouse/els.db/'|| 'ALL' || '/' || BB.dbname || '/dt=' ||to_char(AA.datatime,'yyyymmdd') || '/' || BB.tablename as allpath	 
from datacheck.datacheckdetail AA
inner join restoreinfo.restoreinfo BB on 1 = 1 
	and BB.gamename = AA.gamename 
	and BB.dbname = AA.dbname
	and BB.tablename = AA.tablename
where 1 = 1 
	and AA.datatime = '[:SelectDate]' ; 
    """

#----------------------------------------------------------------------------------------------------

if __name__ == "__main__":
    #MakeGameTableMain()
    main()

