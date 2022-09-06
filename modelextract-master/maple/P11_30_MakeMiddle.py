import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from dotenv import load_dotenv
import datetime as dt
import sys
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl
from package.common.database.postgreCtrl import PostgresCtrl
from package.common.database.sqlTool import SqlTool
from package.common.inputCtrl import inputCtrl
from os import listdir
#####################################################################################################################
tryCount = 3
runDates = [2]
load_dotenv(dotenv_path="env/hive.env")
hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)


load_dotenv(dotenv_path="env/hdfs.env")
hdfsCtrl = HDFSCtrl(
      url=os.getenv("HDFS_HOST")
    , user=os.getenv("HDFS_USER")
    , password=os.getenv("HDFS_PASSWD")
    , filePath=os.getenv("HDFS_PATH")
)
extracthCtrl = ExtracthCtrl()
load_dotenv(dotenv_path="env/PostgreSQL.env")
pgsql = PostgresCtrl(
    host=os.getenv("POSTGRES_HOST")
    , port=int(os.getenv("POSTGRES_PORT"))
    , user=os.getenv("POSTGRES_USER")
    , password=os.getenv("POSTGRES_PASSWORD")
    , database='maple'
    , schema='otherdata'
)
#####################################################################################################################
def main(parametersData = {}):

    nowTime = dt.datetime.now()
    print('start at : ' + nowTime.strftime("%T"))
    print('running: P30_MakeMiddle.py')
    nowZeroTime = dt.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - dt.timedelta(days=2)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - dt.timedelta(days=2)).strftime("%Y-%m-%d")
    fileNameArr = []

    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    world = ["45", "06", "04", "03", "02", "01", "00"]
    runOld = False
    for key in parametersData.keys():
        if key == "world":
            world = parametersData[key]
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]
        if key in ('dt', 'makedate'):
            startDateStr = parametersData[key][0]
            endDateStr = parametersData[key][0]
        if key == 'runOld':
            runOld = True
        if key == 'tableNameArray':
            fileNameArr = parametersData[key]
            runOld = True

    startDateTime = dt.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = dt.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime
    while makeTime <= endDateTime:
        makeMiddleMain(makeTime,world,fileNameArr,runOld)
        if makeTime.day in runDates:
            monthFirstDay = makeTime - dt.timedelta(days=30)
            monthFirstDay = dt.datetime(monthFirstDay.year, monthFirstDay.month, 1, 0, 0, 0, 0)
            createMonthlyMiddleData(monthFirstDay, world,fileNameArr)
        t2 = dt.datetime.now()
        print(f'{makeTime.strftime("%Y-%m-%d")} finished,  cumulative time consumed :{t2 - nowTime}')
        makeTime = makeTime + dt.timedelta(days=1)


def makeMiddleMain(makeTime,gwList,fileNameArr,runOld):
    d = makeTime
    d_str = d.strftime('%Y%m%d')
    print(d_str)
    path = 'file/maple/model_middle_SQL/'
    sqlfileList = listdir(path)

    runList = [[],[],[]]

    for sqlfile in sqlfileList:
        tableName = sqlfile[3:-4]
        if fileNameArr != [] and tableName not in fileNameArr:
            continue
        if sqlfile[0] == '0':
            continue
        if not runOld :
            if int(sqlfile[:2]) < 38:
                continue
        print(tableName)

        for gw in gwList:
            processPath = f"/user/GTW_PD/DB/Maple/middle/Process/{tableName}/dt={d_str}/world={gw}"

            #hdfsCtrl.deleteDirs(processPath)

            sqlReplaceArr = [['[:FilePath]', processPath],  # 檔案位置
                             ['[:gw]', gw],  # 這種格式的伺服器名稱 :'00'
                             ['[:gwid]', str(int(gw))],  # 這種格式的伺服器名稱 :0
                             ['[:Date0]', d_str],  # 當天日期 格式: 20200206
                             ['[:Date1]', (d + dt.timedelta(days=1)).strftime("%Y%m%d")],  # 隔天日期 格式: 20200207
                             ['[:Date-0]', d.strftime("%Y-%m-%d")],  # 當天日期 格式: 2020-02-06
                             ['[:Date-1]', (d + dt.timedelta(days=1)).strftime("%Y-%m-%d")],  # 隔天日期 格式: 2020-02-07
                             ['[:DateP]', (d - dt.timedelta(days=1)).strftime("%Y%m%d")]]  # 前一天日期 格式: 20200205

            insertHiveSQLArr = SqlTool().loadfile(path + sqlfile).makeSQLStrs(sqlReplaceArr=sqlReplaceArr).makeSQLArr().getSQLArr()
            #print(insertHiveSQLArr[0])

            alterDropStr = f"ALTER TABLE gtwpd.maple_middle_{tableName} DROP IF EXISTS PARTITION (dt='{d_str}' , world='{gw}') "
            alterAddStr = f"ALTER TABLE gtwpd.maple_middle_{tableName} ADD PARTITION (dt='{d_str}' , world='{gw}')  location '{processPath}' "

            sqlStr = f"{insertHiveSQLArr[0]};{alterDropStr};{alterAddStr};"

            if int(sqlfile[0]) == 2:
                runList[0].append(sqlStr)
            elif int(sqlfile[0])==3:
                runList[1].append(sqlStr)
            else:
                runList[2].append(sqlStr)

    extracthCtrl.runsql(hiveCtrl, runList[0], printError=True)
    extracthCtrl.runsql(hiveCtrl, runList[1], printError=True)
    extracthCtrl.runsql(hiveCtrl, runList[2], printError=True)

def createMonthlyMiddleData(makeTime, world, fileNameArr):
    path = 'file/maple/monthly_SQL/'
    nextTime = makeTime + dt.timedelta(days=31)
    nextTime = dt.datetime(nextTime.year, nextTime.month, 1, 0, 0, 0, 0)
    sqlfileList = listdir(path)
    for sqlfile in sqlfileList:
        tableName = f"monthly_{sqlfile[3:-4]}"
        if fileNameArr != [] and tableName not in fileNameArr:
            continue
        print(tableName)

        runList = []
        for gw in world:
            d_str = makeTime.strftime("%Y%m%d")
            processPath = f"/user/GTW_PD/DB/Maple/middle/Process/{tableName}/dt={d_str}/world={gw}"

            sqlReplaceArr = [['[:FilePath]', processPath],
                             ['[:gw]', gw],
                             ['[:Date0]', d_str],
                             ['[:DateN]', (nextTime - dt.timedelta(days=1) ).strftime("%Y%m%d")],
                             ['[:DateNext]', nextTime.strftime("%Y%m%d")]
                             ]

            insertHiveSQLArr = SqlTool().loadfile(path + sqlfile).makeSQLStrs(sqlReplaceArr=sqlReplaceArr).makeSQLArr().getSQLArr()
            runList.append(f'{insertHiveSQLArr[0]};')

        extracthCtrl.runsql(hiveCtrl, runList, printError=True)
        hiveCtrl.executeSQL(f'MSCK REPAIR TABLE gtwpd.maple_middle_{tableName}')


if __name__ == "__main__":
    main()