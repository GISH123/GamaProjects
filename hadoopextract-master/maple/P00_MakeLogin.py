import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from dotenv import load_dotenv
import datetime as dt
import pandas as pd
import sys
import time
import maple.PO_CreateMakeCommon as CreateMakeCommon
import maple.PO_lib_sqls as lib_sqls
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.postgreCtrl import PostgresCtrl
from package.common.database.sqlTool import SqlTool
from package.common.inputCtrl import inputCtrl
#####################################################################################################################
tryCount = 3

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
    print('running: P00_MakeLogin.py')
    nowZeroTime = dt.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - dt.timedelta(days=2)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - dt.timedelta(days=2)).strftime("%Y-%m-%d")
    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)
    maintainDay = 3 # 維修日
    world = ["45", "06", "04", "03", "02", "01" , "00"]
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
        if key == 'maintainday':
            maintainDay = int(parametersData[key][0])

    startDateTime = dt.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = dt.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime
    while makeTime <= endDateTime:
        makeIPMain(makeTime,world,maintainDay)
        makeIPAccMain(makeTime, maintainDay)
        makeTime = makeTime + dt.timedelta(days=1)
    t2 = dt.datetime.now()
    print(f'cumulative time consumed :{t2 - nowTime}')

def makeIPMain(makeTime,gwList,maintainDay = 3):
    d = makeTime
    d_str = d.strftime('%Y%m%d')
    infoTablePath = 'file/maple/info_tables/Maple_login_character.csv'
    print(d_str)
    Mday = d.date().isoweekday() + 7 - maintainDay
    Mday = Mday if Mday > 7 else Mday-7
    maintainDT = d - dt.timedelta(days=Mday)
    runTables = pd.read_csv(infoTablePath)

    # step 0: 創資料表
    sqlReplaceArr = [['[:fileDate]', d_str]]
    hiveStrArr = SqlTool().loadfile('file/maple/ip_SQL/00_createIPLogDataTemp.sql').makeSQLStrs(
        sqlReplaceArr=sqlReplaceArr).makeSQLArr().getSQLArr()

    for hiveStr in hiveStrArr:
        print(hiveStr)
        hiveCtrl.executeSQL_TCByCount(hiveStr, tryCount)

    for gw in gwList:
        print(gw)

        for i in range(runTables.shape[0]):
            worldCol = runTables['worldName'].iloc[i]
            processPath = f"{runTables['path'].iloc[i]}/dt={d_str}/{worldCol}={gw}/"
            tableName = runTables['tableName'].iloc[i]

            # 對照表
            sqlReplaceArr = [['[:gwid]', str(int(gw))],
                             ['[:gw]',gw],
                             ['[:Date0_0]', d.strftime("%Y%m%d000000")],
                             ['[:Date1_0]', (d + dt.timedelta(days=1)).strftime('%Y%m%d000000')],
                             ['[:Date0]', d_str],  # 當天日期 格式: 20200206
                             ['[:Date0Line]', d.strftime("%Y-%m-%d")],  # 隔天日期 格式: 2020-02-06
                             ['[:Date1Line]', (d + dt.timedelta(days=1)).strftime("%Y-%m-%d")],  # 隔天日期 格式: 2020-02-07
                             ['[:Date-1Line]', (d - dt.timedelta(days=1)).strftime("%Y%m%d")],  # 前一天日期 格式: 2020-02-05
                             ['[:DateM]', maintainDT.strftime("%Y%m%d")],  # 前一維修日 格式: 2020-02-05
                             ['[:FilePath]', processPath]
                             ]

            hiveStr = SqlTool().loadfile(f"file/maple/ip_SQL/{runTables['sqlName'].iloc[i]}").makeSQLStrs(sqlReplaceArr=sqlReplaceArr).makeSQLArr().getSQLArr()
            #print(hiveStr[0])
            if runTables['sqlName'].iloc[i] == "01_iplog_select.sql" and d_str >= '20210517':
                print(hiveStr[1])
                hiveCtrl.executeSQL_TCByCount(hiveStr[1],tryCount)
            else :
                hiveCtrl.executeSQL_TCByCount(hiveStr[0],tryCount)

            alterDropStr = f"ALTER TABLE {tableName} DROP IF EXISTS PARTITION (dt='{d_str}' ,{worldCol} = '{gw}') "
            alterAddStr = f"ALTER TABLE {tableName} ADD PARTITION (dt='{d_str}' ,{worldCol} = '{gw}')  location '{processPath}'"

            CreateMakeCommon.createPartition(alterDropStr, alterAddStr)

        # 刪除暫時表格
    dropStr = f"DROP TABLE gtwpd.maple_iplogdata_temp_{d_str}"
    hiveCtrl.executeSQL(sql=dropStr)

def makeIPAccMain(makeTime,prepareDays = 3):
    infoTablePath = 'file/maple/info_tables/Maple_login_account.csv'
    d = makeTime
    d_str = d.strftime('%Y%m%d')
    print(d_str)
    now = dt.datetime.now()

    # step 0: 創資料表
    try:
        sqlReplaceArr = [['[:Date0]', d_str],['[:fileDate]', d_str]]
        hiveStrArr = SqlTool().loadfile('file/maple/ip_SQL/20_createLogIPLogDataTemp.sql').makeSQLStrs(sqlReplaceArr=sqlReplaceArr).makeSQLArr().getSQLArr()

        for hiveStr in hiveStrArr:
            #print(hiveStr)
            hiveCtrl.executeSQL(hiveStr)
    except:
        sqlReplaceArr = [['[:Date0]', d_str], ['[:fileDate]', '20200801']]
        hiveStrArr = SqlTool().loadfile('file/maple/ip_SQL/20_createLogIPLogDataTemp.sql').makeSQLStrs(
            sqlReplaceArr=sqlReplaceArr).makeSQLArr().getSQLArr()

        for hiveStr in hiveStrArr:
            #print(hiveStr)
            hiveCtrl.executeSQL(hiveStr)

    runTables = pd.read_csv(infoTablePath)
    for i in range(runTables.shape[0]):
        processPath = f"{runTables['path'].iloc[i]}/dt={d_str}/"
        tableName = runTables['tableName'].iloc[i]


        # 對照表
        sqlReplaceArr = [['[:Date0]', d_str],  # 當天日期 格式: 20200206
                         ['[:Date1]', (d + dt.timedelta(days=1)).strftime("%Y%m%d")],  # 隔天日期 格式: 20200207
                         ['[:Date0Line]', d.strftime("%Y-%m-%d")],  # 隔天日期 格式: 2020-02-06
                         ['[:Date1Line]', (d + dt.timedelta(days=1)).strftime("%Y-%m-%d")],  # 隔天日期 格式: 2020-02-07
                         ['[:FilePath]', processPath]
                         ]

        hiveStr = SqlTool().loadfile(f"file/maple/ip_SQL/{runTables['sqlName'].iloc[i]}").makeSQLStrs(sqlReplaceArr=sqlReplaceArr).makeSQLArr().getSQLArr()
        #print(hiveStr[0])
        if runTables['sqlName'].iloc[i] == "21_login_account.sql" and d_str >= '20210517':
            hiveCtrl.executeSQL_TCByCount(hiveStr[1], tryCount)
            print(hiveStr[1])
        else:
            hiveCtrl.executeSQL_TCByCount(hiveStr[0], tryCount)

        alterDropStr = f"ALTER TABLE {tableName} DROP IF EXISTS PARTITION (dt='{d_str}') "
        alterAddStr = f"ALTER TABLE {tableName} ADD PARTITION (dt='{d_str}' )  location '{processPath}'"

        CreateMakeCommon.createPartition(alterDropStr, alterAddStr)
    dropStr = f"DROP TABLE gtwpd.maple_iplog_account_{d_str}"
    hiveCtrl.executeSQL(sql=dropStr)

if __name__ == "__main__":
    main()