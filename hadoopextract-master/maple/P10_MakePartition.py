import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from dotenv import load_dotenv
import maple.PO_CreateMakeCommon as CreateMakeCommon
import maple.PO_lib_sqls as lib_sqls
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.hiveCtrl import HiveCtrl
from package.common.inputCtrl import inputCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas as pd
import datetime as dt
import sys

#################################################################################
trycount = 3
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
tableFilterArray = [
    ["TABLE_NAME", r"^[A-Za-z_]+$", True]
    , ["TABLE_NAME", r"temp", False]
    , ["TABLE_NAME", r"Temp", False]
    , ["TABLE_TYPE", r"BASE TABLE",True]
]
#################################################################################

def main(parametersData = {}):
    nowTime = dt.datetime.now()
    print('start at : ' + nowTime.strftime("%T"))
    print('running: P10_MakePartition.py')
    nowZeroTime = dt.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - dt.timedelta(days=1)).strftime("%Y-%m-%d")
    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)
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
        makePartitionGameMain(makeTime,world)
        makeTime = makeTime + dt.timedelta(days=1)
    t2 = dt.datetime.now()
    print(f'cumulative time consumed :{t2 - nowTime}')


def makePartitionGameMain(makeTime,gwList):

    ########### 時間設定
    d = makeTime
    d_str = d.strftime('%Y%m%d')
    dateStrArrMap = [[d_str, d.strftime('%Y_%m_%d')]]

    ######################## Global Account 處理
    dropPartitionCodeInit = "ALTER TABLE [:TableName] DROP IF EXISTS PARTITION ( [:Partitioned] ) "
    alterPartitionCodeInit = "ALTER TABLE [:TableName] ADD IF NOT EXISTS PARTITION ( [:Partitioned] ) LOCATION '[:FilePath]'"

    inputXlsName ="Maple_GlobalAccount"
    gameWorldArrMap = [["", "GlobalAccount"]]
    schmeaNameInit = "maple_all.globalaccount_"
    filePathInit = "/user/hive/warehouse/maple.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
    partitionedInit = "dt='[:DateNoLine]'"

    sqlList = CreateMakeCommon.PartitionMake(inputXlsName, gameWorldArrMap, dateStrArrMap, tableFilterArray, alterPartitionCodeInit, dropPartitionCodeInit, schmeaNameInit, filePathInit, partitionedInit)
    extracthCtrl.runsql(hiveCtrl,sqlList,printError=True)
    # GameDB 處理
    inputXlsName = "Maple_GameDB"
    gameWorldArrMap = [[gw,f"GameWorld{gw}"] for gw in gwList]
    schmeaNameInit = "maple_all.gamedb_"
    filePathInit = "/user/hive/warehouse/maple.db/ALL/[:DBName]/dt=[:DateNoLine]/[:TableName]"
    partitionedInit = "dt='[:DateNoLine]' , world='[:WorldNumber]'"
    sqlList = CreateMakeCommon.PartitionMake(inputXlsName, gameWorldArrMap, dateStrArrMap, tableFilterArray, alterPartitionCodeInit, dropPartitionCodeInit, schmeaNameInit, filePathInit, partitionedInit)
    extracthCtrl.runsql(hiveCtrl,sqlList,printError=True)
    ####################### AuctionDB 處理

    df = pd.read_csv('file/maple/info_tables/Maple_Auction_column.csv')
    auction_tables = df['TABLE_NAME'].unique()

    for tableName in auction_tables:
        t1 = dt.datetime.now()
        for gw in gwList:
            if gw == '45':
                continue
            processPath = f"/user/hive/warehouse/maple.db/ALL/Auction{gw}/dt={d_str}/{tableName}"
            print(tableName)
            alterDropStr = f"ALTER TABLE gtwpd.maple_auctiondb_{tableName} DROP IF EXISTS PARTITION (world='{gw}') "
            alterAddStr = f"ALTER TABLE gtwpd.maple_auctiondb_{tableName} ADD PARTITION (world='{gw}')  location '{processPath}' "

            CreateMakeCommon.createPartition(alterDropStr, alterAddStr)
            t2 = dt.datetime.now()
            print(f'table:{tableName}, gw: {gw} is finished, cumulative time consumed :{t2 - t1}')

    ############### ClaimDB 處理

    df = pd.read_csv('file/maple/info_tables/Maple_ClaimDB_column.csv')
    tables = df['TABLE_NAME'].unique()

    print(tables)

    for tableName in tables:
        t1 = dt.datetime.now()
        processPath = f"/user/hive/warehouse/maple.db/ALL/LogCenter/dt={d_str}/{tableName}/"
        print(tableName)
        alterDropStr = f"ALTER TABLE gtwpd.maple_claimdb_{tableName.lower()} DROP IF EXISTS PARTITION (dt='{d_str}') "
        alterAddStr = f"ALTER TABLE gtwpd.maple_claimdb_{tableName.lower()} ADD PARTITION (dt='{d_str}')  location '{processPath}' "
        CreateMakeCommon.createPartition(alterDropStr, alterAddStr)

        t2 = dt.datetime.now()
        print(f'table:{tableName} is finished, cumulative time consumed :{t2 - t1}')

if __name__ == "__main__":
    main()