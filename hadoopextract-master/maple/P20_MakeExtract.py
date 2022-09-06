import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import pandas as pd
import datetime as dt
import sys
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.hiveCtrl import HiveCtrl
from package.common.inputCtrl import inputCtrl
from package.common.extracth.extracthCtrl import ExtracthCtrl
from package.common.database.sqlTool import SqlTool
from dotenv import load_dotenv

#####################################################################################################################
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
#####################################################################################################################

def main(parametersData = {}):
    nowTime = dt.datetime.now()
    print('start at : ' + nowTime.strftime("%T"))
    print('running: P20_MakeExtract.py')
    nowZeroTime = dt.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = (nowZeroTime - dt.timedelta(days=2)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - dt.timedelta(days=2)).strftime("%Y-%m-%d")
    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)
    world = ["45", "06", "04", "03", "02", "01" , "00"]
    fileNameArr = []
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
        if key == 'tableNameArray':
            fileNameArr = parametersData[key]

    startDateTime = dt.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = dt.datetime.strptime(endDateStr, "%Y-%m-%d")
    makeTime = startDateTime
    while makeTime <= endDateTime:
        try:
            makeExTractGameMain(makeTime,world,fileNameArr)
            makeExTractGAMain(makeTime,fileNameArr)
        except:
            pass
        makeTime = makeTime + dt.timedelta(days=1)
    t2 = dt.datetime.now()
    print(f'cumulative time consumed :{t2 - nowTime}')
############################################################################################################ 讀取時間
def makeExTractGameMain(makeTime,gwList,fileNameArr):
    path = "file/maple/middle_SQL/"
    d = makeTime
    d_str = d.strftime('%Y%m%d')
    print(d_str)
    dbName = 'gamedb'

    # 需要先建立對照表格

    # 預先處理欲執行的方式
    Processdict = {
                   'S':'0_Game_character.sql',
                   'B':'00_Game_preserveALL.sql',
                   'C':'03_Game_Date.sql',
                   'C*':'04_Game_Month.sql',
                   'D':'01_Game_onlineCharacter.sql',
                   'D*':'02_Game_onlineAccount.sql'
                   }


    # 載入處理資料表
    infoTable = pd.read_csv(f'file/maple/info_tables/MS_midlle_{dbName}.csv',  encoding='CP950')
    infoTable = infoTable[infoTable['code'].notna()].fillna('')
    colNames = pd.read_csv(f'file/maple/info_tables/Maple_{dbName}_column.csv')

    # 每個表分別處理
    runList = []
    for i in range(len(infoTable)):

        code = infoTable['code'].iloc[i]
        tableName = infoTable['tableName'].iloc[i].lower()
        if fileNameArr != [] and f"{dbName}_{tableName}" not in fileNameArr:
            continue
        print(f"{dbName}_{tableName}")
        if code not in Processdict.keys():
            continue

        # 時間相關欄位讀取
        timetype, timeCol  = infoTable['timecoding'].iloc[i] , infoTable['column'].iloc[i]
        sqlfile = Processdict[code]
        cols = colNames[colNames['TABLE_NAME'] == infoTable['tableName'].iloc[i]]['COLUMN_NAME']


        # 把欄位名串起來
        colStr =''
        for col in cols :
            colStr += f"AA.`{col.lower()}` ,"
        colStr = colStr[:-1]

        print(sqlfile)

        for gw in gwList:
            processPath = f"/user/hive/warehouse/maple_extract.db/{dbName}_{tableName}/dt={d_str}/world={gw}"

            #hdfsCtrl.deleteDirs(processPath)
            sqlReplaceArr = [['[:tableName]', tableName],
                             ['[:FilePath]', processPath],
                             ['[:gw]', gw],
                             ['[:Date0]', d_str],
                             ['[:Date1]', (d + dt.timedelta(days=1)).strftime("%Y%m%d")],
                             ['[:Date-0]', d.strftime(timetype)],
                             ['[:Date-1]', (d+dt.timedelta(days=1)).strftime(timetype)],
                             ['[:timeCol]',f"`{timeCol}`"],
                             ['[:cols]',colStr]
                             ]

            insertHiveSQLArr = SqlTool().loadfile(path + sqlfile).makeSQLStrs(sqlReplaceArr=sqlReplaceArr).makeSQLArr().getSQLArr()


            alterDropStr = f"ALTER TABLE maple_extract.gamedb_{tableName} DROP IF EXISTS PARTITION (dt='{d_str}' , world='{gw}') "
            alterAddStr = f"ALTER TABLE maple_extract.gamedb_{tableName} ADD PARTITION (dt='{d_str}' , world='{gw}')  location '{processPath}' "

            sqlStr = f"{insertHiveSQLArr[0]};{alterDropStr};{alterAddStr};"
            runList.append(sqlStr)

        if i == 0:
            # 如果是需要特殊處理的腳色資料表，先執行
            extracthCtrl.runsql(hiveCtrl, runList, printError= True)
            runList = []
    extracthCtrl.runsql(hiveCtrl, runList, printError= True)

def makeExTractGAMain(makeTime,fileNameArr):
    path = "file/maple/middle_SQL/"
    dbName = 'globalaccount'
    lastDate = '20191201'
    Processdict = {'B': '10_GlobalAccount_preserveALL.sql',
                   'C': '13_GlobalAccount_Date.sql',
                   'D*': '12_GlobalAccount_onlineAccount.sql'
                   }

    infoTable = pd.read_csv(f'file/maple/info_tables/MS_midlle_{dbName}.csv',  encoding='CP950',na_values=[''],keep_default_na=False)
    infoTable = infoTable[infoTable['code'].notna()].fillna('')

    colNames = pd.read_csv(f'file/maple/info_tables/Maple_{dbName}_column.csv',  encoding='CP950')
    d = makeTime
    d_str = d.strftime('%Y%m%d')

    runList = []
    for i in range(len(infoTable)):

        tableName = infoTable['tableName'].iloc[i].lower()
        code = infoTable['code'].iloc[i]
        if fileNameArr != [] and f"{dbName}_{tableName}" not in fileNameArr:
            continue
        if code not in Processdict.keys():
            continue
        print(f"{dbName}_{tableName}")
        timetype, timeCol = infoTable['timecoding'].iloc[i], infoTable['column'].iloc[i]
        sqlfile = Processdict[code]
        cols = colNames[colNames['TABLE_NAME'] == infoTable['tableName'].iloc[i]]['COLUMN_NAME']

        colStr =''
        for col in cols :
            colStr += f"AA.`{col.lower()}` ,"
        colStr = colStr[:-1]
        #print(colStr)
        print(sqlfile)

        processPath = f"/user/hive/warehouse/maple_extract.db/{dbName}_{tableName}/dt={d_str}"
        sqlReplaceArr = [['[:tableName]', tableName],
                         ['[:FilePath]', processPath],
                         ['[:Date0]', d_str],
                         ['[:Date1]', max((d + dt.timedelta(days=1)).strftime("%Y%m%d"), lastDate)],
                         ['[:Date-0]', d.strftime(timetype)],
                         ['[:Date-1]', (d + dt.timedelta(days=1)).strftime(timetype)],
                         ['[:timeCol]', f"`{timeCol}`"],
                         ['[:cols]', colStr]
                         ]
        insertHiveSQLArr = SqlTool().loadfile(path + sqlfile).makeSQLStrs(sqlReplaceArr=sqlReplaceArr).makeSQLArr().getSQLArr()

        alterDropStr = f"ALTER TABLE maple_extract.{dbName}_{tableName} DROP IF EXISTS PARTITION (dt='{d_str}') "
        alterAddStr = f"ALTER TABLE maple_extract.{dbName}_{tableName} ADD PARTITION (dt='{d_str}')  location '{processPath}' "

        sqlStr = f"{insertHiveSQLArr[0]};{alterDropStr};{alterAddStr};"
        runList.append(sqlStr)
    extracthCtrl.runsql(hiveCtrl, runList,printError=True)

if __name__ == "__main__":
    main()