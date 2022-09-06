import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.inputCtrl import inputCtrl
from package.common.bureport.gpBUReportCtrl import GPBUReportCtrl
from sql.maple.bureport.ReportInfo import ReportInfo
from dotenv import load_dotenv
import time
import datetime
import pandas as pd
import calendar
import os ,sys
from package.common.database.postgreCtrl import PostgresCtrl
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.sqlTool import SqlTool
import package.common.common.GamaniaDateTime as gamaniaDateTime
from package.common.inputCtrl import inputCtrl
from dotenv import load_dotenv
# from openpyxl.utils.dataframe import dataframe_to_rows
import time
import datetime
# import openpyxl
import pandas as pd
import calendar

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)

load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/PostgreSQL.env")
load_dotenv(dotenv_path="env/GreenPlum.env")
gpBUReportCtrl = GPBUReportCtrl()
reportInfo= ReportInfo()
sqlTool = SqlTool()

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)
pd.set_option('display.float_format', lambda x: '%.0f' % x)

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
    gameName = "maple" if gameName == "" else gameName

    makeInfo = {
        "gameName" : gameName
        , "schemaName" : gameName
        , "tableNumberArr": tableNumberArr
        , "startDateNoLine": datetime.datetime.strptime(startDateStr, "%Y-%m-%d").strftime("%Y%m%d")
        , "endDateNoLine": datetime.datetime.strptime(endDateStr, "%Y-%m-%d").strftime("%Y%m%d")
        , "hiveTableName" : "business_bureport"
        , "gpTableStartName" : "bu"
        , "ishaveDataNotMove": False

    }
    print("Run [MoveBUDetail] {} to {}".format(startDateStr,endDateStr))
    MoveBUReportToGP(makeInfo)

def MoveBUReportToGP(makeInfo):
    gameName = makeInfo["gameName"] if "gameName" in makeInfo.keys() else "bf"
    schemaName = makeInfo["schemaName"] if "schemaName" in makeInfo.keys() else "bf"
    startDateNoLine = makeInfo["startDateNoLine"]
    endDateNoLine = makeInfo["endDateNoLine"]
    tableNumberArr = makeInfo["tableNumberArr"] if "tableNumberArr" in makeInfo.keys() else []
    hiveTableName = makeInfo["hiveTableName"] if "hiveTableName" in makeInfo.keys() else "business_bureport"
    gpTableStartName = makeInfo["gpTableStartName"] if "gpTableStartName" in makeInfo.keys() else "d"
    ishaveDataNotMove = makeInfo["ishaveDataNotMove"] if "ishaveDataNotMove" in makeInfo.keys() else False

    hiveCtrl = HiveCtrl(
        host=os.getenv("HIVE_HOST")
        , port=int(os.getenv("HIVE_PORT"))
        , user=os.getenv("HIVE_USER")
        , password=os.getenv("HIVE_PASS")
        , database='default'
        , auth_mechanism='PLAIN'
    )

    greenplumCtrl = PostgresCtrl(
        host=os.getenv("GREENPLUS_HOST")
        , port=int(os.getenv("GREENPLUS_PORT"))
        , user=os.getenv("GREENPLUS_USER")
        , password=os.getenv("GREENPLUS_PASSWORD")
        , database="bureport"
        , schema=schemaName
    )

    for tableNumber in tableNumberArr:
        try :
            # 製作相關參數
            tableName = "{}{}".format(gpTableStartName, tableNumber)
            startDateLine = datetime.datetime.strptime(startDateNoLine, "%Y%m%d").strftime("%Y-%m-%d")
            endDateLine = datetime.datetime.strptime(endDateNoLine, "%Y%m%d").strftime("%Y-%m-%d")
            print("{} {} {} {} ".format(gameName, tableName, startDateLine, endDateLine))

            # 製作相關sqlReplace
            sqlReplaceArr = [
                ['[:SchemaName]', schemaName]
                , ['[:GameName]', gameName]
                , ['[:TableNumber]', tableNumber]
                , ['[:TableName]', tableName]
                , ['[:HiveTableName]',hiveTableName]
                , ['[:StartDateNoLine]', startDateNoLine]
                , ['[:EndDateNoLine]', endDateNoLine]
            ]

            # 檢查是否存在Table
            # 存在檢查是否有資料
            if greenplumCtrl.checkTable(tableName) and ishaveDataNotMove == True :
                sql = "SELECT count(*) FROM {}.{} WHERE game = '{}' and dt >= '{}' and dt <= '{}'".format(schemaName, tableName,gameName, startDateLine, endDateLine)
                df_count = greenplumCtrl.searchSQL(sql)
                if df_count.to_numpy()[0][0] != 0:
                    continue
            print("MOVE")

            # 檢查是否存在Table
            # 存在刪除資料
            # 不存在建立Table
            if greenplumCtrl.checkTable(tableName):
                startDateLine = datetime.datetime.strptime(startDateNoLine, "%Y%m%d").strftime("%Y-%m-%d")
                endDateLine = datetime.datetime.strptime(endDateNoLine, "%Y%m%d").strftime("%Y-%m-%d")
                greenplumCtrl.executeSQL("DELETE FROM {}.{} WHERE game = '{}' and dt >= '{}' and dt <= '{}'".format(schemaName, tableName,gameName, startDateLine, endDateLine))
            else:
                sqlCreateTableStr, sqlCreateIndexStr = getCreateTableReportSQL()
                for sqlReplace in sqlReplaceArr:
                    sqlCreateTableStr = sqlCreateTableStr.replace(sqlReplace[0], sqlReplace[1])
                    sqlCreateIndexStr = sqlCreateIndexStr.replace(sqlReplace[0], sqlReplace[1])
                greenplumCtrl.executeSQL(sqlCreateTableStr)
                greenplumCtrl.executeSQL(sqlCreateIndexStr)

            # 撈取資料
            sqlStr = getBusinessUnitReportSQL()
            for sqlReplace in sqlReplaceArr:
                sqlStr = sqlStr.replace(sqlReplace[0], sqlReplace[1])
            df = hiveCtrl.searchSQL(sqlStr)

            # 塞入資料
            tableInfoDF = greenplumCtrl.getTableInfo(tableName)
            for infoIndex, infoRow in tableInfoDF.iterrows():
                columnName = infoRow['columnname']
                insertType = infoRow['inserttype']
                if insertType == "Integer":
                    df[columnName] = df[columnName].astype('Int64')
            greenplumCtrl.writeDFToTable(df, tableName, "append", float_format="%.0f")
            #greenplumCtrl.writeDFToTable(df, tableName, "append")
        except Exception as e:
            print("error {} to {} {}".format(startDateLine, endDateLine, tableNumber))
            print(e)

def MakeBUReportStatistics(makeInfo):
    gameName = makeInfo["gameName"]
    reportName = makeInfo["reportName"]
    dataReportName = makeInfo["dataReportName"] if "dataReportName" in makeInfo.keys() else []
    startDateStr = makeInfo["startDateStr"] if "startDateStr" in makeInfo.keys() else []
    endDateStr = makeInfo["endDateStr"] if "endDateStr" in makeInfo.keys() else []
    layerArr = makeInfo["layerArr"] if "layerArr" in makeInfo.keys() else []
    layerInfoArrMap = makeInfo["layerInfoArrMap"] if "layerInfoArrMap" in makeInfo.keys() else []
    reportCodeArr = makeInfo["reportCodeArr"] if "reportCodeArr" in makeInfo.keys() else []
    periodType = makeInfo["periodType"] if "periodType" in makeInfo.keys() else []
    firstWeekDay = makeInfo["firstWeekDay"] if "firstWeekDay" in makeInfo.keys() else []

    preEndDate = datetime.datetime.strptime(startDateStr, "%Y-%m-%d") - datetime.timedelta(days=1)
    preStartDate , preEndDate = gamaniaDateTime.getPeriodThisFirstAndLastDate(preEndDate, periodType, firstWeekDay)
    preStartDateStr = preStartDate.strftime("%Y-%m-%d")
    preEndDateStr = preEndDate.strftime("%Y-%m-%d")

    eval(f"exec('from sql.{gameName}.{reportName}.ReportMain import ReportMain')")
    reportMain = eval("ReportMain()")
    layerTaskSQLStrArrMap = {}
    layerTaskSQLDeleteStrArrMap = {}
    for layer in layerArr:
        taskSQLStrArr = []
        taskSQLDeleteStrArr = []
        for reportCode in layerInfoArrMap[layer]:
            if reportCode in reportCodeArr:
                reportType, reportSQLArray = eval(f"reportMain.make{reportCode}DataSQL({makeInfo})")
                makeInfo['sqlReplaceArr'] = [
                    ['[:GameName]', gameName]
                    , ['[:DataReportName]', dataReportName]
                    , ['[:PeriodType]', periodType]
                    , ['[:ReportCode]', reportCode]
                    , ['[:StartDateLine]', startDateStr]
                    , ['[:EndDateLine]', endDateStr]
                    , ['[:PreStartDateLine]', preStartDateStr]
                    , ['[:PreEndDateLine]', preEndDateStr]
                ]

                deleteSqlStr = """
                   DELETE FROM [:GameName].bureportstatistics AA
                   WHERE 1 = 1
                       AND AA.reportname = '[:DataReportName]' 
                       AND AA.gamename = '[:GameName]'
                       AND AA.reportcode = '[:ReportCode]' ;
                """

                for sqlReplace in makeInfo['sqlReplaceArr'] :
                    deleteSqlStr = deleteSqlStr.replace(sqlReplace[0], sqlReplace[1])
                taskSQLDeleteStrArr.append(deleteSqlStr)

                if reportType == "OrderInsert":
                    reportSQLArr = __makeOrderInsertSQL(makeInfo, reportSQLArray)

                for reportSQL in reportSQLArr:
                    taskSQLStrArr.append(reportSQL)
        layerTaskSQLStrArrMap[layer] = taskSQLStrArr
        layerTaskSQLDeleteStrArrMap[layer] = taskSQLDeleteStrArr

    return layerTaskSQLStrArrMap , layerTaskSQLDeleteStrArrMap

# ----------------------------------------------------------------------------------------------------

def __makeOrderInsertSQL( makeInfo, reportSQLArray):
    taskSQLStrArr = []
    sqlReplaceArr = makeInfo["sqlReplaceArr"] if "sqlReplaceArr" in makeInfo.keys() else []
    reportSQLAll = ""
    for reportSQL in reportSQLArray:
        for sqlReplace in sqlReplaceArr:
            reportSQL = reportSQL.replace(sqlReplace[0], sqlReplace[1])
        reportSQLAll = reportSQLAll + reportSQL
    taskSQLStrArr.append(reportSQLAll)
    return taskSQLStrArr

# ----------------------------------------------------------------------------------------------------

def getBusinessUnitReportSQL():
    return """
    SELECT
        to_date(from_unixtime(unix_timestamp(AA.dt,'yyyyMMdd'))) as dt
        , AA.world as world
        , AA.game as game
        , AA.CommonData_1 as CommonData_1
        , AA.CommonData_2 as CommonData_2
        , AA.CommonData_3 as CommonData_3
        , AA.CommonData_4 as CommonData_4
        , AA.CommonData_5 as CommonData_5
        , AA.CommonData_6 as CommonData_6
        , AA.CommonData_7 as CommonData_7
        , AA.CommonData_8 as CommonData_8 
        , AA.CommonData_9 as CommonData_9
        , AA.CommonData_10 as CommonData_10 
        , AA.CommonData_11 as CommonData_11
        , AA.CommonData_12 as CommonData_12
        , AA.CommonData_13 as CommonData_13
        , AA.CommonData_14 as CommonData_14
        , AA.CommonData_15 as CommonData_15
        , AA.UniqueInt_1 as UniqueInt_1
        , AA.UniqueInt_2 as UniqueInt_2
        , AA.UniqueInt_3 as UniqueInt_3
        , AA.UniqueInt_4 as UniqueInt_4
        , AA.UniqueInt_5 as UniqueInt_5
        , AA.UniqueInt_6 as UniqueInt_6
        , AA.UniqueInt_7 as UniqueInt_7
        , AA.UniqueInt_8 as UniqueInt_8
        , AA.UniqueInt_9 as UniqueInt_9
        , AA.UniqueInt_10 as UniqueInt_10
        , AA.UniqueInt_11 as UniqueInt_11
        , AA.UniqueInt_12 as UniqueInt_12
        , AA.UniqueInt_13  as UniqueInt_13
        , AA.UniqueInt_14 as UniqueInt_14
        , AA.UniqueInt_15 as UniqueInt_15
        , AA.UniqueStr_1 as UniqueStr_1
        , AA.UniqueStr_2 as UniqueStr_2
        , AA.UniqueStr_3 as UniqueStr_3
        , AA.UniqueStr_4 as UniqueStr_4
        , AA.UniqueStr_5 as UniqueStr_5
        , AA.UniqueStr_6 as UniqueStr_6
        , AA.UniqueStr_7 as UniqueStr_7
        , AA.UniqueStr_8 as UniqueStr_8
        , AA.UniqueStr_9 as UniqueStr_9
        , AA.UniqueStr_10 as UniqueStr_10
        , AA.UniqueStr_11 as UniqueStr_11
        , AA.UniqueStr_12 as UniqueStr_12
        , AA.UniqueStr_13 as UniqueStr_13
        , AA.UniqueStr_14 as UniqueStr_14
        , AA.UniqueStr_15 as UniqueStr_15
        , AA.UniqueStr_16 as UniqueStr_16
        , AA.UniqueStr_17 as UniqueStr_17
        , AA.UniqueStr_18 as UniqueStr_18
        , AA.UniqueStr_19 as UniqueStr_19
        , AA.UniqueStr_20 as UniqueStr_20
        , AA.uniquedbl_1 as uniquedbl_1 
        , AA.uniquedbl_2 as uniquedbl_2 
        , AA.uniquedbl_3 as uniquedbl_3 
        , AA.uniquedbl_4 as uniquedbl_4 
        , AA.uniquedbl_5 as uniquedbl_5 
        , AA.uniquedbl_6 as uniquedbl_6 
        , AA.uniquedbl_7 as uniquedbl_7 
        , AA.uniquedbl_8 as uniquedbl_8 
        , AA.uniquedbl_9 as uniquedbl_9 
        , AA.uniquedbl_10 as uniquedbl_10 
        , AA.uniquedbl_11 as uniquedbl_11 
        , AA.uniquedbl_12 as uniquedbl_12 
        , AA.uniquedbl_13 as uniquedbl_13 
        , AA.uniquedbl_14 as uniquedbl_14 
        , AA.uniquedbl_15 as uniquedbl_15 
        , AA.uniquedbl_16 as uniquedbl_16 
        , AA.uniquedbl_17 as uniquedbl_17 
        , AA.uniquedbl_18 as uniquedbl_18 
        , AA.uniquedbl_19 as uniquedbl_19 
        , AA.uniquedbl_20 as uniquedbl_20 
        , AA.UniqueTime_1 as UniqueTime_1
        , AA.UniqueTime_2 as UniqueTime_2
        , AA.UniqueTime_3 as UniqueTime_3
        , AA.otherstr_1 as otherstr_1 
        , AA.otherstr_2 as otherstr_2 
        , AA.otherstr_3 as otherstr_3 
        , AA.otherstr_4 as otherstr_4 
        , AA.otherstr_5 as otherstr_5 
        , AA.otherstr_6 as otherstr_6 
        , AA.otherstr_7 as otherstr_7 
        , AA.otherstr_8 as otherstr_8 
        , AA.otherstr_9 as otherstr_9 
        , AA.otherstr_10 as otherstr_10 
    FROM gtwpd.[:HiveTableName] AA
    WHERE 1 = 1
        AND AA.game = '[:GameName]'
        AND AA.dt >= '[:StartDateNoLine]'
        AND AA.dt <= '[:EndDateNoLine]' 
        AND AA.tablenumber = '[:TableNumber]'     
    """

def getCreateTableReportSQL():
    return """
    CREATE TABLE [:SchemaName].[:TableName] (
        dt text NULL
        , world text NULL
        , game text NULL
        , commondata_1 text NULL
        , commondata_2 text NULL
        , commondata_3 text NULL
        , commondata_4 text NULL
        , commondata_5 text NULL
        , commondata_6 text NULL
        , commondata_7 text NULL
        , commondata_8 text NULL
        , commondata_9 text NULL
        , commondata_10 text NULL
        , commondata_11 int8 NULL
        , commondata_12 int8 NULL
        , commondata_13 int8 NULL
        , commondata_14 int8 NULL
        , commondata_15 int8 NULL
        , uniqueint_1 int8 NULL
        , uniqueint_2 int8 NULL
        , uniqueint_3 int8 NULL
        , uniqueint_4 int8 NULL
        , uniqueint_5 int8 NULL
        , uniqueint_6 int8 NULL
        , uniqueint_7 int8 NULL
        , uniqueint_8 int8 NULL
        , uniqueint_9 int8 NULL
        , uniqueint_10 int8 NULL
        , uniqueint_11 int8 NULL
        , uniqueint_12 int8 NULL
        , uniqueint_13 int8 NULL
        , uniqueint_14 int8 NULL
        , uniqueint_15 int8 NULL
        , uniquestr_1 text NULL
        , uniquestr_2 text NULL
        , uniquestr_3 text NULL
        , uniquestr_4 text NULL
        , uniquestr_5 text NULL
        , uniquestr_6 text NULL
        , uniquestr_7 text NULL
        , uniquestr_8 text NULL
        , uniquestr_9 text NULL
        , uniquestr_10 text NULL
        , uniquestr_11 text NULL
        , uniquestr_12 text NULL
        , uniquestr_13 text NULL
        , uniquestr_14 text NULL
        , uniquestr_15 text NULL
        , uniquestr_16 text NULL
        , uniquestr_17 text NULL
        , uniquestr_18 text NULL
        , uniquestr_19 text NULL
        , uniquestr_20 text NULL
        , uniquedbl_1 double precision NULL
        , uniquedbl_2 double precision NULL
        , uniquedbl_3 double precision NULL
        , uniquedbl_4 double precision NULL
        , uniquedbl_5 double precision NULL
        , uniquedbl_6 double precision NULL
        , uniquedbl_7 double precision NULL
        , uniquedbl_8 double precision NULL
        , uniquedbl_9 double precision NULL
        , uniquedbl_10 double precision NULL
        , uniquedbl_11 double precision NULL
        , uniquedbl_12 double precision NULL
        , uniquedbl_13 double precision NULL
        , uniquedbl_14 double precision NULL
        , uniquedbl_15 double precision NULL
        , uniquedbl_16 double precision NULL
        , uniquedbl_17 double precision NULL
        , uniquedbl_18 double precision NULL
        , uniquedbl_19 double precision NULL
        , uniquedbl_20 double precision NULL
        , uniquetime_1 timestamp NULL
        , uniquetime_2 timestamp NULL
        , uniquetime_3 timestamp NULL
        , otherstr_1 text NULL
        , otherstr_2 text NULL
        , otherstr_3 text NULL
        , otherstr_4 text NULL
        , otherstr_5 text NULL
        , otherstr_6 text NULL
        , otherstr_7 text NULL
        , otherstr_8 text NULL
        , otherstr_9 text NULL
        , otherstr_10 text NULL
    )
    ""","""
    CREATE INDEX index_[:SchemaName]_[:TableName]_dt ON [:SchemaName].[:TableName] (dt)
    """

if __name__ == "__main__":
    main()

