import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.bureport.gpBUReportCtrl import GPBUReportCtrl
from package.common.asyncio.asyncioSQLTool import AsyncioSQLTool
from package.common.inputCtrl import inputCtrl
from dotenv import load_dotenv
from sql.wod.bureportstatistics.ReportInfo import ReportInfo
import package.common.common.GamaniaDateTime as gamaniaDateTime
from package.common.database.postgreCtrl import PostgresCtrl
import time
import datetime
import pandas

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)
pandas.set_option("display.float_format", lambda x: '%.4f' % x)

load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/PostgreSQL.env")
load_dotenv(dotenv_path="env/GreenPlum.env")
gpBUReportCtrl = GPBUReportCtrl()
reportInfo= ReportInfo()
asyncioSQLTool = AsyncioSQLTool()



def main(parametersData = {}) :
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = ""
    endDateStr = ""
    gameName = ""
    periodType = ""
    reportCodeArr = []
    firstWeekDay = None
    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]
        if key == "gamename":
            gameName = parametersData[key][0]
        if key == "periodType":
            periodType = parametersData[key][0]
        if key == "reportCodeArr":
            reportCodeArr = parametersData[key]
        if key == "firstweekday" :
            firstWeekDay = int(parametersData[key][0])

    startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
    endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    reportCodeArr = reportInfo.reportCodeArr if reportCodeArr == [] else reportCodeArr
    gameName = "wod" if gameName == "" else gameName
    periodType = "day" if periodType == "" else periodType
    firstWeekDay = 1 if firstWeekDay == None else firstWeekDay

    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")

    for rangeDateInfo in gamaniaDateTime.getRangeDateInfoArr (startDateTime, endDateTime , periodType ,firstWeekDay):
        makeInfo = {
            "gameName" : gameName
            , "reportName" : "bureportstatistics"
            , "dataReportName": rangeDateInfo['dateName']
            , "startDateStr": rangeDateInfo['startDateTime'].strftime("%Y-%m-%d")
            , "endDateStr": rangeDateInfo['endDateTime'].strftime("%Y-%m-%d")
            , "allDateArr" : rangeDateInfo['allDateArr']
            , "reportCodeArr": reportCodeArr
            , "periodType": periodType
            , "firstWeekDay": firstWeekDay
            , "layerArr": reportInfo.layerArr
            , "layerInfoArrMap": reportInfo.layerInfoArrMap
        }
        print("Run [MakeBUReportStatistics] {} {} to {}".format(periodType,rangeDateInfo['startDateTime'].strftime("%Y-%m-%d"),rangeDateInfo['endDateTime'].strftime("%Y-%m-%d")))

        greenplumCtrl = PostgresCtrl(
            host=os.getenv("GREENPLUS_HOST")
            , port=int(os.getenv("GREENPLUS_PORT"))
            , user=os.getenv("GREENPLUS_USER")
            , password=os.getenv("GREENPLUS_PASSWORD")
            , database="bureport"
            , schema=gameName
        )

        layerTaskSQLStrArrMap , layerTaskSQLDeleteStrArrMap = gpBUReportCtrl.MakeBUReportStatistics(makeInfo)
        for layerStr in reportInfo.layerArr:
            print("Run [MakeBUReportStatistics] to Layer {} run {} SQL".format(layerStr, str(len(layerTaskSQLStrArrMap[layerStr]))))
            taskSQLDeleteStrArr = layerTaskSQLDeleteStrArrMap[layerStr]
            if taskSQLDeleteStrArr != []:
                asyncioSQLTool.runsql(greenplumCtrl, taskSQLDeleteStrArr, printError=True)
            taskSQLStrArr = layerTaskSQLStrArrMap[layerStr]
            if taskSQLStrArr != []:
                asyncioSQLTool.runsql(greenplumCtrl, taskSQLStrArr, printError=True)

if __name__ == "__main__":
    startTime = time.time()
    main({"periodType": ["day"]})
    main({"periodType": ["week"]})
    main({"periodType": ["month"]})
    endTime = time.time()
    print("Send a request at", endTime - startTime, "seconds.")

