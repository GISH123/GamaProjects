import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.inputCtrl import inputCtrl
from package.common.bureport.gpBUReportStatisticsCtrl import GPBUReportStatisticsCtrl
import package.common.common.GamaniaDateTime as gamaniaDateTime
from dotenv import load_dotenv
import time
import datetime
import pandas


pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)
pandas.set_option("display.float_format", lambda x: '%.4f' % x)

load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/PostgreSQL.env")
gpBUReportStatisticsCtrl = GPBUReportStatisticsCtrl()

def main(parametersData = {}) :
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = ""
    endDateStr = ""
    gameName = ""
    periodType = ""
    reportNameArr = []
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
        if key == "reportNameArr":
            reportNameArr = parametersData[key]
        if key == "firstweekday" :
            firstWeekDay = int(parametersData[key][0])

    startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
    endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    reportNameArr = gpBUReportStatisticsCtrl.getInitReportNameArr() if reportNameArr == [] else reportNameArr
    gameName = "cso" if gameName == "" else gameName
    periodType = "day" if periodType == "" else periodType
    firstWeekDay = 1 if firstWeekDay == None else firstWeekDay

    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
    endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")

    maxDataName = ""
    minDataName = ""
    dataNameArr = []
    for rangeDateInfo in gamaniaDateTime.getRangeDateInfoArr(startDateTime, endDateTime, periodType, firstWeekDay):
        maxDataName = rangeDateInfo['dateName'] if maxDataName == "" else maxDataName
        minDataName = rangeDateInfo['dateName'] if minDataName == "" else minDataName
        maxDataName = rangeDateInfo['dateName'] if maxDataName < rangeDateInfo['dateName'] else maxDataName
        minDataName = rangeDateInfo['dateName'] if minDataName > rangeDateInfo['dateName'] else minDataName
        dataNameArr.append(rangeDateInfo['dateName'])

    databaseInfo = {
        "gameName": gameName
        , "maxDataName": maxDataName
        , "minDataName": minDataName
        , "reportNameArr": reportNameArr
        , "dataNameArr": dataNameArr
    }
    print("Run [MakeBUReport] {} {} to {}".format(periodType, minDataName, maxDataName))
    gpBUReportStatisticsCtrl.MakeBUDetails(databaseInfo)

if __name__ == "__main__":
    startTime = time.time()
    main({"periodType": ["day"]})
    main({"periodType": ["week"]})
    main({"periodType": ["month"]})
    endTime = time.time()
    print("Send a request at", endTime - startTime, "seconds.")

