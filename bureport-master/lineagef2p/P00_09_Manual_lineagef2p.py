import os, sys, datetime, time ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.inputCtrl import inputCtrl
import lineagef2p.P31_01_MakeBUReport as makeBUReportCtrl
import lineagef2p.P35_MoveBUReport as moveBUReportCtrl
import lineagef2p.P36_01_MakeBUReportStatistics as makeBUReportStatisticsCtrl
# import lineagef2p.P36_02_MakeBUReport as makeBUReportExcelCtrl
import package.common.common.GamaniaDateTime as gamaniaDateTime

def main(parametersData={}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    dateRangeArr = []
    gameName = ""
    tableNumberArr = []
    runDataType = []

    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "makedate":
            makeDateStrArr = parametersData[key]
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]
        if key == "dateRangeArr":
            dateRangeArr = parametersData[key]
        if key == "gamename":
            gameName = parametersData[key][0]
        if key == "tableNumberArr":
            tableNumberArr = parametersData[key]
        if key == "runDataType":
            runDataType = parametersData[key]

    gameName = "lineagef2p" if gameName == "" else gameName
    # tableNumberArr = [] if tableNumberArr == [] else tableNumberArr
    runDataType = [1, 1, 1, 0] if runDataType == [] else runDataType

    if makeDateStrArr != []:
        for makeDateStr in makeDateStrArr:
            makeInfo = {
                "makeDateStr": makeDateStr
                , "gameName": gameName
                , "tableNumberArr": tableNumberArr
                , "runDataType": runDataType
            }
            mainALLData(makeInfo)
    elif dateRangeArr != []:
        for makeDateRange in dateRangeArr:
            startDateTime = datetime.datetime.strptime(makeDateRange[0], "%Y-%m-%d")
            endDateTime = datetime.datetime.strptime(makeDateRange[1], "%Y-%m-%d")
            makeDateStrArr = []
            makeDatetime = startDateTime
            while makeDatetime <= endDateTime:
                makeDateStrArr.append(makeDatetime.strftime("%Y-%m-%d"))
                makeDatetime = makeDatetime + datetime.timedelta(days=1)
            for makeDateStr in makeDateStrArr:
                makeInfo = {
                    "makeDateStr": makeDateStr
                    , "gameName": gameName
                    , "tableNumberArr": tableNumberArr
                    , "runDataType": runDataType
                }
                mainALLData(makeInfo)
    else:
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeDateStrArr.append(makeDatetime.strftime("%Y-%m-%d"))
            makeDatetime = makeDatetime + datetime.timedelta(days=1)
        for makeDateStr in makeDateStrArr:
            makeInfo = {
                "makeDateStr": makeDateStr
                , "gameName": gameName
                , "tableNumberArr": tableNumberArr
                , "runDataType": runDataType
            }
            mainALLData(makeInfo)


def mainALLData(makeInfo):
    ####################################################################################################
    runMakeBUReport = True if int(makeInfo["runDataType"][0]) == 1 else False
    runMoveBUReport = True if int(makeInfo["runDataType"][1]) == 1 else False
    runMakeBUReportStatistics = True if int(makeInfo["runDataType"][2]) == 1 else False
    runMakeBUReportExcel = True if int(makeInfo["runDataType"][3]) == 1 else False

    parametersData = {
        "startdate": [makeInfo["makeDateStr"]]
        , "enddate": [makeInfo["makeDateStr"]]
        , "gamename": [makeInfo["gameName"]]
        , "tableNumberArr": makeInfo["tableNumberArr"]
    }
    # MakeBUReportStatistics
    makeBUReportStatistics_reportCodeArr = []
    makeBUReportStatistics_periodType = ["day", "week", "month"]
    # MakeBUReport
    makeBUReportExcel_reportNameArr = ["loginbasicreport", "logintimereport", "incomereport", "incomelogintypereport"]
    makeBUReportExcel_periodType = ["day"]
    ####################################################################################################
    """
    MakeBUReport
    """
    if runMakeBUReport == True:
        makeBUReportCtrl.main(parametersData)
    ####################################################################################################
    """
    MoveBUReport
    """
    if runMoveBUReport == True:
        moveBUReportCtrl.main(parametersData)
    ####################################################################################################
    """
        P36_01_MakeBUReportStatistics 製作報表資料
    """
    makeDatetime = datetime.datetime.strptime(makeInfo["makeDateStr"], "%Y-%m-%d")
    lastWeekTime = gamaniaDateTime.getThisWeekLastDate(makeDatetime, 1).strftime("%Y-%m-%d")
    lastMonthTime = gamaniaDateTime.getThisMonthLastDate(makeDatetime).strftime("%Y-%m-%d")
    for periodType in makeBUReportStatistics_periodType:
        makeBUReportStatistics_parametersData = {
            "startdate": [makeInfo["makeDateStr"]]
            , "enddate": [makeInfo["makeDateStr"]]
            , "reportCodeArr": makeBUReportStatistics_reportCodeArr
            , "periodType": [periodType]
        }
        if runMakeBUReportStatistics == True:
            if periodType == 'month' and lastMonthTime == makeInfo["makeDateStr"]:
                makeBUReportStatisticsCtrl.main(makeBUReportStatistics_parametersData)
            elif periodType == 'week' and lastWeekTime == makeInfo["makeDateStr"]:
                makeBUReportStatisticsCtrl.main(makeBUReportStatistics_parametersData)
            elif periodType == 'day':
                makeBUReportStatisticsCtrl.main(makeBUReportStatistics_parametersData)
    ####################################################################################################
    """
        P36_02_MakeBUReport 製作報表
    """
    makeBUReportExcel_parametersData = {
        "startdate": [makeInfo["makeDateStr"]]
        , "enddate": [makeInfo["makeDateStr"]]
        , "reportNameArr": makeBUReportExcel_reportNameArr
        , "periodType": makeBUReportExcel_periodType
    }
    if runMakeBUReportExcel == True:
        # makeBUReportExcelCtrl.main(makeBUReportExcel_parametersData)
        pass

if __name__ == "__main__":
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    # main()
    # main({"makedate": ["2021-01-28", "2021-02-10", "2021-02-11"]})
    # main({"makedate": ["2021-01-01"], "tableNumberArr": ["1003"]})
    # main({"dateRangeArr": [['2021-01-03', '2021-01-08'], ['2021-01-19', '2021-01-24']]})
    # main({"dateRangeArr": [['2021-01-20', '2021-01-24']]})
    main({"startdate": ["2021-01-02"], "enddate": ["2021-07-17"], "tableNumberArr": ["1003"]})
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))

