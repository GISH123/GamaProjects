import os, sys, datetime, time ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.inputCtrl import inputCtrl
import lineageweb.P31_01_MakeBUReport as makeBUReportCtrl
import lineageweb.P35_MoveBUReport as moveBUReportCtrl
import lineageweb.P36_01_MakeBUReportStatistics as makeBUReportStatisticsCtrl
# import lineageweb.P36_02_MakeBUReport as makeBUReportExcelCtrl
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

    gameName = "lineageweb" if gameName == "" else gameName
    # tableNumberArr = [] if tableNumberArr == [] else tableNumberArr

    if makeDateStrArr != []:
        for makeDateStr in makeDateStrArr:
            makeInfo = {
                "makeDateStr": makeDateStr
                , "gameName": gameName
                , "tableNumberArr": tableNumberArr
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
            }
            mainALLData(makeInfo)


def mainALLData(makeInfo):
    ####################################################################################################
    runMakeBUReport = True
    runMoveBUReport = True
    runMakeBUReportStatistics = False
    runMakeBUReportExcel = False
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
        P36_01_MakeBUReportStatistics ??????????????????
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
        P36_02_MakeBUReport ????????????
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
    # main({"makedate": ["2020-12-17", "2020-12-23"]})
    # main({"makedate": ["2021-01-01"], "tableNumberArr": ["16509", "16503", "6509", "6503"]})
    '''main({"dateRangeArr": [
        # All
        # ['2020-12-01', '2020-12-31'], ['2020-11-01', '2020-11-30']
        # , ['2020-10-01', '2020-10-31'], ['2020-09-01', '2020-09-30']
        # , ['2020-08-01', '2020-08-31'], ['2020-07-01', '2020-07-31']
        # , ['2020-06-01', '2020-06-30'], ['2020-05-01', '2020-05-31']
        # , ['2020-04-01', '2020-04-30'], ['2020-03-01', '2020-03-31']
        # , ['2020-02-01', '2020-02-29'], ['2020-01-01', '2020-01-31']
    ]})'''
    main({"startdate": ["2021-04-07"], "enddate": ["2021-05-22"]})
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))

