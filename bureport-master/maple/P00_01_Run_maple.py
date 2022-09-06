import os , datetime ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import maple.P31_01_MakeBUReport as makeBUReportCtrl
import maple.P35_MoveBUReport as moveBUReportCtrl
import maple.P36_01_MakeBUReportStatistics as makeBUReportStatisticsCtrl
import sys
from package.common.inputCtrl import inputCtrl
import package.common.common.GamaniaDateTime as GDT

def main(makeTimeStr = ""):
    mainALLData(makeTimeStr)

def mainALLData(makeTimeStr = ""):
    ####################################################################################################
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = [] if makeTimeStr == "" else [makeTimeStr]
    startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    gameName = "maple"
    layerDefList = ['101','111'] # 以layerArr 輸入
    parametersData = inputCtrl.makeParametersData(sys.argv)
    ####################################################################################################
    # maple.P31_01_MakeBUReport
    makeBUReport_tableNumberArr = []
    makeBUReport_layerFilePathArr = parametersData.setdefault('layerArr', layerDefList)
    # maple.P35_MoveBUReport
    moveBUReport_tableNumberArr = []

    # maple.P36_MakeBUReportStatistics
    makeBUReportStatistics_reportCodeArr = []
    makeBUReportStatistics_periodType = ["day","week","month"]
    ####################################################################################################
    runMakeBUReport = True
    runMoveBUReport = True
    runMakeBUReportStatistics = True
    ####################################################################################################
    if makeDateStrArr == []:
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeDateStrArr.append(makeDatetime.strftime("%Y-%m-%d"))
            makeDatetime = makeDatetime + datetime.timedelta(days=1)

    for makeDateStr in makeDateStrArr:
        """
            P31_01_MakeBUReport
        """
        makeBUDetail_parametersData = {
            "startdate": [makeDateStr]
            , "enddate": [makeDateStr]
            , "tableNumberArr": makeBUReport_tableNumberArr
            , "layerArr": makeBUReport_layerFilePathArr
        }
        if runMakeBUReport == True:
            makeBUReportCtrl.main(makeBUDetail_parametersData)

        if makeDateStr[-2:] == '4' or makeDateStr[-2:] == '11':
            dt = datetime.datetime.strptime(makeDateStr, "%Y-%m-%d")
            endMonthFirstDate = GDT.getThisMonthFirstDate(dt) + datetime.timedelta(days=-1)
            startMonthFirstDate = GDT.getThisMonthFirstDate(endMonthFirstDate)
            makeBUDetail_parametersData = {
                "startdate": [startMonthFirstDate.strftime("%Y-%m-%d")]
                , "enddate": [endMonthFirstDate.strftime("%Y-%m-%d")]
                , "tableNumberArr": []
                , "layerArr":  ['121','122']
            }

            if runMakeBUReport == True:
                makeBUReportCtrl.main(makeBUDetail_parametersData)
        ####################################################################################################
        """
            P35_MoveBUReport
        """
        moveBUReport_parametersData = {
            "startdate": [makeDateStr]
            , "enddate": [makeDateStr]
            , "tableNumberArr": moveBUReport_tableNumberArr
        }
        if runMoveBUReport == True:
            moveBUReportCtrl.main(moveBUReport_parametersData)
        ####################################################################################################
        """
            P36_MakeBUReportStatistics
        """
        for periodTypeSingle in makeBUReportStatistics_periodType :
            makeBUPeriodReportDetail_parametersData = {
                "startdate": [makeDateStr]
                , "enddate": [makeDateStr]
                , "reportCodeArr": makeBUReportStatistics_reportCodeArr
                , "periodType": [periodTypeSingle]
            }
            if runMakeBUReportStatistics == True:
                makeBUReportStatisticsCtrl.main(makeBUPeriodReportDetail_parametersData)

if __name__ == "__main__" :
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    main(makeDateStr)
    makeDateStr = (nowZeroTime - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    main(makeDateStr)