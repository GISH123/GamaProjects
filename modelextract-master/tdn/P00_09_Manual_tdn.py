import os , datetime ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import tdn.temp.P21_01_MakeModelExtract as makeModelExtractCtrl

def main(makeTimeStr = ""):
    mainALLData(makeTimeStr)

def mainALLData(makeTimeStr = ""):
    ####################################################################################################
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = [] if makeTimeStr == "" else [makeTimeStr]
    startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    gameName = "tdn"
    ####################################################################################################
    # tdn.P25_01_MakeModelExtract
    makeModelExtract_tableNumberArr = []
    ####################################################################################################
    runMakeModelExtract = True
    startDateStr = "2021-01-13"
    endDateStr = "2021-01-14"
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
            25_01 MakeModelExtract
        """
        makeModelExtract_parametersData = {
            "startdate": [makeDateStr]
            , "enddate": [makeDateStr]
            , "tableNumberArr": makeModelExtract_tableNumberArr
        }
        if runMakeModelExtract == True:
            makeModelExtractCtrl.main(makeModelExtract_parametersData)
        ####################################################################################################

if __name__ == "__main__" :
    main()