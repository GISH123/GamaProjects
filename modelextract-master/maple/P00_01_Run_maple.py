import os , datetime ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import maple.P21_01_MakeModelExtract as makeModelExtractCtrl
import maple.P11_30_MakeMiddle as P30Ctrl
import maple.P11_70_UseModel as P70Ctrl

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
    ####################################################################################################
    # tdn.P25_01_MakeModelExtract
    makeModelExtract_tableNumberArr = []
    ####################################################################################################
    runP11_30 = True
    runP11_P70 = True
    runMakeModelExtract = True
    ####################################################################################################
    if makeDateStrArr == []:
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeDateStrArr.append(makeDatetime.strftime("%Y-%m-%d"))
            makeDatetime = makeDatetime + datetime.timedelta(days=1)

    for makeDateStr in makeDateStrArr:
        ####################################################################################################
        """
            runP30
        """
        makeP11_30_parametersData = {
            "startdate": [makeDateStr]
            , "enddate": [makeDateStr]
        }
        if runP11_30 == True:
            P30Ctrl.main(makeP11_30_parametersData)
        ####################################################################################################
        """
            runP70
        """
        makeP11_P70_parametersData = {
            "startdate": [makeDateStr]
            , "enddate": [makeDateStr]
        }
        if runP11_P70 == True:
            try:
                P70Ctrl.main(makeP11_P70_parametersData)
            except:
                print('Warning: model build fail')
        ####################################################################################################
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
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    main(makeDateStr)
    makeDateStr = (nowZeroTime - datetime.timedelta(days=7)).strftime("%Y-%m-%d")
    main(makeDateStr)