import os , datetime ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import wod.P11_00_MakeInitExtract as makeInitExtractCtrl
import wod.P11_10_MakeALL as makeALLCtrl
import wod.P11_11_MakeALLMonthFirst as makeALLMonthFirstCtrl


def main(makeTimeStr = ""):
    mainALLData(makeTimeStr)

def mainALLData(makeTimeStr = ""):
    ####################################################################################################
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = [] if makeTimeStr == "" else [makeTimeStr]
    startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    gameName = "wod"
    hiveDBName = ""
    gameWorldArr = []
    typeArray = []  # 11_10 階段 如何重作中繼站資料
    tableNameArray = []  # 11_10 階段 如何重作中繼站資料
    ####################################################################################################
    runMakeInitExtract = True
    runMakeALL = True
    runMakeALLMonthFirst = False
    #startDateStr = "2021-08-21"
    #endDateStr = "2021-08-21"
    """
        11_00 每日登入製作
    """
    print('start P11_00_MakeInitExtract')
    makeInitMakeDateStrArr = []
    if makeDateStrArr != []:
        makeDateStrSet = set()
        for makeDateStr in makeDateStrArr:
            makeDateStrSet.add((datetime.datetime.strptime(makeDateStr, "%Y-%m-%d") - datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
            makeDateStrSet.add(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d").strftime("%Y-%m-%d"))
        makeInitMakeDateStrArr = list(makeDateStrSet)
        makeInitMakeDateStrArr.sort()
    else :
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d") - datetime.timedelta(days=1)
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeInitMakeDateStrArr.append(makeDatetime.strftime("%Y-%m-%d"))
            makeDatetime = makeDatetime + datetime.timedelta(days=1)
    makeInit_parametersData = {
        "makedate": makeInitMakeDateStrArr
    }
    if runMakeInitExtract == True:
        makeInitExtractCtrl.main(makeInit_parametersData)
    print('end P11_00_MakeInitExtract')
    ####################################################################################################
    """
        02_10 中繼站資料製作
    """
    print('start P11_10_MakeALL')
    if makeDateStrArr != []:
        makeALLMakeDateBeforeStrArr = []
        makeALLMakeDateStrArr = []
        makeDateBeforeStrSet = set()
        makeDateStrSet = set()
        for makeDateStr in makeDateStrArr:
            beforeMakeDayStr =  (datetime.datetime.strptime(makeDateStr, "%Y-%m-%d") - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
            makeDayStr = datetime.datetime.strptime(makeDateStr, "%Y-%m-%d").strftime("%Y-%m-%d")
            makeDateBeforeStrSet.add(beforeMakeDayStr)
            if makeDayStr in makeDateBeforeStrSet :
                makeDateBeforeStrSet.remove(makeDayStr)
            makeDateStrSet.add(makeDayStr)
        makeALLMakeDateBeforeStrArr = list(makeDateBeforeStrSet)
        makeALLMakeDateBeforeStrArr.sort()
        makeALLMakeDateStrArr = list(makeDateStrSet)
        makeALLMakeDateStrArr.sort()

        makeALL_parametersData = {
            "makedate": makeALLMakeDateBeforeStrArr
            , "gamename": [gameName]
            , "dbname": [hiveDBName]
            , "gameWorldArr": gameWorldArr
            , "typeArray": ["grammar"]
            , "tableNameArray": tableNameArray
        }
        if runMakeALL == True:
            makeALLCtrl.main(makeALL_parametersData)

        makeALL_parametersData = {
            "makedate": makeALLMakeDateStrArr
            , "gamename": [gameName]
            , "dbname": [hiveDBName]
            , "gameWorldArr": gameWorldArr
            , "typeArray": typeArray
            , "tableNameArray": tableNameArray
        }
        if runMakeALL == True:
            makeALLCtrl.main(makeALL_parametersData)

    else :
        makeALLMakeDateStrArr = []
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d") - datetime.timedelta(days=1)
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeALLMakeDateStrArr.append(makeDatetime.strftime("%Y-%m-%d"))
            makeDatetime = makeDatetime + datetime.timedelta(days=1)
        makeALL_parametersData = {
            "makedate": makeALLMakeDateStrArr
            , "gamename": [gameName]
            , "dbname": [hiveDBName]
            , "gameWorldArr": gameWorldArr
            , "typeArray": typeArray
            , "tableNameArray": tableNameArray
        }
        if runMakeALL == True:
            makeALLCtrl.main(makeALL_parametersData)
    print('end P11_10_MakeALL')
    ####################################################################################################
    """
        02_11 每月備份製作
    """
    print('start P11_11_MakeALLMonthFirst')
    makeALLMonthFirstMakeDateStrArr = []
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeALLMonthFirstMakeDateStrArr.append(nowZeroTime)
    makeALLMonthFirst_parametersData = {
        "makedate": makeALLMakeDateStrArr
        , "gamename": [gameName]
        , "dbname": [hiveDBName]
        , "typeArray": typeArray
        , "tableNameArray": tableNameArray
    }
    if runMakeALLMonthFirst == True:
        makeALLMonthFirstCtrl.main(makeALLMonthFirst_parametersData)
    print('end P11_11_MakeALLMonthFirst')
    ####################################################################################################

if __name__ == "__main__" :
    startDateStr = "2021-08-21"
    endDateStr = "2021-08-21"
    startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d") - datetime.timedelta(days=1)
    endDateTime = datetime.datetime.strptime( endDateStr, "%Y-%m-%d")
    makeDatetime = startDateTime
    while makeDatetime <= endDateTime:
        main(makeDatetime.strftime("%Y-%m-%d"))
        makeDatetime = makeDatetime + datetime.timedelta(days=1)

