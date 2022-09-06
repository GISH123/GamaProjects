import os, sys, datetime, time; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import gamania.P21_01_MakeModelExtract as makeModelExtractCtrl
from package.common.inputCtrl import inputCtrl


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

    gameName = "gamania" if gameName == "" else gameName
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
        endDateStr = (nowZeroTime - datetime.timedelta(days=7)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
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
    runMakeModelExtract = True
    parametersData = {
        "startdate": [makeInfo["makeDateStr"]]
        , "enddate": [makeInfo["makeDateStr"]]
        , "gamename": [makeInfo["gameName"]]
        , "tableNumberArr": makeInfo["tableNumberArr"]
    }
    ####################################################################################################
    """
    MakeModelExtract
    """
    if runMakeModelExtract == True:
        makeModelExtractCtrl.main(parametersData)
    ####################################################################################################


if __name__ == "__main__":
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    # main()
    main({"makedate": ["2021-06-10"]})
    # main({"makedate": ["2021-06-10"], "tableNumberArr": ["16002", "6002"]})
    # main({"dateRangeArr": [['2021-01-01', '2021-01-31'], ['2021-02-01', '2021-02-28'], ['2021-03-01', '2021-03-31']]})
    # main({"startdate": ["2021-06-04"], "enddate": ["2021-06-04"], "tableNumberArr": ["11001", "11002", "11005", "16001", "16002", "1001", "1101", "6002"]})
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))

