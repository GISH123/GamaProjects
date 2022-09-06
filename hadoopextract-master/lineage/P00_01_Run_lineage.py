import os, sys, datetime, time; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import lineage.P10_MakeTableAndPartition as makeTableAndPartition
import lineage.P20_MakeInitExtract as makeInitExtract
import lineage.P21_MakeExtract as makeExtract
import lineage.P50_MakeALLMonthFirst as makeALLMonthFirst
from package.common.inputCtrl import inputCtrl


def main(parametersData={}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    dateRangeArr = []

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

    if makeDateStrArr != []:
        for makeDateStr in makeDateStrArr:
            makeInfo = {
                "makeDateStr": makeDateStr
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
                }
                mainALLData(makeInfo)
    else:
        '''startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeDateStrArr.append(makeDatetime.strftime("%Y-%m-%d"))
            makeDatetime = makeDatetime + datetime.timedelta(days=1)'''
        makeDateStrArr.append((nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d"))
        makeDateStrArr.append((nowZeroTime - datetime.timedelta(days=5)).strftime("%Y-%m-%d"))
        
        for makeDateStr in makeDateStrArr:
            makeInfo = {
                "makeDateStr": makeDateStr
            }
            mainALLData(makeInfo)


def mainALLData(makeInfo):
    ####################################################################################################
    runMakeTableAndPartition = True
    runMakeInitExtract = True
    runMakeExtract = True
    runMakeALLMonthFirst = False


    parametersData = {
        "startdate": [makeInfo["makeDateStr"]]
        , "enddate": [makeInfo["makeDateStr"]]
    }
    ####################################################################################################
    """
    MakeTableAndPartition
    """
    if runMakeTableAndPartition == True:
        makeDate = datetime.datetime.strptime(makeInfo["makeDateStr"], "%Y-%m-%d")
        parametersDataSource = {
            "startdate": [(makeDate - datetime.timedelta(days=-1)).strftime("%Y-%m-%d")]
            , "enddate": [(makeDate - datetime.timedelta(days=-1)).strftime("%Y-%m-%d")]
        }
        print('RUN Lineage MakeTableAndPartition')
        makeTableAndPartition.main(parametersDataSource)
    ####################################################################################################
    """
    MakeInitExtract
    """
    if runMakeInitExtract == True:
        print('RUN Lineage MakeInitExtract')
        makeInitExtract.main(parametersData)
    ####################################################################################################
    """
    MakeExtract
    """
    if runMakeExtract == True:
        print('RUN Lineage MakeExtract')
        makeExtract.main(parametersData)
    ####################################################################################################
    """
    MakeALLMonthFirst
    """
    if runMakeALLMonthFirst == True:
        print('RUN Lineage MakeALLMonthFirst')
        makeALLMonthFirst.main()
    ####################################################################################################


if __name__ == "__main__":
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    main()
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))

