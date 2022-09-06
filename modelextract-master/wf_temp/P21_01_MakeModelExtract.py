import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.asyncio.asyncioSQLTool import AsyncioSQLTool
from package.common.database.hiveCtrl import HiveCtrl
from package.common.modelextract.modelExtractCtrl import ModelExtractCtrl
from sql.wf_temp.modelextract.ReportInfo import ReportInfo
from package.common.database.sqlTool import SqlTool
from package.common.inputCtrl import inputCtrl
from dotenv import load_dotenv
import pandas
import datetime
import package.common.common.GamaniaDateTime as gamaniaDateTime
import copy

load_dotenv(dotenv_path="env/hive.env")
# SQLCtrl
hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

sqlTool = SqlTool()
modelExtractCtrl = ModelExtractCtrl()
asyncioSQLTool = AsyncioSQLTool()

reportInfo= ReportInfo()

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 2000)

def main(parametersData = {}):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeMode = ""
    makeDateStrArr = []
    startDateStr = ""
    endDateStr = ""
    gameName = ""
    worldNameArr = []
    tableNumberArr = []
    layerArr = []

    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "makemode":
            makeMode = parametersData[key][0]
        if key == "makedate":
            makeDateStrArr = parametersData[key]
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]
        if key == "gamename":
            gameName = parametersData[key][0]
        if key == "worldNameArr":
            worldNameArr = parametersData[key]
        if key == "tableNumberArr":
            tableNumberArr = parametersData[key]
        if key == "layerArr":
            layerArr = parametersData[key]

    makeMode = "SingleDay" if makeMode == "" else makeMode
    if makeDateStrArr == []:
        startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
        endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    gameName = "wf_temp" if gameName == "" else gameName
    worldNameArr = reportInfo.worldNameArr if worldNameArr == [] else worldNameArr
    tableNumberArr = reportInfo.tableNumberArr if tableNumberArr == [] else tableNumberArr
    layerArr = reportInfo.layerArr if layerArr == [] else layerArr

    makeTimeArr = []
    if makeDateStrArr != []:
        for makeDateStr in makeDateStrArr:
            makeTimeArr.append(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d"))
    else:
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d")
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeTimeArr.append(makeDatetime)
            makeDatetime = makeDatetime + datetime.timedelta(days=1)

    layerTaskSQLStrArrDayMap = {}
    layerTaskLinkSQLStrArrDayMap = {}
    # 收集每天的語法
    for makeTime in makeTimeArr:
        worldNameArr_make = worldNameArr
        print("Run [MakeModelExtract] to {}".format(makeTime.strftime("%Y-%m-%d")))
        makeInfo = {
            "makeTime": makeTime
            , "gameName": gameName
            , "reportName": "modelextract"
            , "worldNameArr": worldNameArr_make
            , "tableNumberArr": tableNumberArr
            , "layerArr": layerArr
            , "layerInfoArrMap": reportInfo.layerInfoArrMap if hasattr(reportInfo,'layerInfoArrMap') else {}
            , "tableNumberInfoMap" : reportInfo.tableNumberInfoMap if hasattr(reportInfo,'tableNumberInfoMap') else {}
        }
        layerTaskSQLStrArrDayMap[makeTime.strftime("%Y-%m-%d")] = makeModelExtract(makeInfo)
        layerTaskLinkSQLStrArrDayMap[makeTime.strftime("%Y-%m-%d")] = makeLayerTaskLinkSQLStrArrDayMap(makeInfo)

    if makeMode == "SingleDay":
        for makeTime in makeTimeArr:
            layerTaskSQLStrArrMap = layerTaskSQLStrArrDayMap[makeTime.strftime("%Y-%m-%d")]
            for layerStr in layerArr:
                print("Run [MakeModelExtract] to Layer {} run {} SQL".format(layerStr, str(len(layerTaskSQLStrArrMap[layerStr]))))
                taskSQLStrArr = layerTaskSQLStrArrMap[layerStr]
                if taskSQLStrArr != []:
                    asyncioSQLTool.runsql(hiveCtrl, taskSQLStrArr, printError=True)
    elif makeMode == "MutiDay":
        layerTaskSQLStrArrMap = {}
        for layerNumber in layerArr:
            layerTaskSQLStrArrMap[layerNumber] = []
        for makeTime in makeTimeArr:
            layerTaskSQLStrSingleArrMap = layerTaskSQLStrArrDayMap[makeTime.strftime("%Y-%m-%d")]
            for layerStr in layerArr:
                taskSQLStrArr = layerTaskSQLStrArrMap[layerStr]
                taskSQLStrSingleArr = layerTaskSQLStrSingleArrMap[layerStr]
                for taskSQLStr in taskSQLStrSingleArr:
                    taskSQLStrArr.append(taskSQLStr)
                layerTaskSQLStrArrMap[layerStr] = taskSQLStrArr
        for layerStr in layerArr:
            print("Run [MakeModelExtract] to Layer {} run {} SQL".format(layerStr, str(len(layerTaskSQLStrArrMap[layerStr]))))
            taskSQLStrArr = layerTaskSQLStrArrMap[layerStr]
            if taskSQLStrArr != []:
                asyncioSQLTool.runsql(hiveCtrl, taskSQLStrArr, printError=True)

    for makeTime in makeTimeArr:
        print("Run [MakeModelExtractLink] to Layer SQL")
        linkSQLStrArrDayMapAll = layerTaskLinkSQLStrArrDayMap[makeTime.strftime("%Y-%m-%d")]
        for linkSQLStrArrDayMapKey in linkSQLStrArrDayMapAll.keys() :
            linkSQLStrArrDayMap = linkSQLStrArrDayMapAll[linkSQLStrArrDayMapKey]
            for layerStr in layerArr:
                taskSQLStrArr = linkSQLStrArrDayMap[layerStr]
                for taskSQLStr in taskSQLStrArr :
                    # print(taskSQLStr)
                    asyncioSQLTool.executeSQL(hiveCtrl, taskSQLStr, printError=False)

def makeLayerTaskLinkSQLStrArrDayMap (makeInfo):
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeTime = makeInfo["makeTime"]
    tableNumberArr = makeInfo["tableNumberArr"]
    tableNumberInfoMap = makeInfo["tableNumberInfoMap"]
    linkCount = 0
    linkSQLStrArrDayMapAll = {}
    for tableNumber in tableNumberArr:
        if tableNumber in tableNumberInfoMap.keys():
            if "linkTable" in tableNumberInfoMap[tableNumber].keys():
                linkTableArr = tableNumberInfoMap[tableNumber]["linkTable"]
                for linkTable in linkTableArr:
                    startDateLinkTime = makeTime + datetime.timedelta(days=linkTable[1])
                    endDateLinkTime = makeTime + datetime.timedelta(days=linkTable[2])
                    makeDateLinkTime = startDateLinkTime
                    endDateLinkTime = endDateLinkTime if endDateLinkTime <= (nowZeroTime - datetime.timedelta(days=2)) else nowZeroTime - datetime.timedelta(days=2)
                    while makeDateLinkTime <= endDateLinkTime:
                        makeInfo["makeTime"] = makeDateLinkTime
                        makeInfo["tableNumberArr"] = [linkTable[0]]
                        linkSQLStrArrDayMapAll[linkCount] = makeModelExtract(makeInfo)
                        makeDateLinkTime = makeDateLinkTime + datetime.timedelta(days=1)
                        linkCount = linkCount + 1
    return linkSQLStrArrDayMapAll

def makeModelExtract (makeInfo):
    makeTime = makeInfo["makeTime"]
    gameName = makeInfo["gameName"]
    firstMonthTime = gamaniaDateTime.getThisMonthFirstDate(makeTime)
    lastMonthTime = gamaniaDateTime.getThisMonthLastDate(makeTime)
    makeInfo["gameIdCodeArr"] = [[reportInfo.serverCode,gameName]]
    makeInfo["sqlReplaceArr"] = [
        ["[:GameName]", gameName]
        , ["[:DateLine]", makeTime.strftime("%Y-%m-%d")]
        , ["[:DateNoLine]", makeTime.strftime("%Y%m%d")]
        , ["[:DateLineFirstMonth]", firstMonthTime.strftime("%Y-%m-%d")]
        , ["[:DateLineLastMonth]", lastMonthTime.strftime("%Y-%m-%d")]
        , ["[:DateNoLineFirstMonth]", firstMonthTime.strftime("%Y%m%d")]
        , ["[:DateNoLineLastMonth]", lastMonthTime.strftime("%Y%m%d")]
    ]
    return modelExtractCtrl.makeOtherDetil(makeInfo)

if __name__ == "__main__":
    main({"makedate":["2021-07-24"]})

