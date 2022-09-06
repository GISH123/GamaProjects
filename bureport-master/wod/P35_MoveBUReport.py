import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.inputCtrl import inputCtrl
from package.common.bureport.gpBUReportCtrl import GPBUReportCtrl
from sql.wod.bureport.ReportInfo import ReportInfo
from dotenv import load_dotenv
import time
import datetime
import pandas as pd
import calendar

pd.set_option('display.max_columns', None)
pd.set_option('display.max_rows', None)
pd.set_option('display.width', 2000)

load_dotenv(dotenv_path="env/hive.env")
load_dotenv(dotenv_path="env/PostgreSQL.env")
load_dotenv(dotenv_path="env/GreenPlum.env")
gpBUReportCtrl = GPBUReportCtrl()
reportInfo= ReportInfo()


def main(parametersData = {}) :
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    startDateStr = ""
    endDateStr = ""
    gameName = ""
    tableNumberArr = []
    if parametersData == {}:
        parametersData = inputCtrl.makeParametersData(sys.argv)

    for key in parametersData.keys():
        if key == "startdate":
            startDateStr = parametersData[key][0]
        if key == "enddate":
            endDateStr = parametersData[key][0]
        if key == "gamename":
            gameName = parametersData[key][0]
        if key == "tableNumberArr":
            tableNumberArr = parametersData[key]

    startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if startDateStr == "" else startDateStr
    endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d") if endDateStr == "" else endDateStr
    tableNumberArr = reportInfo.tableNumberArr if tableNumberArr == [] else tableNumberArr
    gameName = "wod" if gameName == "" else gameName

    makeInfo = {
        "gameName" : gameName
        , "schemaName" : gameName
        , "tableNumberArr": tableNumberArr
        , "startDateNoLine": datetime.datetime.strptime(startDateStr, "%Y-%m-%d").strftime("%Y%m%d")
        , "endDateNoLine": datetime.datetime.strptime(endDateStr, "%Y-%m-%d").strftime("%Y%m%d")
        , "hiveTableName" : "business_bureport"
        , "gpTableStartName" : "bu"
        , "ishaveDataNotMove": False
        , "layerArr": reportInfo.layerArr
        , "layerInfoArrMap": reportInfo.layerInfoArrMap if hasattr(reportInfo, 'layerInfoArrMap') else {}
        , "tableNumberInfoMap": reportInfo.tableNumberInfoMap if hasattr(reportInfo, 'tableNumberInfoMap') else {}
    }
    print("Run [MoveBUDetail] {} to {}".format(startDateStr,endDateStr))
    gpBUReportCtrl.MoveBUReportToGP(makeInfo)

if __name__ == "__main__":
    main()
