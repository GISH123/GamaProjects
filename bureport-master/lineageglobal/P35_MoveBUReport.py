import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.inputCtrl import inputCtrl
from package.common.bureport.gpBUReportCtrl import GPBUReportCtrl
from package.common.bureport.gpCreateViewCtrl import GPCreateViewCtrl
from sql.lineageglobal.bureport.ReportInfo import ReportInfo
from dotenv import load_dotenv
from package.common.common.colorsCtrl import ColorCtrl
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
gpCreateViewCtrl = GPCreateViewCtrl()
colorCtrl = ColorCtrl()
reportInfo = ReportInfo()


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
    gameName = "lineageglobal" if gameName == "" else gameName

    makeInfo = {
        "gameName": gameName
        , "schemaName": gameName
        # 第一個為View的SchemaName，若無多產品可為空
        , "viewSchemaMapArr": ['lineage', 'lineagef2p', 'lineageglobal', 'lineageweb']
        , "tableNumberArr": tableNumberArr
        , "startDateNoLine": datetime.datetime.strptime(startDateStr, "%Y-%m-%d").strftime("%Y%m%d")
        , "endDateNoLine": datetime.datetime.strptime(endDateStr, "%Y-%m-%d").strftime("%Y%m%d")
        , "hiveTableName": "business_bureport"
        , "gpTableStartName": "bu"
        , "ishaveDataNotMove": False
        , "layerArr": reportInfo.layerArr
        , "layerInfoArrMap": reportInfo.layerInfoArrMap if hasattr(reportInfo, 'layerInfoArrMap') else {}
        , "tableNumberInfoMap": reportInfo.tableNumberInfoMap if hasattr(reportInfo, 'tableNumberInfoMap') else {}
    }
    print(f"Run {colorCtrl.fg(gameName, 160)} [MoveBUDetail] {startDateStr} to {endDateStr}")
    startRunTime_2 = time.time()
    gpBUReportCtrl.MoveBUReportToGP(makeInfo)
    gpCreateViewCtrl.CreateView(makeInfo)
    print(f"Total Used {time.time() - startRunTime_2} seconds.")


if __name__ == "__main__":
    print('start_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
    main()
    # main({"startdate": ["2021-10-11"], "enddate": ["2021-10-11"], "tableNumberArr": []})
    print('end_time:' + time.strftime("%Y-%m-%d %H:%M:%S"))
