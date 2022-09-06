import os , datetime
from info.maple.tableinfo.TableInfoMain import TableInfoMain as TableInfoMain
from package.common.document.documentCtrl import DocumentCtrl as DocumentCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from dotenv import load_dotenv

# 撈取相關
load_dotenv(dotenv_path="env/hdfs.env")
documentCtrl = DocumentCtrl()
tableInfoMain = TableInfoMain()
tableInfoMap = tableInfoMain.getInitInfoMap()
hdfsCtrl = HDFSCtrl(os.getenv("HDFS_HOST"), os.getenv("HDFS_USER"), os.getenv("HDFS_PASSWD"), os.getenv("HDFS_PATH"))

makeInfo = {
    "tableauReportName" : ""
    , "gameName" :"maple"
    , "gameCHName" : ""
    , "schemaName" : ""
    , "serverName": ""
    , "serverPort": ""
    , "dbName": ""
    , "userName": ""
}

dataMap = {
    "BU20000_模型與標籤": [
        tableInfoMain.getBUReport21001Info(makeInfo)[1]
        , tableInfoMain.getBUReport22001Info(makeInfo)[1]
        , tableInfoMain.getBUReport22002Info(makeInfo)[1]
        , tableInfoMain.getBUReport24001Info(makeInfo)[1]
        , tableInfoMain.getBUReport25001Info(makeInfo)[1]
        , tableInfoMain.getBUReport25002Info(makeInfo)[1]
        , tableInfoMain.getBUReport26001Info(makeInfo)[1]
        , tableInfoMain.getBUReport26002Info(makeInfo)[1]
        , tableInfoMain.getBUReport27001Info(makeInfo)[1]
        , tableInfoMain.getBUReport27002Info(makeInfo)[1]

        , tableInfoMain.getBUReport21011Info(makeInfo)[1]
        , tableInfoMain.getBUReport21012Info(makeInfo)[1]

    ]
    ,
    "ME1000_登入與IP": [
        tableInfoMain.getBUReport1001Info(makeInfo)[1]
        , tableInfoMain.getBUReport1002Info(makeInfo)[1]
        , tableInfoMain.getBUReport1003Info(makeInfo)[1]
        , tableInfoMain.getBUReport1101Info(makeInfo)[1]
        , tableInfoMain.getBUReport1102Info(makeInfo)[1]
        , tableInfoMain.getBUReport1103Info(makeInfo)[1]
        , tableInfoMain.getBUReport1131Info(makeInfo)[1]
        , tableInfoMain.getBUReport1132Info(makeInfo)[1]
        , tableInfoMain.getBUReport1133Info(makeInfo)[1]
        , tableInfoMain.getBUReport1134Info(makeInfo)[1]
        , tableInfoMain.getBUReport1135Info(makeInfo)[1]
        , tableInfoMain.getBUReport1136Info(makeInfo)[1]
        , tableInfoMain.getBUReport1137Info(makeInfo)[1]
        , tableInfoMain.getBUReport1802Info(makeInfo)[1]
        , tableInfoMain.getBUReport1804Info(makeInfo)[1]
        , tableInfoMain.getBUReport1806Info(makeInfo)[1]
    ]
    , "ME2000_角色能力": [
        tableInfoMain.getBUReport2001Info(makeInfo)[1]
        , tableInfoMain.getBUReport2002Info(makeInfo)[1]
        , tableInfoMain.getBUReport2101Info(makeInfo)[1]
        , tableInfoMain.getBUReport2102Info(makeInfo)[1]
        , tableInfoMain.getBUReport2103Info(makeInfo)[1]
        , tableInfoMain.getBUReport2111Info(makeInfo)[1]
        , tableInfoMain.getBUReport2112Info(makeInfo)[1]
        , tableInfoMain.getBUReport2113Info(makeInfo)[1]
    ]
    , "ME3000_裝備道具": [
        tableInfoMain.getBUReport3001Info(makeInfo)[1]
        , tableInfoMain.getBUReport3002Info(makeInfo)[1]
        , tableInfoMain.getBUReport3011Info(makeInfo)[1]
        , tableInfoMain.getBUReport3012Info(makeInfo)[1]
    ]
    , "ME4000_任務相關": [
        tableInfoMain.getBUReport4001Info(makeInfo)[1]
        , tableInfoMain.getBUReport4009Info(makeInfo)[1]
    ]
    , "ME5000_社交": [
        tableInfoMain.getBUReport5001Info(makeInfo)[1]
        , tableInfoMain.getBUReport5101Info(makeInfo)[1]
    ]
    , "ME6000_金流": [
        tableInfoMain.getBUReport6001Info(makeInfo)[1]
        , tableInfoMain.getBUReport6002Info(makeInfo)[1]
        , tableInfoMain.getBUReport6011Info(makeInfo)[1]
        , tableInfoMain.getBUReport6012Info(makeInfo)[1]
        , tableInfoMain.getBUReport6019Info(makeInfo)[1]
        , tableInfoMain.getBUReport6509Info(makeInfo)[1]
        , tableInfoMain.getBUReport6609Info(makeInfo)[1]
    ]
    , "ME7000_特別系統": [
    ]
}

fileName = "39_53_01_BuReportDoc.xlsx"
initFilePath = 'file/common/doc/BuReportDocInit.xlsx'
outFilePath = 'file/{}/doc/{}'.format(makeInfo["gameName"],fileName)
documentCtrl.MakeBuReportDoc(dataMap,initFilePath,outFilePath)

nowTime = datetime.datetime.now()
hdfsPathNameInit = "/user/GTW_PD/Document/03_BUReport"
hdfsPathNameNew = hdfsPathNameInit + "/{}/00_new".format(makeInfo["gameName"])
hdfsPathNameDate = hdfsPathNameInit + "/{}/{}".format(makeInfo["gameName"],nowTime.strftime("%Y%m%d"))
hdfsCtrl.makeDirs(hdfsPathNameNew)
hdfsCtrl.makeDirs(hdfsPathNameDate)
hdfsCtrl.deleteDirs("{}/{}".format(hdfsPathNameNew,fileName))
hdfsCtrl.deleteDirs("{}/{}".format(hdfsPathNameDate,fileName))
hdfsCtrl.upload("{}/{}".format(hdfsPathNameNew,fileName),outFilePath)
hdfsCtrl.upload("{}/{}".format(hdfsPathNameDate,fileName),outFilePath)

