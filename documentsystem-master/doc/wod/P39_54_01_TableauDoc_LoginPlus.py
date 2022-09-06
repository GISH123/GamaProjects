import os , datetime
from info.wod.tableinfo.TableInfoMain import TableInfoMain as TableInfoMain
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
    , "gameName" :"wod"
    , "gameCHName" : ""
    , "schemaName" : ""
    , "serverName": ""
    , "serverPort": ""
    , "dbName": ""
    , "userName": ""
}

dataMap = {
    "BU1000_登入與IP" : [
        tableInfoMain.getBUReport1001Info(makeInfo)[1]
        , tableInfoMain.getBUReport1002Info(makeInfo)[1]
        , tableInfoMain.getBUReport1003Info(makeInfo)[1]
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
        , tableInfoMain.getBUReport1804Info(makeInfo)[1]
    ]
    ,"BU6000_金流" : [
        tableInfoMain.getBUReport6002Info(makeInfo)[1]
        , tableInfoMain.getBUReport6011Info(makeInfo)[1]
        , tableInfoMain.getBUReport6012Info(makeInfo)[1]
        , tableInfoMain.getBUReport6019Info(makeInfo)[1]
    ]
}
fileName = "WOD_Tableau文件_Login+.xlsx"
initFilePath = 'file/common/doc/TableauDocInit.xlsx'
outFilePath = 'file/{}/doc/{}'.format(makeInfo["gameName"],fileName)
documentCtrl.MakeTableauDoc(dataMap,initFilePath,outFilePath)

nowTime = datetime.datetime.now()
hdfsPathNameInit = "/user/GTW_PD/Document/04_01_Tableau_Doc"
hdfsPathNameNew = hdfsPathNameInit + "/{}/00_new".format(makeInfo["gameName"])
hdfsPathNameDate = hdfsPathNameInit + "/{}/{}".format(makeInfo["gameName"],nowTime.strftime("%Y%m%d"))
hdfsCtrl.makeDirs(hdfsPathNameNew)
hdfsCtrl.makeDirs(hdfsPathNameDate)
hdfsCtrl.deleteDirs("{}/{}".format(hdfsPathNameNew,fileName))
hdfsCtrl.deleteDirs("{}/{}".format(hdfsPathNameDate,fileName))
hdfsCtrl.upload("{}/{}".format(hdfsPathNameNew,fileName),outFilePath)
hdfsCtrl.upload("{}/{}".format(hdfsPathNameDate,fileName),outFilePath)

