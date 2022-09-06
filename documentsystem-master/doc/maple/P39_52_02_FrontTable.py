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
    "ME11000_登入與IP" : [
        tableInfoMain.getBUReport11002Info(makeInfo)[1]
        , tableInfoMain.getBUReport11003Info(makeInfo)[1]
        , tableInfoMain.getBUReport11102Info(makeInfo)[1]
        , tableInfoMain.getBUReport11103Info(makeInfo)[1]
        , tableInfoMain.getBUReport11082Info(makeInfo)[1]
        , tableInfoMain.getBUReport11083Info(makeInfo)[1]
        , tableInfoMain.getBUReport11851Info(makeInfo)[1]
        , tableInfoMain.getBUReport11852Info(makeInfo)[1]
        , tableInfoMain.getBUReport11853Info(makeInfo)[1]
        , tableInfoMain.getBUReport11854Info(makeInfo)[1]
        , tableInfoMain.getBUReport11861Info(makeInfo)[1]
        , tableInfoMain.getBUReport11862Info(makeInfo)[1]
    ]
    ,"ME15XXX_社交" : [
        tableInfoMain.getBUReport15009Info(makeInfo)[1]
        , tableInfoMain.getBUReport15109Info(makeInfo)[1]
    ]
}

fileName = "39_52_02_MEFrontTableDoc.xlsx"
initFilePath = 'file/common/doc/ModelExtractDocInit.xlsx'
outFilePath = 'file/{}/doc/{}'.format(makeInfo["gameName"],fileName)
documentCtrl.MakeModelExtractDoc(dataMap,initFilePath,outFilePath)

nowTime = datetime.datetime.now()
hdfsPathNameInit = "/user/GTW_PD/Document/02_ModelExtract"
hdfsPathNameNew = hdfsPathNameInit + "/{}/00_new".format(makeInfo["gameName"])
hdfsPathNameDate = hdfsPathNameInit + "/{}/{}".format(makeInfo["gameName"],nowTime.strftime("%Y%m%d"))
hdfsCtrl.makeDirs(hdfsPathNameNew)
hdfsCtrl.makeDirs(hdfsPathNameDate)
hdfsCtrl.deleteDirs("{}/{}".format(hdfsPathNameNew,fileName))
hdfsCtrl.deleteDirs("{}/{}".format(hdfsPathNameDate,fileName))
hdfsCtrl.deleteDirs("{}/{}".format(hdfsPathNameNew,fileName))
hdfsCtrl.deleteDirs("{}/{}".format(hdfsPathNameDate,fileName))
hdfsCtrl.upload("{}/{}".format(hdfsPathNameNew,fileName),outFilePath)
hdfsCtrl.upload("{}/{}".format(hdfsPathNameDate,fileName),outFilePath)