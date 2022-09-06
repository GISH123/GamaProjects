import os , datetime
from info.other.tableinfo.TableInfoMain import TableInfoMain as TableInfoMain
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
    , "gameName" :"other"
    , "gameCHName" : ""
    , "schemaName" : ""
    , "serverName": ""
    , "serverPort": ""
    , "dbName": ""
    , "userName": ""
}

dataMap = {
    "ME11000_登入與IP" : [
        tableInfoMain.getBUReport11851Info(makeInfo)[1]
        ,tableInfoMain.getBUReport11852Info(makeInfo)[1]
    ]
}

fileName = "39_52_02_ModelExtractDoc10000.xlsx"
initFilePath = 'file/common/doc/ModelExtractDocInit.xlsx'
outFilePath = 'file/{}/doc/{}'.format(makeInfo["gameName"],fileName)
documentCtrl.MakeModelExtractDoc(dataMap,initFilePath,outFilePath)

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
