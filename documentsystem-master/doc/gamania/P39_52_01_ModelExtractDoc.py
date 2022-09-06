import os , datetime
from info.gamania.tableinfo.TableInfoMain import TableInfoMain as TableInfoMain
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
    "tableauReportName": ""
    , "gameName": "gamania"
    , "gameCHName": ""
    , "schemaName": ""
    , "serverName": ""
    , "serverPort": ""
    , "dbName": ""
    , "userName": ""
}

dataMap = {
    "ME10000_基本平台資料": [
        tableInfoMain.getBUReport10001Info(makeInfo)[1]
    ]
    ,"EX10000_登入與IP": [
        tableInfoMain.getBUReport11001Info(makeInfo)[1]
        , tableInfoMain.getBUReport11002Info(makeInfo)[1]
        , tableInfoMain.getBUReport11005Info(makeInfo)[1]
    ]
    , "EX16000_金流": [
        tableInfoMain.getBUReport16001Info(makeInfo)[1]
        , tableInfoMain.getBUReport16002Info(makeInfo)[1]
    ]
}


fileName = "gamania_ModelExtractDoc.xlsx"
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
hdfsCtrl.upload("{}/{}".format(hdfsPathNameNew,fileName),outFilePath)
hdfsCtrl.upload("{}/{}".format(hdfsPathNameDate,fileName),outFilePath)