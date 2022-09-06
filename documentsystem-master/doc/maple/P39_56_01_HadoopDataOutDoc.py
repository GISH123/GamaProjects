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
    "總部資料介接" : [
        tableInfoMain.getBUReport1002Info(makeInfo)[1]
        , tableInfoMain.getBUReport1003Info(makeInfo)[1]
        , tableInfoMain.getBUReport1132Info(makeInfo)[1]
        , tableInfoMain.getBUReport1133Info(makeInfo)[1]
        , tableInfoMain.getBUReport6002Info(makeInfo)[1]
        , tableInfoMain.getBUReport6019Info(makeInfo)[1]
    ]
}
fileName = "39_52_11_HadoopDataOutDoc.xlsx"
initFilePath = 'file/common/doc/HadoopDataOutInit.xlsx'
outFilePath = 'file/{}/doc/{}'.format(makeInfo["gameName"],fileName)
documentCtrl.MakeModelExtractDoc(dataMap,initFilePath,outFilePath)

