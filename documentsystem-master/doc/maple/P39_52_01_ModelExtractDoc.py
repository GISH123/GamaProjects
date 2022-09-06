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
    "ME1000_登入與IP" : [
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
        , tableInfoMain.getBUReport11851Info(makeInfo)[1]
        , tableInfoMain.getBUReport11852Info(makeInfo)[1]
        , tableInfoMain.getBUReport11853Info(makeInfo)[1]
        , tableInfoMain.getBUReport11854Info(makeInfo)[1]
        , tableInfoMain.getBUReport11861Info(makeInfo)[1]
        , tableInfoMain.getBUReport11862Info(makeInfo)[1]
    ]
    ,"ME2000_角色能力" : [
        tableInfoMain.getBUReport2001Info(makeInfo)[1]
        , tableInfoMain.getBUReport2002Info(makeInfo)[1]
        , tableInfoMain.getBUReport2101Info(makeInfo)[1]
        , tableInfoMain.getBUReport2102Info(makeInfo)[1]
        , tableInfoMain.getBUReport2103Info(makeInfo)[1]
        , tableInfoMain.getBUReport2111Info(makeInfo)[1]
        , tableInfoMain.getBUReport2112Info(makeInfo)[1]
        , tableInfoMain.getBUReport2113Info(makeInfo)[1]
    ]
    ,"ME3000_裝備道具" : [
        tableInfoMain.getBUReport3001Info(makeInfo)[1]
        , tableInfoMain.getBUReport3002Info(makeInfo)[1]
        , tableInfoMain.getBUReport3011Info(makeInfo)[1]
        , tableInfoMain.getBUReport3012Info(makeInfo)[1]
        , tableInfoMain.getBUReport13051Info(makeInfo)[1]
        , tableInfoMain.getBUReport13052Info(makeInfo)[1]
        , tableInfoMain.getBUReport13053Info(makeInfo)[1]
        , tableInfoMain.getBUReport13054Info(makeInfo)[1]
        , tableInfoMain.getBUReport13091Info(makeInfo)[1]
    ]
    ,"ME4000_任務相關" : [
        tableInfoMain.getBUReport4001Info(makeInfo)[1]
        , tableInfoMain.getBUReport4009Info(makeInfo)[1]
    ]
    ,"ME5000_社交" : [
        tableInfoMain.getBUReport5001Info(makeInfo)[1]
        , tableInfoMain.getBUReport5101Info(makeInfo)[1]
        , tableInfoMain.getBUReport15009Info(makeInfo)[1]
        , tableInfoMain.getBUReport15109Info(makeInfo)[1]
    ]
    ,"ME6000_金流" : [
        tableInfoMain.getBUReport6001Info(makeInfo)[1]
        , tableInfoMain.getBUReport6002Info(makeInfo)[1]
        , tableInfoMain.getBUReport6006Info(makeInfo)[1]
        , tableInfoMain.getBUReport6007Info(makeInfo)[1]
        , tableInfoMain.getBUReport6011Info(makeInfo)[1]
        , tableInfoMain.getBUReport6012Info(makeInfo)[1]
        , tableInfoMain.getBUReport6019Info(makeInfo)[1]
        , tableInfoMain.getBUReport6509Info(makeInfo)[1]
        , tableInfoMain.getBUReport6609Info(makeInfo)[1]
        , tableInfoMain.getBUReport16009Info(makeInfo)[1]
        , tableInfoMain.getBUReport16301Info(makeInfo)[1]
        , tableInfoMain.getBUReport16401Info(makeInfo)[1]
        , tableInfoMain.getBUReport16451Info(makeInfo)[1]
        , tableInfoMain.getBUReport16452Info(makeInfo)[1]
        , tableInfoMain.getBUReport16453Info(makeInfo)[1]
        , tableInfoMain.getBUReport16509Info(makeInfo)[1]
        , tableInfoMain.getBUReport16608Info(makeInfo)[1]
        , tableInfoMain.getBUReport16609Info(makeInfo)[1]
        , tableInfoMain.getBUReport16091Info(makeInfo)[1]
        , tableInfoMain.getBUReport16092Info(makeInfo)[1]
    ]
    ,"ME7000_特別系統" : [
        tableInfoMain.getBUReport17001Info(makeInfo)[1]
        , tableInfoMain.getBUReport17002Info(makeInfo)[1]
    ]

}
fileName = "39_52_01_ModelExtractDoc.xlsx"
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
