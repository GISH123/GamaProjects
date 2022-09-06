import os , datetime
from dotenv import load_dotenv
from info.tdn.tableinfo.TableInfoMain import TableInfoMain
from package.common.database.hdfsCtrl import HDFSCtrl

# 撈取相關
load_dotenv(dotenv_path="env/hdfs.env")
hdfsCtrl = HDFSCtrl(os.getenv("HDFS_HOST"), os.getenv("HDFS_USER"), os.getenv("HDFS_PASSWD"), os.getenv("HDFS_PATH"))
tableInfoMain = TableInfoMain()
tableInfoMap = tableInfoMain.getInitInfoMap()


makeInfo = {
    "tableauReportName" : "01_Login數據指標"
    , "gameName" : "tdn"
    , "gameCHName" : "龍之谷"
    , "schemaName" : "tdn_v"
    , "serverName": "10.10.99.151"
    , "serverPort" : "5432"
    , "dbName" : "bureport"
    , "userName" : ""
}

tableInfoMap["[::DataSourceListXML]"] = {
    "value": [
        tableInfoMain.getBUReportStatisticsInfo(makeInfo)
        , tableInfoMain.getBUReport1001Info(makeInfo)[0]
        , tableInfoMain.getBUReport1002Info(makeInfo)[0]
        , tableInfoMain.getBUReport1003Info(makeInfo)[0]
        , tableInfoMain.getBUReport1102Info(makeInfo)[0]
        , tableInfoMain.getBUReport1103Info(makeInfo)[0]
        , tableInfoMain.getBUReport1131Info(makeInfo)[0]
        , tableInfoMain.getBUReport1132Info(makeInfo)[0]
        , tableInfoMain.getBUReport1133Info(makeInfo)[0]
        , tableInfoMain.getBUReport1134Info(makeInfo)[0]
        , tableInfoMain.getBUReport1135Info(makeInfo)[0]
        , tableInfoMain.getBUReport1136Info(makeInfo)[0]
        , tableInfoMain.getBUReport1137Info(makeInfo)[0]
        , tableInfoMain.getBUReport1802Info(makeInfo)[0]
        , tableInfoMain.getBUReport1804Info(makeInfo)[0]
        , tableInfoMain.getBUReport1806Info(makeInfo)[0]
        , tableInfoMain.getBUReport6001Info(makeInfo)[0]
        , tableInfoMain.getBUReport6002Info(makeInfo)[0]
        , tableInfoMain.getBUReport6011Info(makeInfo)[0]
        , tableInfoMain.getBUReport6012Info(makeInfo)[0]
        , tableInfoMain.getBUReport6019Info(makeInfo)[0]
    ]
}

tableauXMLStr = tableInfoMain.makeXMLByInfoMap(tableInfoMap)
fileName = "{}_{}_模板_{}.twb".format(makeInfo["gameCHName"], makeInfo["tableauReportName"], "0000000000")
outFilePath = 'file/{}/twb/{}'.format(makeInfo["gameName"],fileName)
shfileMake = open(outFilePath,"w", encoding="utf-8", newline="\n")
shfileMake.writelines(tableauXMLStr)
shfileMake.close()

nowTime = datetime.datetime.now()
hdfsPathNameInit = "/user/GTW_PD/Document/04_02_Tableau_Twb"
hdfsPathNameNew = hdfsPathNameInit + "/{}/00_new".format(makeInfo["gameName"])
hdfsPathNameDate = hdfsPathNameInit + "/{}/{}".format(makeInfo["gameName"],nowTime.strftime("%Y%m%d"))
hdfsCtrl.makeDirs(hdfsPathNameNew)
hdfsCtrl.makeDirs(hdfsPathNameDate)
hdfsCtrl.upload("{}/{}".format(hdfsPathNameNew,fileName),outFilePath)
hdfsCtrl.upload("{}/{}".format(hdfsPathNameDate,fileName),outFilePath)