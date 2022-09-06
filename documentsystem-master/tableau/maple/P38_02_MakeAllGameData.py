import os , datetime
from dotenv import load_dotenv
from info.maple.tableinfo.TableInfoMain import TableInfoMain
from package.common.database.hdfsCtrl import HDFSCtrl
from info.maple.tableinfo.TableInfo_add import TableInfo_add
# 撈取相關
load_dotenv(dotenv_path="env/hdfs.env")
hdfsCtrl = HDFSCtrl(os.getenv("HDFS_HOST"), os.getenv("HDFS_USER"), os.getenv("HDFS_PASSWD"), os.getenv("HDFS_PATH"))
tableInfoMain = TableInfoMain()
tableInfoMap = tableInfoMain.getInitInfoMap()


makeInfo = {
    "tableauReportName" : "02_全數據"
    , "gameName" : "maple"
    , "gameCHName" : "楓之谷"
    , "schemaName" : "maple_v"
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
        , tableInfoMain.getBUReport1003Info(makeInfo)[0]
        , tableInfoMain.getBUReport1101Info(makeInfo)[0]
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
        , tableInfoMain.getBUReport1851Info(makeInfo)[0]
        # BU2000_角色能力
        , tableInfoMain.getBUReport2001Info(makeInfo)[0]
        , tableInfoMain.getBUReport2002Info(makeInfo)[0]
        , tableInfoMain.getBUReport2101Info(makeInfo)[0]
        , tableInfoMain.getBUReport2102Info(makeInfo)[0]
        , tableInfoMain.getBUReport2103Info(makeInfo)[0]
        , tableInfoMain.getBUReport2111Info(makeInfo)[0]
        , tableInfoMain.getBUReport2112Info(makeInfo)[0]
        , tableInfoMain.getBUReport2113Info(makeInfo)[0]
        # ME3000_裝備道具
        , tableInfoMain.getBUReport3001Info(makeInfo)[0]
        , tableInfoMain.getBUReport3002Info(makeInfo)[0]
        , tableInfoMain.getBUReport3011Info(makeInfo)[0]
        , tableInfoMain.getBUReport3012Info(makeInfo)[0]
        # BU4000_任務相關
        , tableInfoMain.getBUReport4001Info(makeInfo)[0]
        # BU3000_裝備道具
        , tableInfoMain.getBUReport3001Info(makeInfo)[0]
        , tableInfoMain.getBUReport3002Info(makeInfo)[0]
        # BU5XXX_社交
        , tableInfoMain.getBUReport5001Info(makeInfo)[0]
        , tableInfoMain.getBUReport5101Info(makeInfo)[0]
        # BU6000_金流
        , tableInfoMain.getBUReport6002Info(makeInfo)[0]
        , tableInfoMain.getBUReport6011Info(makeInfo)[0]
        , tableInfoMain.getBUReport6012Info(makeInfo)[0]
        , tableInfoMain.getBUReport6019Info(makeInfo)[0]
        , tableInfoMain.getBUReport6509Info(makeInfo)[0]
        , tableInfoMain.getBUReport6609Info(makeInfo)[0]

        , tableInfoMain.getBUReport21001Info(makeInfo)[0]
        , tableInfoMain.getBUReport22001Info(makeInfo)[0]
        , tableInfoMain.getBUReport22002Info(makeInfo)[0]
        , tableInfoMain.getBUReport24001Info(makeInfo)[0]
        , tableInfoMain.getBUReport25001Info(makeInfo)[0]
        , tableInfoMain.getBUReport25002Info(makeInfo)[0]
        , tableInfoMain.getBUReport26001Info(makeInfo)[0]
        , tableInfoMain.getBUReport26002Info(makeInfo)[0]
        , tableInfoMain.getBUReport27001Info(makeInfo)[0]
        , tableInfoMain.getBUReport27002Info(makeInfo)[0]

        , tableInfoMain.getBUReport21011Info(makeInfo)[0]
        , tableInfoMain.getBUReport21012Info(makeInfo)[0]

        , TableInfo_add.getBURport6019_2001_2002Info(makeInfo)[0]
        , TableInfo_add.getBURport1003_2001_2002Info(makeInfo)[0]
        , TableInfo_add.getBURport1806_22002_21012Info(makeInfo)[0]

    ]
}
nowTime = datetime.datetime.now()
tableauXMLStr = tableInfoMain.makeXMLByInfoMap(tableInfoMap)
fileName = "{}_{}_模板_{}.twb".format(makeInfo["gameCHName"], makeInfo["tableauReportName"], nowTime.strftime("%Y%m%d"))
outFilePath = 'file/{}/twb/{}'.format(makeInfo["gameName"],fileName)
shfileMake = open(outFilePath,"w", encoding="utf-8", newline="\n")
shfileMake.writelines(tableauXMLStr)
shfileMake.close()
hdfsPathNameInit = "/user/GTW_PD/Document/04_02_Tableau_Twb"
hdfsPathNameNew = hdfsPathNameInit + "/{}/00_new".format(makeInfo["gameName"])
hdfsPathNameDate = hdfsPathNameInit + "/{}/{}".format(makeInfo["gameName"],nowTime.strftime("%Y%m%d"))
hdfsCtrl.makeDirs(hdfsPathNameNew)
hdfsCtrl.makeDirs(hdfsPathNameDate)
hdfsCtrl.deleteDirs("{}/{}".format(hdfsPathNameNew,fileName))
hdfsCtrl.deleteDirs("{}/{}".format(hdfsPathNameDate,fileName))
hdfsCtrl.upload("{}/{}".format(hdfsPathNameNew,fileName),outFilePath)
hdfsCtrl.upload("{}/{}".format(hdfsPathNameDate,fileName),outFilePath)