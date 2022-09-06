from info.maple.tableauinfo.TableauInfoMain import TableauInfoMain
import time

# 撈取相關
tableauInfoMain = TableauInfoMain()
tableauInfoMap = tableauInfoMain.getInitInfoMap()

gameInfoArr = [
    ["maple" ,"楓之谷" ,"maple_v"]
]

for gameInfo in gameInfoArr :
    makeInfo = {
        "tableauReportName" : "01_Login數據指標"
        , "gameName" : gameInfo[0]
        , "gameCHName" : gameInfo[1]
        , "schemaName" : gameInfo[2]
        , "serverName": "10.10.99.151"
        , "serverPort": "5432"
        , "dbName": "bureport"
        , "userName": "gpadmin"
    }

    tableauInfoMap["[::DataSourceListXML]"] = {
        "value": [
            tableauInfoMain.getBUReportStatisticsInfo(makeInfo)
            , tableauInfoMain.getBUReport1001Info(makeInfo)
            , tableauInfoMain.getBUReport1002Info(makeInfo)
            , tableauInfoMain.getBUReport1003Info(makeInfo)
            , tableauInfoMain.getBUReport1102Info(makeInfo)
            , tableauInfoMain.getBUReport1103Info(makeInfo)
            , tableauInfoMain.getBUReport1131Info(makeInfo)
            , tableauInfoMain.getBUReport1132Info(makeInfo)
            , tableauInfoMain.getBUReport1133Info(makeInfo)
            , tableauInfoMain.getBUReport1134Info(makeInfo)
            , tableauInfoMain.getBUReport1135Info(makeInfo)
            , tableauInfoMain.getBUReport1136Info(makeInfo)
            , tableauInfoMain.getBUReport1137Info(makeInfo)
            , tableauInfoMain.getBUReport1804Info(makeInfo)
            , tableauInfoMain.getBUReport6001Info(makeInfo)
            , tableauInfoMain.getBUReport6002Info(makeInfo)
            , tableauInfoMain.getBUReport6011Info(makeInfo)
            , tableauInfoMain.getBUReport6012Info(makeInfo)
            , tableauInfoMain.getBUReport6019Info(makeInfo)

            , tableauInfoMain.getBUReport2001Info(makeInfo)
            , tableauInfoMain.getBUReport2002Info(makeInfo)
            , tableauInfoMain.getBUReport2101Info(makeInfo)
            , tableauInfoMain.getBUReport2102Info(makeInfo)
            , tableauInfoMain.getBUReport2103Info(makeInfo)
            , tableauInfoMain.getBUReport2111Info(makeInfo)
            , tableauInfoMain.getBUReport2112Info(makeInfo)
            , tableauInfoMain.getBUReport2113Info(makeInfo)
            , tableauInfoMain.getBUReport6609Info(makeInfo)
            , tableauInfoMain.getBUReport6509Info(makeInfo)
            , tableauInfoMain.getBUReport3001Info(makeInfo)
            , tableauInfoMain.getBUReport3002Info(makeInfo)
            , tableauInfoMain.getBUReport5001Info(makeInfo)
            , tableauInfoMain.getBUReport5101Info(makeInfo)
        ]
    }

    tableauXMLStr = tableauInfoMain.makeXMLByInfoMap(tableauInfoMap)
    shfileMake = open("file/{}/{}_{}_模板_{}.twb".format(
                                makeInfo["gameName"]
                                , makeInfo["gameCHName"]
                                , makeInfo["tableauReportName"]
                                , "0000000000"
                            ),"w", encoding="utf-8", newline="\n"
                        )
    shfileMake.writelines(tableauXMLStr)
    shfileMake.close()
