from info.maple.tableauinfo.TableauInfoMain import TableauInfoMain
import time

# 撈取相關
tableauInfoMain = TableauInfoMain()
tableauInfoMap = tableauInfoMain.getInitInfoMap()

gameInfoArr = [
    ["maple" ,"楓之谷" ,"maple_v","10.10.99.151","5432","bureport","gpadmin"]
    , ["lineage", "天堂" , "lineage_v","10.10.99.151","5432","bureport","gpadmin"]
    , ["tdn" ,"龍之谷" ,"tdn_v","10.10.99.151","5432","bureport","gpadmin"]
    , ["cso" ,"絕對武力" ,"cso_v","10.10.99.151","5432","bureport","gpadmin"]
    , ["mabi" ,"瑪奇" ,"mabi_v","10.10.99.151","5432","bureport","gpadmin"]
    , ["wod" ,"龍之谷新世界" ,"wod_v","10.10.99.151","5432","bureport","gpadmin"]
    , ["lineagem" ,"天堂M" ,"lineagem_v","10.100.71.224","5432","bureport",""]
]

for gameInfo in gameInfoArr :
    makeInfo = {
        "tableauReportName" : "01_Login數據指標"
        , "gameName" : gameInfo[0]
        , "gameCHName" : gameInfo[1]
        , "schemaName" : gameInfo[2]
        , "serverName": gameInfo[3]
        , "serverPort": gameInfo[4]
        , "dbName": gameInfo[5]
        , "userName": gameInfo[6]
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
        ]
    }




    tableauXMLStr = tableauInfoMain.makeXMLByInfoMap(tableauInfoMap)

    if "lineagem" == gameInfo[0]:
        tableauXMLStr = tableauXMLStr.replace("greenplum","postgres")

    shfileMake = open("file/{}/{}_{}_模板_{}.twb".format(
                                makeInfo["gameName"]
                                , makeInfo["gameCHName"]
                                , makeInfo["tableauReportName"]
                                , "0000000000"
                            ),"w", encoding="utf-8", newline="\n"
                        )
    shfileMake.writelines(tableauXMLStr)
    shfileMake.close()