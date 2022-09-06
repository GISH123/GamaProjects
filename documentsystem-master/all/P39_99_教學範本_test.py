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
        "tableauReportName" : "99_教學範本"
        , "gameName" : gameInfo[0]
        , "gameCHName" : gameInfo[1]
        , "schemaName" : gameInfo[2]
        , "serverName": "10.10.99.151"
        , "serverPort": "5432"
        , "dbName": "bureport"
        , "userName": ""
    }

    date =  [
        tableauInfoMain.getBUReport1001Info(makeInfo)[0]
        , tableauInfoMain.getBUReport1002Info(makeInfo)[0]
        , tableauInfoMain.getBUReport1003Info(makeInfo)[0]
    ]

    tableauXMLStr = tableauInfoMain.makeXMLByInfoMap(tableauInfoMap)
    shfileMake = open("file/{}/{}_{}_01_test_{}.twb".format(
                                makeInfo["gameName"]
                                , makeInfo["gameCHName"]
                                , makeInfo["tableauReportName"]
                                , "0000000000"
                            ),"w", encoding="utf-8", newline="\n"
                        )
    shfileMake.writelines(tableauXMLStr)
    shfileMake.close()




