from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic


class TableInfo_5100(TableInfoBasic):
    def __init__(self):
        pass

    @classmethod
    def getBUReport5101Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo= {
            "dataCode": "BU5101"
            , "tableNumber": "5101"
            , "dataName" : "公會統計"
            , "memo": "公會統計"
            , "dataType": "AllData"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]","{} {} {}".format(tableInfo["dataCode"],makeInfo["gameCHName"],tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu5101".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu5101"]
            , ["[:HashCode10Upper]", "0000BU5101"]
        ]
        columnListMap = {
            "value": self.getColumnListXML()
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_8"] = {"description": "公會ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "公會名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "guildmembercount", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "guildmemberlogincount", "hidden": "false", "datatype": "integer", "memo": ""}


        for columnInfoKey in columnInfoMap.keys():
            if columnInfoKey != "tableInfo":
                xmlStr = self.getBasicColumnInitXml()[columnInfoMap[columnInfoKey]["datatype"]]
                xmlStr = xmlStr.replace("[::Description]", columnInfoMap[columnInfoKey]["description"])
                xmlStr = xmlStr.replace("[::Hidden]", columnInfoMap[columnInfoKey]["hidden"])
                xmlStr = xmlStr.replace("[::DataName]", columnInfoKey)
                columnListMap["[::{}]".format(columnInfoKey)] = {"value": xmlStr}

        tableauInfoMap["[::ColumnListXML]"] = columnListMap
        tableauXMLStr = self.makeXMLByInfoMap(tableauInfoMap)
        for xmlReplace in xmlReplaceArr:
            tableauXMLStr = tableauXMLStr.replace(xmlReplace[0], xmlReplace[1])
        return {"value": tableauXMLStr}, columnInfoMap