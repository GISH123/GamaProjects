from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic


class TableInfo_6600(TableInfoBasic):
    def __init__(self):
        pass

    @classmethod
    def getBUReport6609Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo= {
            "dataCode": "BU6609"
            , "tableNumber": "6609"
            , "dataName" : "一對一交易"
            , "memo": "一對一交易"
            , "dataType": "AllData"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]","{} {} {}".format(tableInfo["dataCode"],makeInfo["gameCHName"],tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu6609".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu6609"]
            , ["[:HashCode10Upper]", "0000BU6609"]
        ]
        columnListMap = {
            "value": self.getColumnListXML()
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "取得服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "取得遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "取得遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "取得遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "道具ID", "datatype": "string", "hidden": "false"}
        columnInfoMap["commondata_9"] = {"description": "道具名稱", "datatype": "string", "hidden": "false", "memo": "無道具名稱則用ID顯示"}
        columnInfoMap["uniqueint_2"] = {"description": "交易數量", "datatype": "integer", "hidden": "false"}
        # columnInfoMap["uniquestr_1"] = {"description": "給予服務帳號(遊戲帳號)", "datatype": "string", "hidden": "true"}
        #columnInfoMap["uniquestr_2"] = {"description": "給予遊戲帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_3"] = {"description": "給予遊戲角色ID", "datatype": "string", "hidden": "true"}
        #columnInfoMap["uniquestr_4"] = {"description": "給予遊戲角色名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquetime_1"] = {"description": "交易時間", "hidden": "false", "datatype": "integer", "memo": ""}
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