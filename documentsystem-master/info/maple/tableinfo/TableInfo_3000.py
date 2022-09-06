from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic


class TableInfo_3000(TableInfoBasic):
    def __init__(self):
        pass


    @classmethod
    def getBUReport3001Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo= {
            "dataCode": "BU3001"
            , "tableNumber": "3001"
            , "dataName" : "裝備統計(起)"
            , "memo": "裝備統計(起)"
            , "dataType": "AllData"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]","{} {} {}".format(tableInfo["dataCode"],makeInfo["gameCHName"],tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu3001".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu3001"]
            , ["[:HashCode10Upper]", "0000BU3001"]
        ]
        columnListMap = {
            "value": self.getColumnListXML()
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "總計裝備數量", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "eqp裝備數量", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "opt裝備數量", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_4"] = {"description": "現金eqp裝備數量", "hidden": "false", "datatype": "integer", "memo": ""}

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


    @classmethod
    def getBUReport3002Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo= {
            "dataCode": "BU3002"
            , "tableNumber": "3002"
            , "dataName" : "裝備統計(迄)"
            , "memo": "裝備統計(迄)"
            , "dataType": "AllData"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]","{} {} {}".format(tableInfo["dataCode"],makeInfo["gameCHName"],tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu3002".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu3002"]
            , ["[:HashCode10Upper]", "0000BU3002"]
        ]
        columnListMap = {
            "value": self.getColumnListXML()
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "總計裝備數量", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "eqp裝備數量", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "opt裝備數量", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_4"] = {"description": "現金eqp裝備數量", "hidden": "false", "datatype": "integer", "memo": ""}

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

    @classmethod
    def getBUReport3011Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME3011"
            , "tableNumber": "3011"
            , "dataName": "裝備狀況(起)"
            , "memo": "裝備狀況(起)"
            , "dataType": "ModelExtract"
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_8"] = {"description": "道具ID", "datatype": "string", "hidden": "false", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "道具名稱", "datatype": "string", "hidden": "false", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "裝備類別", "datatype": "string", "hidden": "false", "memo": "裝備類別一"}
        #columnInfoMap["uniquestr_2"] = {"description": "uniquestr_2", "datatype": "string", "hidden": "false", "memo": "裝備類別二"}
        columnInfoMap["uniquestr_3"] = {"description": "是否為現金道具", "datatype": "string", "hidden": "false", "memo": "裝備類別三"}
        columnInfoMap["uniquestr_4"] = {"description": "裝備欄位", "datatype": "string", "hidden": "false", "memo": "裝備欄位一"}
        #columnInfoMap["uniquestr_5"] = {"description": "uniquestr_5", "datatype": "string", "hidden": "false", "memo": "裝備欄位二"}
        #columnInfoMap["uniquestr_6"] = {"description": "uniquestr_6", "datatype": "string", "hidden": "false", "memo": "裝備欄位三"}

        return {"value": ""}, columnInfoMap

    @classmethod
    def getBUReport3012Info(self, makeInfo):

        tableInfo = {
            "dataCode": "ME3012"
            , "tableNumber": "3012"
            , "dataName": "裝備狀況(迄)"
            , "memo": "裝備狀況(迄)"
            , "dataType": "ModelExtract"
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_8"] = {"description": "道具ID", "datatype": "string", "hidden": "false", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "道具名稱", "datatype": "string", "hidden": "false", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "裝備類別", "datatype": "string", "hidden": "false", "memo": "裝備類別一"}
        # columnInfoMap["uniquestr_2"] = {"description": "uniquestr_2", "datatype": "string", "hidden": "false", "memo": "裝備類別二"}
        columnInfoMap["uniquestr_3"] = {"description": "是否為現金道具", "datatype": "string", "hidden": "false", "memo": "裝備類別三"}
        columnInfoMap["uniquestr_4"] = {"description": "裝備欄位", "datatype": "string", "hidden": "false", "memo": "裝備欄位一"}
        # columnInfoMap["uniquestr_5"] = {"description": "uniquestr_5", "datatype": "string", "hidden": "false", "memo": "裝備欄位二"}
        # columnInfoMap["uniquestr_6"] = {"description": "uniquestr_6", "datatype": "string", "hidden": "false", "memo": "裝備欄位三"}

        return {"value": ""}, columnInfoMap