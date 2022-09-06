from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic


class TableInfo_6000(TableInfoBasic):

    @classmethod
    def getBUReport6001Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU6001"
            , "tableNumber": "6001"
            , "dataName": "平台儲值統計"
            , "memo": "平台儲值統計"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu6001".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu6001"]
            , ["[:HashCode10Upper]", "0000BU6001"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON","hidden":"true", "datatype":"string","memo":"" }
        columnInfoMap["commondata_1"] = {"description": "帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_2"] = {"description": "帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_8"] = {"description": "商品ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_9"] = {"description": "商品名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_10"] = {"description": "平台", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniqueint_1"] = {"description": "(保留位)", "datatype": "integer", "hidden": "true", "memo": "道具總價保留位"}
        columnInfoMap["uniqueint_2"] = {"description": "消費次數", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_3"] = {"description": "(保留位)", "datatype": "integer", "hidden": "true", "memo": "道具單價保留位"}
        columnInfoMap["uniquestr_1"] = {"description": "貨幣類別", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_2"] = {"description": "付費平台", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquedbl_1"] = {"description": "道具總價", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_2"] = {"description": "道具單價", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquetime_1"] = {"description": "消費時間", "datatype": "datetime", "hidden": "true"}

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
    def getBUReport6002Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU6002"
            , "tableNumber": "6002"
            , "dataName": "平台扣點統計"
            , "memo": "平台扣點統計"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu6002".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu6002"]
            , ["[:HashCode10Upper]", "0000BU6002"]
        ]
        columnListMap = {
            "value": self.getColumnListXML()
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON","hidden":"true", "datatype":"string","memo":"" }

        columnInfoMap["commondata_1"] = {"description": "帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_2"] = {"description": "帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_8"] = {"description": "商品ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_9"] = {"description": "商品名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_10"] = {"description": "平台", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniqueint_1"] = {"description": "(保留位)", "datatype": "integer", "hidden": "true", "memo": "道具總價保留位"}
        columnInfoMap["uniqueint_2"] = {"description": "道具數量", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_3"] = {"description": "(保留位)", "datatype": "integer", "hidden": "true", "memo": "道具單價保留位"}
        columnInfoMap["uniquestr_1"] = {"description": "貨幣類別", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquedbl_1"] = {"description": "貨幣消耗", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_2"] = {"description": "道具單價", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquetime_1"] = {"description": "消費時間", "datatype": "datetime", "hidden": "true"}

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
    def getBUReport6011Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU6011"
            , "tableNumber": "6011"
            , "dataName": "消費道具統計(by角色)"
            , "memo": "消費道具統計(by角色)"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu6011".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu6011"]
            , ["[:HashCode10Upper]", "0000BU6011"]
        ]
        columnListMap = {
            "value" : self.getColumnListXML()
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_1"] = {"description": "帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_2"] = {"description": "帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_8"] = {"description": "商品ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_9"] = {"description": "商品名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_10"] = {"description": "平台", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniqueint_1"] = {"description": "(保留位)", "datatype": "integer", "hidden": "true", "memo": "道具總價保留位"}
        columnInfoMap["uniqueint_2"] = {"description": "道具數量", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_3"] = {"description": "(保留位)", "datatype": "integer", "hidden": "true", "memo": "道具單價保留位"}
        columnInfoMap["uniquestr_1"] = {"description": "貨幣類別", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquedbl_1"] = {"description": "貨幣消耗", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_2"] = {"description": "道具單價", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquetime_1"] = {"description": "消費時間", "datatype": "datetime", "hidden": "true"}

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
    def getBUReport6012Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU6012"
            , "tableNumber": "6012"
            , "dataName": "消費道具統計(by道具)"
            , "memo": "消費道具統計(by道具)"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu6012".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu6012"]
            , ["[:HashCode10Upper]", "0000BU6012"]
        ]
        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo

        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "道具ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "道具名稱", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniqueint_1"] = {"description": "(保留位)", "hidden": "true", "datatype": "integer", "memo": "消費金額保留位"}
        columnInfoMap["uniqueint_2"] = {"description": "消費次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "(保留位)", "hidden": "true", "datatype": "integer", "memo": "道具單價保留位"}
        columnInfoMap["uniquestr_1"] = {"description": "計費方式", "hidden": "false", "datatype": "string", "memo": "可能有台灣香港 請看6009"}
        columnInfoMap["uniquedbl_1"] = {"description": "消費金額", "datatype": "real", "hidden": "false"}
        columnInfoMap["uniquedbl_2"] = {"description": "道具單價", "datatype": "real", "hidden": "false"}

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
    def getBUReport6019Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU6019"
            , "tableNumber": "6019"
            , "dataName": "道具消費細項"
            , "memo": "道具消費細項"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu6019".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu6019"]
            , ["[:HashCode10Upper]", "0000BU6019"]
        ]
        columnListMap = {
            "value": self.getColumnListXML()
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo

        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_1"] = {"description": "帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_2"] = {"description": "帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_8"] = {"description": "商品ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_9"] = {"description": "商品名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_10"] = {"description": "平台", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniqueint_1"] = {"description": "(保留位)", "datatype": "integer", "hidden": "true", "memo": "道具總價保留位"}
        columnInfoMap["uniqueint_2"] = {"description": "道具數量", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_3"] = {"description": "(保留位)", "datatype": "integer", "hidden": "true", "memo": "道具單價保留位"}
        columnInfoMap["uniquestr_1"] = {"description": "貨幣類別", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquedbl_1"] = {"description": "貨幣消耗", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_2"] = {"description": "道具單價", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquetime_1"] = {"description": "消費時間", "datatype": "datetime", "hidden": "true"}

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







