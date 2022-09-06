from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic


class TableInfo_1000(TableInfoBasic):

    @classmethod
    def getBUReport1001Info(self,makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo= {
            "dataCode": "BU1001"
            , "tableNumber": "1001"
            , "dataName" : "平台在線帳號"
            , "memo": "當天在線平台帳號"
            , "dataType": "Login+"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]","{} {} {}".format(tableInfo["dataCode"],makeInfo["gameCHName"],tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1001".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1001"]
            , ["[:HashCode10Upper]", "0000BU1001"]
        ]

        columnListMap = {
            "value" : self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料日期"}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "true", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": "服務帳號(遊戲帳號)"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": "遊戲帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "hidden": "false", "datatype": "string", "memo": "平台名稱"}
        columnInfoMap["uniqueint_1"] = {"description": "當天登入次數", "hidden": "false", "datatype": "integer", "memo": "當天實際登入次數"}
        columnInfoMap["uniqueint_2"] = {"description": "當天登出次數", "hidden": "false", "datatype": "integer", "memo": "當天實際登出次數"}
        columnInfoMap["uniquetime_1"] = {"description": "服務帳號創立時間", "hidden": "false", "datatype": "datetime", "memo": "服務帳號(遊戲帳號)的創立時間"}

        for columnInfoKey in columnInfoMap.keys() :
            if columnInfoKey != "tableInfo":
                xmlStr = self.getBasicColumnInitXml()[columnInfoMap[columnInfoKey]["datatype"]]
                xmlStr = xmlStr.replace("[::Description]",columnInfoMap[columnInfoKey]["description"])
                xmlStr = xmlStr.replace("[::Hidden]", columnInfoMap[columnInfoKey]["hidden"])
                xmlStr = xmlStr.replace("[::DataName]", columnInfoKey)
                columnListMap["[::{}]".format(columnInfoKey)] = { "value" : xmlStr}

        tableauInfoMap["[::ColumnListXML]"] = columnListMap
        tableauXMLStr = self.makeXMLByInfoMap(tableauInfoMap)
        for xmlReplace in xmlReplaceArr:
            tableauXMLStr = tableauXMLStr.replace(xmlReplace[0], xmlReplace[1])
        return {"value": tableauXMLStr} , columnInfoMap

    @classmethod
    def getBUReport1002Info(self,makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1002"
            , "tableNumber": "1002"
            , "dataName": "在線遊戲帳號資料"
            , "memo": "當天在線帳號資料"
            , "dataType": "Login+"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1002".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1002"]
            , ["[:HashCode10Upper]", "0000BU1002"]
        ]
        columnListMap = {
            "value" : self.getColumnListXML()
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料日期"}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "true", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": "服務帳號(遊戲帳號)"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": "遊戲帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "hidden": "false", "datatype": "string", "memo": "平台名稱"}
        columnInfoMap["uniqueint_1"] = {"description": "當天登入次數", "hidden": "false", "datatype": "integer", "memo": "當天登入次數"}
        columnInfoMap["uniqueint_2"] = {"description": "當天登出次數", "hidden": "false", "datatype": "integer", "memo": "當天登出次數"}
        columnInfoMap["uniquetime_1"] = {"description": "帳號創立時間", "hidden": "false", "datatype": "datetime", "memo": "帳號創立時間"}

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
        return {"value": tableauXMLStr} , columnInfoMap

    @classmethod
    def getBUReport1003Info(self,makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1003"
            , "tableNumber": "1003"
            , "dataName": "遊戲在線角色資料"
            , "memo": "當天遊戲在線角色"
            , "dataType": "Login+"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1003".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1003"]
            , ["[:HashCode10Upper]", "0000BU1003"]
        ]
        columnListMap = {
            "value" : self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料日期"}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": " 伺服器代碼"}
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": "服務帳號(遊戲帳號)"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": "遊戲帳號ID"}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": "遊戲角色ID"}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": "遊戲角色名稱"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "hidden": "false", "datatype": "string", "memo": "平台名稱"}
        columnInfoMap["uniqueint_1"] = {"description": "當天登入次數", "hidden": "false", "datatype": "integer", "memo": "當天登入次數"}
        columnInfoMap["uniqueint_2"] = {"description": "當天登出次數", "hidden": "false", "datatype": "integer", "memo": "當天登出次數"}
        columnInfoMap["uniquetime_1"] = {"description": "角色創立時間", "hidden": "false", "datatype": "datetime", "memo": "角色創立時間"}

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
        return {"value": tableauXMLStr} , columnInfoMap

