from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic


class TableInfo_1800(TableInfoBasic):

    @classmethod
    def getBUReport1802Info(self, makeInfo):
        tableInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1802"
            , "tableNumber": "1802"
            , "dataName": "帳號IP登入相關資料"
            , "memo": "帳號IP登入相關資料"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1802".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1802"]
            , ["[:HashCode10Upper]", "0000BU1802"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON","hidden":"true", "datatype":"string","memo":"" }
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "hidden": "false", "datatype": "string", "memo": "平台名稱"}

        columnInfoMap["uniqueint_1"] = {"description": "該IP數字代碼", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "該IP登入次數", "hidden": "false", "datatype": "integer", "memo": ""}

        columnInfoMap["uniquestr_1"] = {"description": "該IP位置", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "國家代碼", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "國家全名", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "區域名", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_5"] = {"description": "城市名", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_6"] = {"description": "郵遞區號", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_7"] = {"description": "時區", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquedbl_1"] = {"description": "經度", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquedbl_2"] = {"description": "緯度", "hidden": "false", "datatype": "integer", "memo": ""}

        for columnInfoKey in columnInfoMap.keys():
            if columnInfoKey != "tableInfo":
                xmlStr = self.getBasicColumnInitXml()[columnInfoMap[columnInfoKey]["datatype"]]
                xmlStr = xmlStr.replace("[::Description]", columnInfoMap[columnInfoKey]["description"])
                xmlStr = xmlStr.replace("[::Hidden]", columnInfoMap[columnInfoKey]["hidden"])
                xmlStr = xmlStr.replace("[::DataName]", columnInfoKey)
                columnListMap["[::{}]".format(columnInfoKey)] = {"value": xmlStr}

        tableInfoMap["[::ColumnListXML]"] = columnListMap
        tableXMLStr = self.makeXMLByInfoMap(tableInfoMap)
        for xmlReplace in xmlReplaceArr:
            tableXMLStr = tableXMLStr.replace(xmlReplace[0], xmlReplace[1])
        return {"value": tableXMLStr}, columnInfoMap


    @classmethod
    def getBUReport1804Info(self, makeInfo):
        tableInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1804"
            , "tableNumber": "1804"
            , "dataName": "帳號IP登入資料"
            , "memo": "帳號IP登入資料"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1804".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1804"]
            , ["[:HashCode10Upper]", "0000BU1804"]
        ]
        columnListMap = {
            "value" : self.getColumnListXML()
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON","hidden":"true", "datatype":"string","memo":"" }
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "hidden": "false", "datatype": "string", "memo": "平台名稱"}

        columnInfoMap["uniqueint_1"] = {"description": "該IP數字代碼", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "該IP登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "該IP登入帳號數", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "該IP位置", "hidden": "false", "datatype": "string", "memo": ""}




        for columnInfoKey in columnInfoMap.keys():
            if columnInfoKey != "tableInfo":
                xmlStr = self.getBasicColumnInitXml()[columnInfoMap[columnInfoKey]["datatype"]]
                xmlStr = xmlStr.replace("[::Description]", columnInfoMap[columnInfoKey]["description"])
                xmlStr = xmlStr.replace("[::Hidden]", columnInfoMap[columnInfoKey]["hidden"])
                xmlStr = xmlStr.replace("[::DataName]", columnInfoKey)
                columnListMap["[::{}]".format(columnInfoKey)] = {"value": xmlStr}

        tableInfoMap["[::ColumnListXML]"] = columnListMap
        tableXMLStr = self.makeXMLByInfoMap(tableInfoMap)
        for xmlReplace in xmlReplaceArr:
            tableXMLStr = tableXMLStr.replace(xmlReplace[0], xmlReplace[1])
        return {"value": tableXMLStr}, columnInfoMap









