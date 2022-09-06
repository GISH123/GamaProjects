from info.common.tableinfo.TableInfo_1100 import TableInfo_1100 as TableInfo_1100_Common


class TableInfo_1100(TableInfo_1100_Common) :

    @classmethod
    def getBUReport1101Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1101"
            , "tableNumber": "1101"
            , "dataName": "平台在線時間"
            , "memo": "平台當天的在線時間"
            , "dataType": "Login+"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1101".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1101"]
            , ["[:HashCode10Upper]", "0000BU1101"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "true", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": "遊戲帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "hidden": "false", "datatype": "string", "memo": "平台名稱"}
        columnInfoMap["uniquetime_1"] = {"description": "當天登入時間", "hidden": "false", "datatype": "datetime", "memo": ""}
        columnInfoMap["uniquetime_2"] = {"description": "當天登出時間", "hidden": "false", "datatype": "datetime", "memo": ""}

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
    def getBUReport1102Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()

        tableInfo = {
            "dataCode": "BU1102"
            , "tableNumber": "1102"
            , "dataName": "帳號當天在線時間"
            , "memo": "帳號當天的在線時間"
            , "dataType": "Login+"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1102".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1102"]
            , ["[:HashCode10Upper]", "0000BU1102"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "true", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": "遊戲帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "hidden": "false", "datatype": "string", "memo": "平台名稱"}
        columnInfoMap["uniquetime_1"] = {"description": "當天登入時間", "hidden": "false", "datatype": "datetime", "memo": ""}
        columnInfoMap["uniquetime_2"] = {"description": "當天登出時間", "hidden": "false", "datatype": "datetime", "memo": ""}

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
    def getBUReport1103Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1103"
            , "tableNumber": "1103"
            , "dataName": "角色當天在線時間"
            , "memo": "角色當天的在線時間"
            , "dataType": "Login+"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1103".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1103"]
            , ["[:HashCode10Upper]", "0000BU1103"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "hidden": "false", "datatype": "string", "memo": "平台名稱"}
        columnInfoMap["uniquetime_1"] = {"description": "當天登入時間", "hidden": "false", "datatype": "datetime", "memo": ""}
        columnInfoMap["uniquetime_2"] = {"description": "當天登出時間", "hidden": "false", "datatype": "datetime", "memo": ""}

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
    def getBUReport1131Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1131"
            , "tableNumber": "1131"
            , "dataName": "當時在線人數"
            , "memo": "當時在線人數"
            , "dataType": "Login+"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1131".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1131"]
            , ["[:HashCode10Upper]", "0000BU1131"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "在線人數", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquetime_1"] = {"description": "時間", "hidden": "false", "datatype": "datetime", "memo": ""}

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
    def getBUReport1132Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1132"
            , "tableNumber": "1132"
            , "dataName": "角色每日在線時數"
            , "memo": "角色的每日在線秒數_上半天"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1132".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1132"]
            , ["[:HashCode10Upper]", "0000BU1132"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "hidden": "false", "datatype": "string", "memo": "平台名稱"}
        columnInfoMap["uniqueint_1"] = {"description": "00點到01點(秒)", "hidden": "false", "datatype": "integer", "memo": "凌晨00點到凌晨01點(秒)"}
        columnInfoMap["uniqueint_2"] = {"description": "01點到02點(秒)", "hidden": "false", "datatype": "integer", "memo": "凌晨01點到凌晨02點(秒)"}
        columnInfoMap["uniqueint_3"] = {"description": "02點到03點(秒)", "hidden": "false", "datatype": "integer", "memo": "凌晨02點到凌晨03點(秒)"}
        columnInfoMap["uniqueint_4"] = {"description": "03點到04點(秒)", "hidden": "false", "datatype": "integer", "memo": "凌晨03點到凌晨04點(秒)"}
        columnInfoMap["uniqueint_5"] = {"description": "04點到05點(秒)", "hidden": "false", "datatype": "integer", "memo": "凌晨04點到凌晨05點(秒)"}
        columnInfoMap["uniqueint_6"] = {"description": "05點到06點(秒)", "hidden": "false", "datatype": "integer", "memo": "凌晨05點到凌晨06點(秒)"}
        columnInfoMap["uniqueint_7"] = {"description": "06點到07點(秒)", "hidden": "false", "datatype": "integer", "memo": "凌晨06點到早上07點(秒)"}
        columnInfoMap["uniqueint_8"] = {"description": "07點到08點(秒)", "hidden": "false", "datatype": "integer", "memo": "早上07點到早上08點(秒)"}
        columnInfoMap["uniqueint_9"] = {"description": "08點到09點(秒)", "hidden": "false", "datatype": "integer", "memo": "早上08點到早上09點(秒)"}
        columnInfoMap["uniqueint_10"] = {"description": "09點到10點(秒)", "hidden": "false", "datatype": "integer", "memo": "早上09點到早上10點(秒)"}
        columnInfoMap["uniqueint_11"] = {"description": "10點到11點(秒)", "hidden": "false", "datatype": "integer", "memo": "早上10點到早上11點(秒)"}
        columnInfoMap["uniqueint_12"] = {"description": "11點到12點(秒)", "hidden": "false", "datatype": "integer", "memo": "早上11點到中午12點(秒)"}

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
    def getBUReport1133Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1133"
            , "tableNumber": "1133"
            , "dataName": "角色每日在線時數"
            , "memo": "角色的每日在線秒數_下半天"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1133".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1133"]
            , ["[:HashCode10Upper]", "0000BU1133"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "hidden": "false", "datatype": "string", "memo": "平台名稱"}
        columnInfoMap["uniqueint_1"] = {"description": "12點到13點(秒)", "hidden": "false", "datatype": "integer", "memo": "中午12點到下午13點(秒)"}
        columnInfoMap["uniqueint_2"] = {"description": "13點到14點(秒)", "hidden": "false", "datatype": "integer", "memo": "下午13點到下午14點(秒)"}
        columnInfoMap["uniqueint_3"] = {"description": "14點到15點(秒)", "hidden": "false", "datatype": "integer", "memo": "下午14點到下午15點(秒)"}
        columnInfoMap["uniqueint_4"] = {"description": "15點到16點(秒)", "hidden": "false", "datatype": "integer", "memo": "下午15點到下午16點(秒)"}
        columnInfoMap["uniqueint_5"] = {"description": "16點到17點(秒)", "hidden": "false", "datatype": "integer", "memo": "下午16點到下午17點(秒)"}
        columnInfoMap["uniqueint_6"] = {"description": "17點到18點(秒)", "hidden": "false", "datatype": "integer", "memo": "下午17點到晚上18點(秒)"}
        columnInfoMap["uniqueint_7"] = {"description": "18點到19點(秒)", "hidden": "false", "datatype": "integer", "memo": "晚上18點到晚上19點(秒)"}
        columnInfoMap["uniqueint_8"] = {"description": "19點到20點(秒)", "hidden": "false", "datatype": "integer", "memo": "晚上19點到晚上20點(秒)"}
        columnInfoMap["uniqueint_9"] = {"description": "20點到21點(秒)", "hidden": "false", "datatype": "integer", "memo": "晚上20點到晚上21點(秒)"}
        columnInfoMap["uniqueint_10"] = {"description": "21點到22點(秒)", "hidden": "false", "datatype": "integer", "memo": "晚上21點到晚上22點(秒)"}
        columnInfoMap["uniqueint_11"] = {"description": "22點到23點(秒)", "hidden": "false", "datatype": "integer", "memo": "晚上22點到晚上23點(秒)"}
        columnInfoMap["uniqueint_12"] = {"description": "23點到24點(秒)", "hidden": "false", "datatype": "integer", "memo": "晚上23點到凌晨24點(秒)"}
        columnInfoMap["uniqueint_14"] = {"description": "今天登入次數(次)", "hidden": "false", "datatype": "integer", "memo": "今天登入次數(次)"}
        columnInfoMap["uniqueint_15"] = {"description": "今天在線時數(秒)", "hidden": "false", "datatype": "integer", "memo": "今天在線時數(秒)"}

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
    def getBUReport1134Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1134"
            , "tableNumber": "1134"
            , "dataName": "伺服器最大在線數"
            , "memo": "伺服器每小時最大在線數_上半天"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1134".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1134"]
            , ["[:HashCode10Upper]", "0000BU1134"]
        ]
        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        # columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "00點到01點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨00點到凌晨01點(人)"}
        columnInfoMap["uniqueint_2"] = {"description": "01點到02點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨01點到凌晨02點(人)"}
        columnInfoMap["uniqueint_3"] = {"description": "02點到03點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨02點到凌晨03點(人)"}
        columnInfoMap["uniqueint_4"] = {"description": "03點到04點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨03點到凌晨04點(人)"}
        columnInfoMap["uniqueint_5"] = {"description": "04點到05點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨04點到凌晨05點(人)"}
        columnInfoMap["uniqueint_6"] = {"description": "05點到06點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨05點到凌晨06點(人)"}
        columnInfoMap["uniqueint_7"] = {"description": "06點到07點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨06點到早上07點(人)"}
        columnInfoMap["uniqueint_8"] = {"description": "07點到08點(人)", "hidden": "false", "datatype": "integer", "memo": "早上07點到早上08點(人)"}
        columnInfoMap["uniqueint_9"] = {"description": "08點到09點(人)", "hidden": "false", "datatype": "integer", "memo": "早上08點到早上09點(人)"}
        columnInfoMap["uniqueint_10"] = {"description": "09點到10點(人)", "hidden": "false", "datatype": "integer", "memo": "早上09點到早上10點(人)"}
        columnInfoMap["uniqueint_11"] = {"description": "10點到11點(人)", "hidden": "false", "datatype": "integer", "memo": "早上10點到早上11點(人)"}
        columnInfoMap["uniqueint_12"] = {"description": "11點到12點(人)", "hidden": "false", "datatype": "integer", "memo": "早上11點到中午12點(人)"}

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
    def getBUReport1135Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1135"
            , "tableNumber": "1135"
            , "dataName": "伺服器最大在線數"
            , "memo": "伺服器每小時最大在線數_下半天"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1135".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1135"]
            , ["[:HashCode10Upper]", "0000BU1135"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        # columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "12點到13點(人)", "hidden": "false", "datatype": "integer", "memo": "中午12點到下午13點(人)"}
        columnInfoMap["uniqueint_2"] = {"description": "13點到14點(人)", "hidden": "false", "datatype": "integer", "memo": "下午13點到下午14點(人)"}
        columnInfoMap["uniqueint_3"] = {"description": "14點到15點(人)", "hidden": "false", "datatype": "integer", "memo": "下午14點到下午15點(人)"}
        columnInfoMap["uniqueint_4"] = {"description": "15點到16點(人)", "hidden": "false", "datatype": "integer", "memo": "下午15點到下午16點(人)"}
        columnInfoMap["uniqueint_5"] = {"description": "16點到17點(人)", "hidden": "false", "datatype": "integer", "memo": "下午16點到下午17點(人)"}
        columnInfoMap["uniqueint_6"] = {"description": "17點到18點(人)", "hidden": "false", "datatype": "integer", "memo": "下午17點到晚上18點(人)"}
        columnInfoMap["uniqueint_7"] = {"description": "18點到19點(人)", "hidden": "false", "datatype": "integer", "memo": "晚上18點到晚上19點(人)"}
        columnInfoMap["uniqueint_8"] = {"description": "19點到20點(人)", "hidden": "false", "datatype": "integer", "memo": "晚上19點到晚上20點(人)"}
        columnInfoMap["uniqueint_9"] = {"description": "20點到21點(人)", "hidden": "false", "datatype": "integer", "memo": "晚上20點到晚上21點(人)"}
        columnInfoMap["uniqueint_10"] = {"description": "21點到22點(人)", "hidden": "false", "datatype": "integer", "memo": "晚上21點到晚上22點(人)"}
        columnInfoMap["uniqueint_11"] = {"description": "22點到23點(人)", "hidden": "false", "datatype": "integer", "memo": "晚上22點到晚上23點(人)"}
        columnInfoMap["uniqueint_12"] = {"description": "23點到24點(人)", "hidden": "false", "datatype": "integer", "memo": "晚上23點到凌晨24點(人)"}

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
    def getBUReport1136Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1136"
            , "tableNumber": "1136"
            , "dataName": "伺服器平均在線數"
            , "memo": "伺服器每小時平均在線數_上半天"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1136".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1136"]
            , ["[:HashCode10Upper]", "0000BU1136"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        # columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "00點到01點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨00點到凌晨01點(人)"}
        columnInfoMap["uniqueint_2"] = {"description": "01點到02點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨01點到凌晨02點(人)"}
        columnInfoMap["uniqueint_3"] = {"description": "02點到03點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨02點到凌晨03點(人)"}
        columnInfoMap["uniqueint_4"] = {"description": "03點到04點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨03點到凌晨04點(人)"}
        columnInfoMap["uniqueint_5"] = {"description": "04點到05點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨04點到凌晨05點(人)"}
        columnInfoMap["uniqueint_6"] = {"description": "05點到06點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨05點到凌晨06點(人)"}
        columnInfoMap["uniqueint_7"] = {"description": "06點到07點(人)", "hidden": "false", "datatype": "integer", "memo": "凌晨06點到早上07點(人)"}
        columnInfoMap["uniqueint_8"] = {"description": "07點到08點(人)", "hidden": "false", "datatype": "integer", "memo": "早上07點到早上08點(人)"}
        columnInfoMap["uniqueint_9"] = {"description": "08點到09點(人)", "hidden": "false", "datatype": "integer", "memo": "早上08點到早上09點(人)"}
        columnInfoMap["uniqueint_10"] = {"description": "09點到10點(人)", "hidden": "false", "datatype": "integer", "memo": "早上09點到早上10點(人)"}
        columnInfoMap["uniqueint_11"] = {"description": "10點到11點(人)", "hidden": "false", "datatype": "integer", "memo": "早上10點到早上11點(人)"}
        columnInfoMap["uniqueint_12"] = {"description": "11點到12點(人)", "hidden": "false", "datatype": "integer", "memo": "早上11點到中午12點(人)"}

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
    def getBUReport1137Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1137"
            , "tableNumber": "1137"
            , "dataName": "伺服器平均在線數"
            , "memo": "伺服器每小時平均在線數_下半天"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1137".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1137"]
            , ["[:HashCode10Upper]", "0000BU1137"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        # columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "12點到13點(人)", "hidden": "false", "datatype": "integer", "memo": "中午12點到下午13點(人)"}
        columnInfoMap["uniqueint_2"] = {"description": "13點到14點(人)", "hidden": "false", "datatype": "integer", "memo": "下午13點到下午14點(人)"}
        columnInfoMap["uniqueint_3"] = {"description": "14點到15點(人)", "hidden": "false", "datatype": "integer", "memo": "下午14點到下午15點(人)"}
        columnInfoMap["uniqueint_4"] = {"description": "15點到16點(人)", "hidden": "false", "datatype": "integer", "memo": "下午15點到下午16點(人)"}
        columnInfoMap["uniqueint_5"] = {"description": "16點到17點(人)", "hidden": "false", "datatype": "integer", "memo": "下午16點到下午17點(人)"}
        columnInfoMap["uniqueint_6"] = {"description": "17點到18點(人)", "hidden": "false", "datatype": "integer", "memo": "下午17點到晚上18點(人)"}
        columnInfoMap["uniqueint_7"] = {"description": "18點到19點(人)", "hidden": "false", "datatype": "integer", "memo": "晚上18點到晚上19點(人)"}
        columnInfoMap["uniqueint_8"] = {"description": "19點到20點(人)", "hidden": "false", "datatype": "integer", "memo": "晚上19點到晚上20點(人)"}
        columnInfoMap["uniqueint_9"] = {"description": "20點到21點(人)", "hidden": "false", "datatype": "integer", "memo": "晚上20點到晚上21點(人)"}
        columnInfoMap["uniqueint_10"] = {"description": "21點到22點(人)", "hidden": "false", "datatype": "integer", "memo": "晚上21點到晚上22點(人)"}
        columnInfoMap["uniqueint_11"] = {"description": "22點到23點(人)", "hidden": "false", "datatype": "integer", "memo": "晚上22點到晚上23點(人)"}
        columnInfoMap["uniqueint_12"] = {"description": "23點到24點(人)", "hidden": "false", "datatype": "integer", "memo": "晚上23點到凌晨24點(人)"}

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
