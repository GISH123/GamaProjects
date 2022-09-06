from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic


class TableInfo_20000(TableInfoBasic):

    # PCA
    @classmethod
    def getBUReport21001Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "MD21001"
            , "tableNumber": "21001"
            , "dataName": "TAG_登入時段"
            , "memo": "各帳號各月，登入時段之分布型態"
            , "dataType": "model_tag"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu21001".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000md21001"]
            , ["[:HashCode10Upper]", "0000MD21001"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料起始日期，資料均以一個月為處理單位"}
        columnInfoMap["game"] = {"description": "新楓之谷", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "true", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "Service Account"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲帳號(全數字)"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}
        columnInfoMap["commondata_9"] = {"description": "建模年月", "hidden": "false", "datatype": "string", "memo": "建模年月(YYYYMM)"}

        columnInfoMap["uniquestr_1"] = {"description": "積極登入型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_2"] = {"description": "日間型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_3"] = {"description": "平日型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_12"] = {"description": "晚間型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_13"] = {"description": "假日型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}

        columnInfoMap["uniquedbl_1"] = {"description": "積極登入(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=越積極登入"}
        columnInfoMap["uniquedbl_2"] = {"description": "日間型(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=越常在日間登入、越小=越常在晚間登入"}
        columnInfoMap["uniquedbl_3"] = {"description": "平日型(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=越常在平日登入、越小=越常在假日登入"}

        columnInfoMap["uniquedbl_11"] = {"description": "積極登入程度(原始值)", "hidden": "false", "datatype": "real", "memo": "(原始值), 越大=越積極登入"}
        columnInfoMap["uniquedbl_12"] = {"description": "日間型程度(原始值)", "hidden": "false", "datatype": "real", "memo": "(原始值), 越大=越常在日間登入、越小=越常在晚間登入"}
        columnInfoMap["uniquedbl_13"] = {"description": "平日型程度(原始值)", "hidden": "false", "datatype": "real", "memo": "(原始值), 越大=越常在平日登入、越小=越常在假日登入"}

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
    def getBUReport22001Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "MD22001"
            , "tableNumber": "22001"
            , "dataName": "TAG_經驗成長"
            , "memo": "各帳號各月，累計經驗成長"
            , "dataType": "model_tag"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu22001".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000md22001"]
            , ["[:HashCode10Upper]", "0000MD22001"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料起始日期，資料均以一個月為處理單位"}
        columnInfoMap["game"] = {"description": "新楓之谷", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "true", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "Service Account"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲帳號(全數字)"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}
        columnInfoMap["commondata_9"] = {"description": "建模年月", "hidden": "false", "datatype": "string", "memo": "建模年月(YYYYMM)"}

        columnInfoMap["uniquestr_1"] = {"description": "練功狂", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        #columnInfoMap["uniquestr_2"] = {"description": "練功增加型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}

        columnInfoMap["uniquestr_11"] = {"description": "不重視練功", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        #columnInfoMap["uniquestr_2"] = {"description": "練功減少型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}

        columnInfoMap["uniquedbl_1"] = {"description": "積極練功(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=越積極練功"}
        #columnInfoMap["uniquedbl_2"] = {"description": "練功增加型(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=月初練功較多、越小=月底練功較多"}

        columnInfoMap["uniquedbl_11"] = {"description": "積極練功程度(原始值)", "hidden": "false", "datatype": "real", "memo": "(原始值), 越大=越積極練功"}
        #columnInfoMap["uniquedbl_12"] = {"description": "練功增加型程度(原始值)", "hidden": "false", "datatype": "real", "memo": "(原始值),  越大=月初練功較多、越小=月底練功較多"}

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
    def getBUReport22002Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "MD22002"
            , "tableNumber": "22002"
            , "dataName": "TAG_戰鬥指標"
            , "memo": "各角色各月，戰鬥指數"
            , "dataType": "model_tag"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu22002".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000md22002"]
            , ["[:HashCode10Upper]", "0000MD22002"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料起始日期，資料均以一個月為處理單位"}
        columnInfoMap["game"] = {"description": "新楓之谷", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "Service Account"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲帳號(全數字)"}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色ID(全數字)"}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色名稱"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}
        columnInfoMap["commondata_9"] = {"description": "建模年月", "hidden": "false", "datatype": "string", "memo": "建模年月(YYYYMM)"}

        columnInfoMap["uniquestr_1"] = {"description": "高戰鬥指標", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquedbl_1"] = {"description": "戰力值(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=戰力越高"}
        columnInfoMap["uniquedbl_11"] = {"description": "戰力值(原始值)", "hidden": "false", "datatype": "real", "memo": "(原始值), 越大=戰力越高"}

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
    def getBUReport27001Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "MD27001"
            , "tableNumber": "27001"
            , "dataName": "TAG_聯盟戰地"
            , "memo": "各角色各月，聯盟戰地參與標籤"
            , "dataType": "model_tag"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu27001".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000md27001"]
            , ["[:HashCode10Upper]", "0000MD27001"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料起始日期，資料均以一個月為處理單位"}
        columnInfoMap["game"] = {"description": "新楓之谷", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "Service Account"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲帳號(全數字)"}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色ID(全數字)"}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色名稱"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}
        columnInfoMap["commondata_9"] = {"description": "建模年月", "hidden": "false", "datatype": "string", "memo": "建模年月(YYYYMM)"}

        columnInfoMap["uniquestr_1"] = {"description": "封頂型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_2"] = {"description": "成長型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_11"] = {"description": "新手型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_12"] = {"description": "停滯型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}

        columnInfoMap["uniquedbl_1"] = {"description": "封頂程度(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=封頂程度高"}
        columnInfoMap["uniquedbl_2"] = {"description": "成長程度(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=成長程度高"}
        columnInfoMap["uniquedbl_11"] = {"description": "封頂型值(原始值)", "hidden": "false", "datatype": "real", "memo": "(原始值), 越大=封頂程度高"}
        columnInfoMap["uniquedbl_12"] = {"description": "成長型(原始值)", "hidden": "false", "datatype": "real", "memo": "(原始值), 越大=成長程度高"}
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
    def getBUReport27002Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "MD27002"
            , "tableNumber": "27002"
            , "dataName": "TAG_武陵道館"
            , "memo": "各角色各最近12週，武陵道館參與標籤"
            , "dataType": "model_tag"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu27002".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000md27002"]
            , ["[:HashCode10Upper]", "0000MD27002"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料起始日期，資料均以一個月為處理單位"}
        columnInfoMap["game"] = {"description": "新楓之谷", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "Service Account"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲帳號(全數字)"}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色ID(全數字)"}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色名稱"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}


        columnInfoMap["uniquestr_1"] = {"description": "排名頂尖型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_2"] = {"description": "排名下降型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_12"] = {"description": "排名上升型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}

        columnInfoMap["uniquedbl_1"] = {"description": "排名頂尖程度(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=排名頂尖程度越高"}
        columnInfoMap["uniquedbl_2"] = {"description": "排名下降程度(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=排名下降越明顯"}
        columnInfoMap["uniquedbl_11"] = {"description": "排名頂尖(原始值)", "hidden": "false", "datatype": "real", "memo": "(原始值), 越大=排名頂尖程度越高"}
        columnInfoMap["uniquedbl_12"] = {"description": "排名下降(原始值)", "hidden": "false", "datatype": "real", "memo": "(原始值), 越大=排名下降越明顯"}
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

    # LDA
    @classmethod
    def getBUReport24001Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "MD24001"
            , "tableNumber": "24001"
            , "dataName": "TAG_任務型態"
            , "memo": "各角色各月，任務型態"
            , "dataType": "model_tag"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu24001".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000md24001"]
            , ["[:HashCode10Upper]", "0000MD24001"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料起始日期，資料均以一個月為處理單位"}
        columnInfoMap["game"] = {"description": "新楓之谷", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "Service Account"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲帳號(全數字)"}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色ID(全數字)"}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色名稱"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}
        columnInfoMap["commondata_9"] = {"description": "建模年月", "hidden": "false", "datatype": "string", "memo": "建模年月(YYYYMM)"}

        columnInfoMap["uniquestr_1"] = {"description": "HappyDay類", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_2"] = {"description": "每日型_低等級", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_3"] = {"description": "里程型_低等級(簡易)", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_4"] = {"description": "里程型_高等級", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_5"] = {"description": "每日型_高等級", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_6"] = {"description": "聯盟戰地類", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}
        columnInfoMap["uniquestr_7"] = {"description": "里程型_低等級(繁瑣)", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是"}


        columnInfoMap["uniquedbl_1"] = {"description": "HappyDay類(佔比)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=該類任務佔比越高"}
        columnInfoMap["uniquedbl_2"] = {"description": "每日型_低等級(佔比)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=該類任務佔比越高"}
        columnInfoMap["uniquedbl_3"] = {"description": "里程型_低等級(簡易)(佔比)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=該類任務佔比越高"}
        columnInfoMap["uniquedbl_4"] = {"description": "里程型_高等級(佔比)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=該類任務佔比越高"}
        columnInfoMap["uniquedbl_5"] = {"description": "每日型_高等級(佔比)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=該類任務佔比越高"}
        columnInfoMap["uniquedbl_6"] = {"description": "聯盟戰地類(佔比)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=該類任務佔比越高"}
        columnInfoMap["uniquedbl_7"] = {"description": "低等繁瑣里程類(佔比)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=該類任務佔比越高"}

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

    # SNA
    @classmethod
    def getBUReport26001Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "MD26001"
            , "tableNumber": "26001"
            , "dataName": "TAG_一對一交易"
            , "memo": "各角色各月，一對一交易標籤"
            , "dataType": "model_tag"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu26001".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000md26001"]
            , ["[:HashCode10Upper]", "0000MD26001"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料起始日期，資料均以一個月為處理單位"}
        columnInfoMap["game"] = {"description": "新楓之谷", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "Service Account"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲帳號(全數字)"}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色ID(全數字)"}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色名稱"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}
        columnInfoMap["commondata_9"] = {"description": "建模年月", "hidden": "false", "datatype": "string", "memo": "建模年月(YYYYMM)"}

        columnInfoMap["uniquestr_1"] = {"description": "緊密度", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是，closeness"}
        columnInfoMap["uniquestr_2"] = {"description": "中介度", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是，betweenness"}
        columnInfoMap["uniquestr_3"] = {"description": "群集度", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是，cluster"}
        columnInfoMap["uniquestr_4"] = {"description": "交易頻繁度(總計)", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是，degree centrality"}
        columnInfoMap["uniquestr_5"] = {"description": "交易頻繁度(販賣)", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是，in degree centrality"}
        columnInfoMap["uniquestr_6"] = {"description": "交易頻繁度(購買)", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是，out degree centrality"}

        columnInfoMap["uniquedbl_1"] = {"description": "緊密度(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=，closeness越高"}
        columnInfoMap["uniquedbl_2"] = {"description": "中介度(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=，betweenness越高"}
        columnInfoMap["uniquedbl_3"] = {"description": "群集度(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=，cluster越高"}
        columnInfoMap["uniquedbl_4"] = {"description": "交易頻繁度(總計)(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=，degree centrality越高"}
        columnInfoMap["uniquedbl_5"] = {"description": "交易頻繁度(販賣)(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=，in degree centrality越高"}
        columnInfoMap["uniquedbl_6"] = {"description": "交易頻繁度(購買)(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=，out degree centrality越高"}

        columnInfoMap["uniquedbl_11"] = {"description": "緊密度(原始值)", "hidden": "false", "datatype": "real", "memo": "closeness"}
        columnInfoMap["uniquedbl_12"] = {"description": "中介度(原始值)", "hidden": "false", "datatype": "real", "memo": "betweenness"}
        columnInfoMap["uniquedbl_13"] = {"description": "群集度(原始值)", "hidden": "false", "datatype": "real", "memo": "cluster"}
        columnInfoMap["uniquedbl_14"] = {"description": "交易頻繁度(總計)(原始值)", "hidden": "false", "datatype": "real", "memo": "degree centrality"}
        columnInfoMap["uniquedbl_15"] = {"description": "交易頻繁度(販賣)(原始值)", "hidden": "false", "datatype": "real", "memo": "in degree centrality"}
        columnInfoMap["uniquedbl_16"] = {"description": "交易頻繁度(購買)(原始值)", "hidden": "false", "datatype": "real", "memo": "out degree centrality"}

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
    def getBUReport25001Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "MD25001"
            , "tableNumber": "25001"
            , "dataName": "TAG_好友"
            , "memo": "各角色各月，好友網絡標籤"
            , "dataType": "model_tag"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu25001".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000md25001"]
            , ["[:HashCode10Upper]", "0000MD25001"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料起始日期，資料均以一個月為處理單位"}
        columnInfoMap["game"] = {"description": "新楓之谷", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "Service Account"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲帳號(全數字)"}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色ID(全數字)"}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色名稱"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}
        columnInfoMap["commondata_9"] = {"description": "建模年月", "hidden": "false", "datatype": "string", "memo": "建模年月(YYYYMM)"}

        #columnInfoMap["uniquestr_1"] = {"description": "緊密度", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是，closeness"}
        columnInfoMap["uniquestr_2"] = {"description": "交友廣度", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是，betweenness"}
        columnInfoMap["uniquestr_3"] = {"description": "家族型", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是，cluster"}
        columnInfoMap["uniquestr_4"] = {"description": "同好密集度", "hidden": "false", "datatype": "string", "memo": "0=否, 1=是，degree centrality"}

        #columnInfoMap["uniquedbl_1"] = {"description": "緊密度(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=，closeness越高"}
        columnInfoMap["uniquedbl_2"] = {"description": "交友廣度(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=，betweenness越高"}
        columnInfoMap["uniquedbl_3"] = {"description": "家族型(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=，cluster越高"}
        columnInfoMap["uniquedbl_4"] = {"description": "同好密集度(百分等級)", "hidden": "false", "datatype": "real", "memo": "介於0-1, 越大=，degree centrality越高"}

        #columnInfoMap["uniquedbl_11"] = {"description": "緊密度(原始值)", "hidden": "false", "datatype": "real", "memo": "closeness"}
        columnInfoMap["uniquedbl_12"] = {"description": "交友廣度(原始值)", "hidden": "false", "datatype": "real", "memo": "betweenness"}
        columnInfoMap["uniquedbl_13"] = {"description": "家族型(原始值)", "hidden": "false", "datatype": "real", "memo": "cluster"}
        columnInfoMap["uniquedbl_14"] = {"description": "同好密集度(總計)(原始值)", "hidden": "false", "datatype": "real", "memo": "degree centrality"}

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


    # NON
    @classmethod
    def getBUReport26002Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "MD26002"
            , "tableNumber": "26002"
            , "dataName": "TAG_拍賣場"
            , "memo": "各角色各月，拍賣場交易標籤"
            , "dataType": "model_tag"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu26002".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000md26002"]
            , ["[:HashCode10Upper]", "0000MD26002"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料起始日期，資料均以一個月為處理單位"}
        columnInfoMap["game"] = {"description": "新楓之谷", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "Service Account"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲帳號(全數字)"}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色ID(全數字)"}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色名稱"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}
        columnInfoMap["commondata_9"] = {"description": "建模年月", "hidden": "false", "datatype": "string", "memo": "建模年月(YYYYMM)"}

        columnInfoMap["uniqueint_1"] = {"description": "販賣次數", "hidden": "false", "datatype": "string", "memo": "販賣次數"}
        columnInfoMap["uniqueint_2"] = {"description": "購買次數", "hidden": "false", "datatype": "string", "memo": "購買次數"}

        columnInfoMap["uniquestr_1"] = {"description": "高拍賣販賣", "hidden": "false", "datatype": "string","memo": "販賣次數 為全服前10%"}
        columnInfoMap["uniquestr_2"] = {"description": "高拍賣購買", "hidden": "false", "datatype": "string", "memo": "購買次數 為全服前10%"}

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
    def getBUReport25002Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "MD25002"
            , "tableNumber": "25002"
            , "dataName": "TAG_公會"
            , "memo": "各角色各月，公會標籤"
            , "dataType": "model_tag"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu25002".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000md25002"]
            , ["[:HashCode10Upper]", "0000MD25002"]
        ]

        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料起始日期，資料均以一個月為處理單位"}
        columnInfoMap["game"] = {"description": "新楓之谷", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "Service Account"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲帳號(全數字)"}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色ID(全數字)"}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "hidden": "false", "datatype": "string", "memo": "楓之谷遊戲角色名稱"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}
        columnInfoMap["commondata_8"] = {"description": "公會ID", "hidden": "false", "datatype": "string", "memo": "guildid"}
        columnInfoMap["commondata_9"] = {"description": "建模年月", "hidden": "false", "datatype": "string", "memo": "建模年月(YYYYMM)"}


        columnInfoMap["uniqueint_1"] = {"description": "公會貢獻", "hidden": "false", "datatype": "string", "memo": "公會貢獻"}
        columnInfoMap["uniqueint_2"] = {"description": "公會等級", "hidden": "false", "datatype": "string", "memo": "公會等級"}

        columnInfoMap["uniquestr_1"] = {"description": "高公會貢獻", "hidden": "false", "datatype": "string", "memo": "超越公會平均"}
        columnInfoMap["uniquestr_2"] = {"description": "公會幹部", "hidden": "false", "datatype": "string", "memo": "正副公會長"}

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
    def getBUReport21011Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "MD21011"
            , "tableNumber": "21011"
            , "dataName": "真實用戶分類(帳號)"
            , "memo": "真實用戶分類(帳號)"
            , "dataType": "model_tag"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu21011".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000md21011"]
            , ["[:HashCode10Upper]", "0000MD21011"]
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
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniqueint_1"] = {"description": "分群代號", "hidden": "false", "datatype": "integer", "memo": "gender"}
        #columnInfoMap["uniqueint_2"] = {"description": "cluster", "hidden": "false", "datatype": "integer", "memo": "skin"}
        columnInfoMap["uniquestr_1"] = {"description": "分群(大類)", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_2"] = {"description": "分群(細類)", "datatype": "string", "hidden": "false"}


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
    def getBUReport21012Info(self, makeInfo):
        tableauInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "MD21012"
            , "tableNumber": "21012"
            , "dataName": "真實用戶分類(角色)"
            , "memo": "真實用戶分類(角色)"
            , "dataType": "model_tag"
        }

        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", "Select * FROM {}.bu21012".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000md21012"]
            , ["[:HashCode10Upper]", "0000MD21012"]
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
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniqueint_1"] = {"description": "分群代號", "hidden": "false", "datatype": "integer", "memo": "gender"}
        #columnInfoMap["uniqueint_2"] = {"description": "cluster", "hidden": "false", "datatype": "integer", "memo": "skin"}
        columnInfoMap["uniquestr_1"] = {"description": "分群(大類)", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_2"] = {"description": "分群(細類)", "datatype": "string", "hidden": "false"}

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














