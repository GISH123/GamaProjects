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

    @classmethod
    def getBUReport1806Info(self, makeInfo):
        tableInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1806"
            , "tableNumber": "1806"
            , "dataName": "帳號IP登入相關資料(新版)"
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
            , ["[:SelectSQLCode]", "Select * FROM {}.bu1806".format(makeInfo["schemaName"])]
            , ["[:HashCode10Lower]", "0000bu1806"]
            , ["[:HashCode10Upper]", "0000BU1806"]
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

        columnInfoMap["uniqueint_1"] = {"description": "該IP數字代碼", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "該IP登入次數", "hidden": "false", "datatype": "integer", "memo": ""}

        columnInfoMap["uniquestr_1"] = {"description": "該IP位置", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "國家代碼", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "國家全名(中文)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "國家全名(英文)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_5"] = {"description": "區域名", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_6"] = {"description": "城市名", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_7"] = {"description": "郵遞區號", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_8"] = {"description": "時區", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_11"] = {"description": "IP第1碼", "hidden": "true", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_12"] = {"description": "IP第2碼", "hidden": "true", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_13"] = {"description": "IP第3碼", "hidden": "true", "datatype": "integer", "memo": ""}



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
    def getBUReport1851Info(self, makeInfo):
        tableInfoMap = self.getDataSourceBasicInfo()
        tableInfo = {
            "dataCode": "BU1851"
            , "tableNumber": "1851"
            , "dataName": "帳號IP、付費登入資料"
            , "memo": "結合18026 6002 1002"
            , "dataType": "Login+"
        }
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", self.__getsql1851()]
            , ["[:HashCode10Lower]", "0000bu1851"]
            , ["[:HashCode10Upper]", "0000BU1851"]
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
        columnInfoMap["commondata_15"] = {"description": "付費服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniqueint_1"] = {"description": "當天登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "當天登出次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "該IP數字代碼", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_4"] = {"description": "該IP登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_5"] = {"description": "消費金額", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_6"] = {"description": "消費次數", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniquestr_1"] = {"description": "該IP位置", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "國家代碼", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "國家全名(中文)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "國家全名(英文)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_5"] = {"description": "區域名", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_6"] = {"description": "城市名", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_7"] = {"description": "郵遞區號", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_8"] = {"description": "時區", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_9"] = {"description": "計費方式", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquedbl_1"] = {"description": "經度", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquedbl_2"] = {"description": "緯度", "hidden": "false", "datatype": "integer", "memo": ""}

        columnInfoMap["uniquetime_1"] = {"description": "帳號創立時間","hidden":"false" ,"datatype":"datetime","memo":"帳號創立時間"}

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
    def __getsql1851(self):
        return """
        select 
            dt, 
            world , 
            game,
            CommonData_1,
            MAX(CommonData_2 ) AS CommonData_2 ,
            MAX(CommonData_3 ) AS CommonData_3 ,
            MAX(CommonData_4 ) AS CommonData_4 ,
            MAX(CommonData_5 ) AS CommonData_5 ,
            MAX(CommonData_6 ) AS CommonData_6 ,
            MAX(CommonData_7 ) AS CommonData_7 ,
            MAX(CommonData_8 ) AS CommonData_8 ,
            MAX(CommonData_9 ) AS CommonData_9 ,
            MAX(CommonData_10) AS CommonData_10,
            MAX(CommonData_11) AS CommonData_11,
            MAX(CommonData_12) AS CommonData_12,
            MAX(CommonData_13) AS CommonData_13,
            MAX(CommonData_14) AS CommonData_14,
            MAX(CASE WHEN tn = 6002 THEN CommonData_1 END) AS CommonData_15,
            MAX(CASE WHEN tn = 1001 THEN UniqueInt_1 END) AS UniqueInt_1,
            MAX(CASE WHEN tn = 1001 THEN UniqueInt_2 END) AS UniqueInt_2,
            MAX(CASE WHEN tn = 1806 THEN UniqueInt_1 END) AS UniqueInt_3,
            MAX(CASE WHEN tn = 1806 THEN UniqueInt_2 END) AS UniqueInt_4,
            MAX(CASE WHEN tn = 6002 THEN UniqueInt_1 END) AS UniqueInt_5,
            MAX(CASE WHEN tn = 6002 THEN UniqueInt_2 END) AS UniqueInt_6,
            NULL AS UniqueInt_7,
            NULL AS UniqueInt_8,
            NULL AS UniqueInt_9,
            NULL AS UniqueInt_10,
            NULL AS UniqueInt_11,
            NULL AS UniqueInt_12,
            NULL AS UniqueInt_13,
            NULL AS UniqueInt_14,
            NULL AS UniqueInt_15,
            MAX(CASE WHEN tn = 1806 THEN UniqueStr_1 END) AS UniqueStr_1,
            MAX(CASE WHEN tn = 1806 THEN UniqueStr_2 END) AS UniqueStr_2,
            MAX(CASE WHEN tn = 1806 THEN UniqueStr_3 END) AS UniqueStr_3,
            MAX(CASE WHEN tn = 1806 THEN UniqueStr_4 END) AS UniqueStr_4,
            MAX(CASE WHEN tn = 1806 THEN UniqueStr_5 END) AS UniqueStr_5,
            MAX(CASE WHEN tn = 1806 THEN UniqueStr_6 END) AS UniqueStr_6,
            MAX(CASE WHEN tn = 1806 THEN UniqueStr_7 END) AS UniqueStr_7,
            MAX(CASE WHEN tn = 1806 THEN UniqueStr_8 END) AS UniqueStr_8,
            MAX(CASE WHEN tn = 6002 THEN UniqueInt_1 END) AS UniqueStr_9, 
            NULL AS UniqueStr_10,
            NULL AS UniqueStr_11,
            NULL AS UniqueStr_12,
            NULL AS UniqueStr_13,
            NULL AS UniqueStr_14,
            NULL AS UniqueStr_15,
            NULL AS UniqueStr_16,
            NULL AS UniqueStr_17,
            NULL AS UniqueStr_18,
            NULL AS UniqueStr_19,
            NULL AS UniqueStr_20,
            MAX(UniqueDbl_1) AS UniqueDbl_1,
            MAX(UniqueDbl_2) AS UniqueDbl_2,
            NULL AS UniqueDbl_3,
            NULL AS UniqueDbl_4,
            NULL AS UniqueDbl_5,
            NULL AS UniqueDbl_6,
            NULL AS UniqueDbl_7,
            NULL AS UniqueDbl_8,
            NULL AS UniqueDbl_9,
            NULL AS UniqueDbl_10,
            NULL AS UniqueDbl_11,
            NULL AS UniqueDbl_12,
            NULL AS UniqueDbl_13,
            NULL AS UniqueDbl_14,
            NULL AS UniqueDbl_15,
            NULL AS UniqueDbl_16,
            NULL AS UniqueDbl_17,
            NULL AS UniqueDbl_18,
            NULL AS UniqueDbl_19,
            NULL AS UniqueDbl_20,
            MAX(UniqueTime_1) as UniqueTime_1,
            NULL as UniqueTime_2,
            NULL as UniqueTime_3,
            NULL AS OtherStr_1,
            NULL AS OtherStr_2,
            NULL AS OtherStr_3,
            NULL AS OtherStr_4,
            NULL AS OtherStr_5,
            NULL AS OtherStr_6,
            NULL AS OtherStr_7,
            NULL AS OtherStr_8,
            NULL AS OtherStr_9,
            NULL AS OtherStr_10
        from 
        (
            select * ,1002 as tn from maple_v.bu1002
            union all 
            select * ,6002 as tn from maple_v.bu6002
            union all 
            select * ,1806 as tn from maple_v.bu1806
        )AA
        group by dt, world , game , commondata_1
        """

