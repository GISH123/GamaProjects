from info.common.tableinfo.TableInfo_6000 import TableInfo_6000 as TableInfo_6000_Common


class TableInfo_6000(TableInfo_6000_Common) :
    def __init__(self):
        pass

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
            ["[:DataSourceName]",
             "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
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
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "viewer_id"}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": "WF"}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "true", "datatype": "string", "memo": "COMMON"}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string",
                                         "memo": "viewer_id"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "商品名稱", "hidden": "false", "datatype": "string", "memo": "product_name"}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "hidden": "false", "datatype": "string",
                                          "memo": "platform"}
        columnInfoMap["uniqueint_2"] = {"description": "付費次數", "hidden": "false", "datatype": "integer", "memo": "1"}
        columnInfoMap["uniquestr_1"] = {"description": "付費平台", "hidden": "false", "datatype": "string", "memo": "platform"}
        columnInfoMap["uniquestr_2"] = {"description": "付費方式", "hidden": "false", "datatype": "string", "memo": "currency_iso"}
        columnInfoMap["uniquedbl_1"] = {"description": "付費金額", "hidden": "false", "datatype": "real", "memo": "payment_money"}
        columnInfoMap["uniquedbl_2"] = {"description": "台幣付費金額", "datatype": "real", "hidden": "true", "memo" : "price"}
        columnInfoMap["uniquetime_1"] = {"description": "付費時間", "datatype": "datetime", "hidden": "true", "memo" : "create_time"}


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

        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "道具ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "道具名稱", "hidden": "false", "datatype": "string", "memo": "沒有道具名稱會以道具ID替代"}
        columnInfoMap["commondata_10"] = {"description": "道具ID", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniqueint_1"] = {"description": "(保留位)", "hidden": "true", "datatype": "integer", "memo": "消費金額保留位"}
        columnInfoMap["uniqueint_2"] = {"description": "消費次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "(保留位)", "hidden": "true", "datatype": "integer", "memo": "道具單價保留位"}
        columnInfoMap["uniqueint_4"] = {"description": "一組商品含道具數量", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_11"] = {"description": "actionID", "hidden": "false", "datatype": "integer", "memo": ""}

        columnInfoMap["uniquestr_11"] = {"description": "動作中文", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniquestr_1"] = {"description": "計費方式", "hidden": "false", "datatype": "string", "memo": "可能有台灣香港 請看6009"}
        columnInfoMap["uniquedbl_1"] = {"description": "消費金額", "datatype": "real", "hidden": "false"}
        columnInfoMap["uniquedbl_2"] = {"description": "道具單價", "datatype": "real", "hidden": "false"}

        columnInfoMap["uniquetime_1"] = {"description": "購買時間", "datatype": "datetime", "hidden": "false"}

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
