from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic

class TableInfo_11000(TableInfoBasic):
    def __init__(self):
        pass

    @classmethod
    def getBUReport11001Info(self, makeInfo):
        tableInfo= {
            "dataCode": "BU11001"
            , "tableNumber": "11001"
            , "dataName" : "測試資料"
            , "memo": "測試資料"
            , "dataType": "TestData"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "服務帳號(遊戲帳號)"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "bf!APPOpenID", "hidden": "false", "datatype": "string", "memo": "bf!APPOpenID"}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}

        columnInfoMap["uniqueint_1"] = {"description": "點數帳號IP登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "遊戲帳號IP登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "點數帳號登出次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_4"] = {"description": "遊戲帳號登出次數", "hidden": "false", "datatype": "integer", "memo": ""}

        columnInfoMap["uniquestr_1"] = {"description": "IP", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "服務帳號創立時間", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "點數帳號創立時間", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "星星帳號創立時間", "hidden": "false", "datatype": "integer", "memo": ""}

        return {"value": ""}, columnInfoMap

    @classmethod
    def getBUReport11002Info(self, makeInfo):
        tableInfo= {
            "dataCode": "BU11002"
            , "tableNumber": "11002"
            , "dataName" : "測試資料"
            , "memo": "測試資料"
            , "dataType": "TestData"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "服務帳號(遊戲帳號)"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "bf!APPOpenID", "hidden": "false", "datatype": "string", "memo": "bf!APPOpenID"}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}

        columnInfoMap["uniqueint_1"] = {"description": "點數帳號IP登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "遊戲帳號IP登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "同IP登入點數帳號數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_4"] = {"description": "同IP登入遊戲帳號數", "hidden": "false", "datatype": "integer", "memo": ""}

        return {"value": ""}, columnInfoMap

    @classmethod
    def getBUReport11005Info(self, makeInfo):
        tableInfo= {
            "dataCode": "BU11005"
            , "tableNumber": "11005"
            , "dataName" : "測試資料"
            , "memo": "測試資料"
            , "dataType": "TestData"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "服務帳號(遊戲帳號)"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}

        columnInfoMap["uniqueint_1"] = {"description": "登入天數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "遊戲時間（秒）", "hidden": "false", "datatype": "integer", "memo": ""}

        columnInfoMap["uniquestr_1"] = {"description": "IP", "hidden": "false", "datatype": "String", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "服務帳號創立時間", "hidden": "false", "datatype": "String", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "點數帳號創立時間", "hidden": "false", "datatype": "String", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "星星帳號創立時間", "hidden": "false", "datatype": "String", "memo": ""}

        columnInfoMap["uniquetime_1"] = {"description": "登入時間", "hidden": "false", "datatype": "TimeStamp", "memo": ""}
        columnInfoMap["uniquetime_2"] = {"description": "登出時間", "hidden": "false", "datatype": "TimeStamp", "memo": ""}

        return {"value": ""}, columnInfoMap