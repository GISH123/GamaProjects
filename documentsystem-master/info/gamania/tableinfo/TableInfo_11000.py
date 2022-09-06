from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic

class TableInfo_11000(TableInfoBasic):

    def __init__(self):
        pass

    @classmethod
    def getBUReport11001Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11001"
            , "tableNumber": "11001"
            , "dataName": "每日登入/出"
            , "memo": "帳號/角色登入資料"
            , "dataType": ""
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務(遊戲)帳號", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "點帳OpenID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "bf!APPOpenID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}
        columnInfoMap["commondata_8"] = {"description": "服務代碼", "datatype": "string", "hidden": "true", "memo": "遊戲的代碼"}
        columnInfoMap["commondata_9"] = {"description": "服務名稱", "datatype": "string", "hidden": "true", "memo": "遊戲的中文名稱"}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniqueint_1"] = {"description": "點數帳號登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "遊戲帳號登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "點數帳號登出次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_4"] = {"description": "遊戲帳號登出次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "IP", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "服務帳號創立時間", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "點數帳號創立時間", "hidden": "false", "datatype": "string", "memo": ""}

        return {}, columnInfoMap

    @classmethod
    def getBUReport11002Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11002"
            , "tableNumber": "11002"
            , "dataName": "每日登入IP"
            , "memo": "帳號/角色登入IP"
            , "dataType": ""
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務(遊戲)帳號", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "點帳OpenID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "bf!APPOpenID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}
        columnInfoMap["commondata_8"] = {"description": "服務代碼", "datatype": "string", "hidden": "true", "memo": "遊戲的代碼"}
        columnInfoMap["commondata_9"] = {"description": "服務名稱", "datatype": "string", "hidden": "true", "memo": "遊戲的中文名稱"}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniqueint_1"] = {"description": "點數帳號登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "遊戲帳號登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "同IP登入點數帳號數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_4"] = {"description": "同IP登入遊戲帳號數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "IP", "hidden": "false", "datatype": "string", "memo": ""}

        return {}, columnInfoMap

    @classmethod
    def getBUReport11005Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11005"
            , "tableNumber": "11005"
            , "dataName": "每日在線"
            , "memo": "遊戲內當天在線角色資料"
            , "dataType": ""
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務(遊戲)帳號", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "點帳OpenID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "bf!APPOpenID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}
        columnInfoMap["commondata_8"] = {"description": "服務代碼", "datatype": "string", "hidden": "true", "memo": "遊戲的代碼"}
        columnInfoMap["commondata_9"] = {"description": "服務名稱", "datatype": "string", "hidden": "true", "memo": "遊戲的中文名稱"}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniqueint_1"] = {"description": "登入天數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "遊戲時間(秒)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "IP", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "服務帳號創立時間", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "點數帳號創立時間", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquetime_1"] = {"description": "登入時間", "datatype": "datetime", "hidden": "true"}
        columnInfoMap["uniquetime_2"] = {"description": "登出時間", "datatype": "datetime", "hidden": "true"}

        return {}, columnInfoMap


