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
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "角色職業", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "角色等級", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "帳號登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "角色登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "帳號登出次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_4"] = {"description": "角色登出次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "帳號創立時間", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "角色創立時間", "hidden": "false", "datatype": "string", "memo": ""}

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
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "角色職業", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "角色等級", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "帳號IP登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "角色IP登入次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "同IP登入帳號數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_4"] = {"description": "同IP登入角色數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_4"] = {"description": "IP數字代碼", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "IP", "hidden": "false", "datatype": "string", "memo": ""}

        return {}, columnInfoMap

    @classmethod
    def getBUReport11103Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11103"
            , "tableNumber": "11103"
            , "dataName": "每日在線(dt)"
            , "memo": "登入登出時間跟DT同一天(由11005製作)"
            , "dataType": ""
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "角色職業", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "角色等級", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "月份", "hidden": "false", "datatype": "string", "memo": "(yyyy-mm)"}
        columnInfoMap["uniqueint_2"] = {"description": "遊戲時間(秒)", "hidden": "false", "datatype": "integer", "memo": ""}
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
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "角色職業", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "角色等級", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "登入天數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "遊戲時間(秒)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "IP", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "帳號創立時間", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "角色創立時間", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquetime_1"] = {"description": "登入時間", "hidden": "false", "datatype": "datetime", "memo": ""}
        columnInfoMap["uniquetime_2"] = {"description": "登出時間", "hidden": "false", "datatype": "datetime", "memo": ""}

        return {}, columnInfoMap

    @classmethod
    def getBUReport11131Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11131"
            , "tableNumber": "11131"
            , "dataName": "當時在線差異人數"
            , "memo": "當時在線人數(由11103製作)"
            , "dataType": ""
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "增加人數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "當時時間", "hidden": "false", "datatype": "string", "memo": ""}
        return {}, columnInfoMap

    @classmethod
    def getBUReport11806Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11806"
            , "tableNumber": "11806"
            , "dataName": "帳號IP登入相關資料(新版)"
            , "memo": "帳號IP登入相關資料"
            , "dataType": "Login+"
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "true", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniqueint_3"] = {"description": "同IP登入帳號數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_4"] = {"description": "同IP登入角色數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_4"] = {"description": "IP數字代碼", "hidden": "false", "datatype": "integer", "memo": ""}

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

        return {}, columnInfoMap
