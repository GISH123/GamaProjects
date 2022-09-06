from info.common.tableinfo.TableInfo_11000 import TableInfo_11000 as TableInfo_11000_Common

class TableInfo_11000(TableInfo_11000_Common):
    def __init__(self):
        pass

    @classmethod
    def getBUReport11002Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11002"
            , "tableNumber": "11002"
            , "dataName": "帳號登入"
            , "memo": "帳號登入"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "true", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquetime_1"] = {"description": "帳號創立時間", "hidden": "false", "datatype": "integer", "memo": ""}
        return {}, columnInfoMap

    @classmethod
    def getBUReport11003Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11003"
            , "tableNumber": "11003"
            , "dataName": "角色登入"
            , "memo": "角色登入"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料日期"}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": " 伺服器代碼"}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "服務帳號(遊戲帳號)"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": "遊戲帳號ID"}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": "遊戲角色ID"}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": "遊戲角色名稱"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["uniquetime_1"] = {"description": "角色創立時間", "hidden": "false", "datatype": "datetime", "memo": "角色創立時間"}
        return {}, columnInfoMap

    @classmethod
    def getBUReport11003Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11003"
            , "tableNumber": "11003"
            , "dataName": "角色登入"
            , "memo": "角色登入"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": "資料日期"}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": " 伺服器代碼"}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": "服務帳號(遊戲帳號)"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": "遊戲帳號ID"}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": "遊戲角色ID"}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": "遊戲角色名稱"}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": "點數帳號ID"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["uniquetime_1"] = {"description": "角色創立時間", "hidden": "false", "datatype": "datetime", "memo": "角色創立時間"}
        return {}, columnInfoMap

    @classmethod
    def getBUReport11102Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11102"
            , "tableNumber": "11102"
            , "dataName": "帳號當天的在線時間"
            , "memo": "帳號當天的在線時間"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "true", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquetime_1"] = {"description": "當天登入時間", "hidden": "false", "datatype": "datetime", "memo": ""}
        columnInfoMap["uniquetime_2"] = {"description": "當天登出時間", "hidden": "false", "datatype": "datetime", "memo": ""}
        return {}, columnInfoMap

    @classmethod
    def getBUReport11103Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11103"
            , "tableNumber": "11103"
            , "dataName": "角色當天在線時間"
            , "memo": "角色當天的在線時間"
            , "dataType": "FrontTable"
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
        columnInfoMap["uniquetime_1"] = {"description": "當天登入時間", "hidden": "false", "datatype": "datetime", "memo": ""}
        columnInfoMap["uniquetime_2"] = {"description": "當天登出時間", "hidden": "false", "datatype": "datetime", "memo": ""}
        return {}, columnInfoMap

    @classmethod
    def getBUReport11082Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11082"
            , "tableNumber": "11082"
            , "dataName": "帳號當天的在線時間"
            , "memo": "帳號當天的在線時間"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "true", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description":"IP整數", "hidden": "false", "datatype":"integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description":"IP登入次數", "hidden": "false", "datatype":"integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "IP位置", "hidden": "true", "datatype": "string", "memo": ""}
        return {}, columnInfoMap

    @classmethod
    def getBUReport11083Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11083"
            , "tableNumber": "11083"
            , "dataName": "角色當天在線時間"
            , "memo": "角色當天的在線時間"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description":"IP整數", "hidden": "false", "datatype":"integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description":"IP登入次數", "hidden": "false", "datatype":"integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "IP位置", "hidden": "true", "datatype": "string", "memo": ""}
        return {}, columnInfoMap