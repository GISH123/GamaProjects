from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic

class TableInfo_16000(TableInfoBasic):

    def __init__(self):
        pass

    @classmethod
    def getBUReport16001Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU16001"
            , "tableNumber": "16001"
            , "dataName": "儲值統計"
            , "memo": "bf!點數帳號儲值 手遊多平台溑費 (Apple,Google...)"
            , "dataType": ""
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "點帳OpenID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "bf!APPOpenID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": "平台帳號的相關國家"}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_1"] = {"description": "IP", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "儲值方式ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "儲值方式", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquedbl_1"] = {"description": "儲值金額", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquetime_1"] = {"description": "儲值時間", "hidden": "false", "datatype": "datetime", "memo": ""}
        return {}, columnInfoMap

    @classmethod
    def getBUReport16002Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU16002"
            , "tableNumber": "16002"
            , "dataName": "扣點統計"
            , "memo": "bf!遊戲帳號扣點 手遊遊戲內商城扣點"
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
        columnInfoMap["uniqueint_1"] = {"description": "扣點總額", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "一般扣點", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "專用扣點", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "IP", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "計費方式", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "扣點方式ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "扣點方式", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_5"] = {"description": "Memo(道具ID)", "hidden": "false", "datatype": "datetime", "memo": ""}

        return {}, columnInfoMap

