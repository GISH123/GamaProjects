from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic

class TableInfo_10000(TableInfoBasic):

    def __init__(self):
        pass

    @classmethod
    def getBUReport10001Info(self, makeInfo):

        tableInfo = {
            "dataCode": "BU10001"
            , "tableNumber": "10001"
            , "dataName": "端遊平台資訊"
            , "memo": "bf!平台帳號資料手遊平台帳號資料"
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
        columnInfoMap["commondata_7"] = {"description": "國家","hidden":"false", "datatype":"string","memo":"平台帳號的相關國家"}
        columnInfoMap["commondata_8"] = {"description": "服務代碼", "datatype": "string", "hidden": "true", "memo": "遊戲的代碼"}
        columnInfoMap["commondata_9"] = {"description": "服務名稱", "datatype": "string", "hidden": "true", "memo": "遊戲的中文名稱"}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_11"] = {"description": "是否為測試帳號", "datatype": "integer", "hidden": "true","memo":"1 為測試 , 0 為正式"}
        columnInfoMap["uniquestr_2"] = {"description": "服務帳號創立時間", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "點數帳號創立時間", "hidden": "false", "datatype": "string", "memo": ""}

        return {}, columnInfoMap

