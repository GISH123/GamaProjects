from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic

class TableInfo_11000(TableInfoBasic):
    def __init__(self):
        pass

    @classmethod
    def getBUReport11851Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME11851"
            , "tableNumber": "11851"
            , "dataName": "IP地理位置"
            , "memo": "IP所在國家、區域對照表"
            , "dataType": "CommonData"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "修改日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家代碼", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "IP區段起始(INT)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "IP區段結束(INT)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "IP區段起始(文字)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "IP區段結束(文字)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "國家代碼", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "國家全名", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_5"] = {"description": "區域名", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_6"] = {"description": "城市名", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_7"] = {"description": "郵遞區號", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_8"] = {"description": "時區", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquedbl_1"] = {"description": "經度", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquedbl_2"] = {"description": "緯度", "hidden": "false", "datatype": "integer", "memo": ""}

        return {"value": ""}, columnInfoMap

    @classmethod
    def getBUReport11852Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME11852"
            , "tableNumber": "11852"
            , "dataName": "IP之ASN"
            , "memo": "發行IP之ASN"
            , "dataType": "CommonData"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "修改日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "IP區段起始(INT)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "IP區段結束(INT)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "IP區段起始(文字)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "IP區段結束(文字)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "ASN代碼", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "ASN全名", "hidden": "false", "datatype": "integer", "memo": ""}

        return {"value": ""}, columnInfoMap

    @classmethod
    def getBUReport11853Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME11853"
            , "tableNumber": "11853"
            , "dataName": "IP網段資料"
            , "memo": "IP網段資料(國家)"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniqueint_1"] = {"description": "IP網段號碼(起)", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_2"] = {"description": "IP網段號碼(迄)", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniquestr_3"] = {"description": "國家簡碼", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_4"] = {"description": "國家全名", "datatype": "string", "hidden": "false"}
        return {}, columnInfoMap

    @classmethod
    def getBUReport11854Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME11854"
            , "tableNumber": "11854"
            , "dataName": "IP地理位置"
            , "memo": "IP所在國家、區域對照表"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家代碼", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "IP區段起始(INT)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "IP區段結束(INT)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "IP區段起始(文字)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "IP區段結束(文字)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "國家代碼", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "國家全名", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_5"] = {"description": "區域名", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_6"] = {"description": "城市名", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_7"] = {"description": "郵遞區號", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_8"] = {"description": "時區", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquedbl_1"] = {"description": "經度", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquedbl_2"] = {"description": "緯度", "hidden": "false", "datatype": "integer", "memo": ""}
        return {}, columnInfoMap
    
    @classmethod
    def getBUReport11861Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME11861"
            , "tableNumber": "11861"
            , "dataName": "IP地理位置"
            , "memo": "IP所在國家、區域對照表"
            , "dataType": "CommonData"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "修改日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家代碼", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "IP區段起始(INT)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "IP區段結束(INT)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "IP區段", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "國家全名(中文)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "國家代碼", "hidden": "false", "datatype": "integer", "memo": ""}
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

        return {"value": ""}, columnInfoMap

    def getBUReport11862Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME11862"
            , "tableNumber": "11862"
            , "dataName": "IP之ASN"
            , "memo": "發行IP之ASN"
            , "dataType": "CommonData"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "修改日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "IP區段起始(INT)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "IP區段結束(INT)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "IP區段起始(文字)", "hidden": "false", "datatype": "integer", "memo": ""}

        columnInfoMap["uniquestr_3"] = {"description": "ASN代碼", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "ASN全名", "hidden": "false", "datatype": "integer", "memo": ""}

        columnInfoMap["uniquestr_11"] = {"description": "IP第1碼", "hidden": "true", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_12"] = {"description": "IP第2碼", "hidden": "true", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquestr_13"] = {"description": "IP第3碼", "hidden": "true", "datatype": "integer", "memo": ""}

        return {"value": ""}, columnInfoMap