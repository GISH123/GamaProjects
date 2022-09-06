from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic

class TableInfo_11000(TableInfoBasic):

    @classmethod
    def getBUReport11802Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11802"
            , "tableNumber": "11802"
            , "dataName": "IP帳號資訊"
            , "memo": "IP帳號資訊"
            , "dataType": "XXXXX"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "hidden": "false", "datatype": "string", "memo": "平台名稱"}
        columnInfoMap["uniqueint_1"] = {"description": "IP編號", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_2"] = {"description": "IP登入次數", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniquestr_1"] = {"description": "IP", "datatype": "string", "hidden": "true"}
        return {}, columnInfoMap

    @classmethod
    def getBUReport11803Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU11803"
            , "tableNumber": "11803"
            , "dataName": "IP角色資訊"
            , "memo": "IP角色資訊"
            , "dataType": "XXXXX"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "遊戲帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": "遊戲角色ID"}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": "遊戲角色名稱"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": "平台openID"}
        columnInfoMap["commondata_10"] = {"description": "平台名稱", "hidden": "false", "datatype": "string", "memo": "平台名稱"}
        columnInfoMap["uniqueint_1"] = {"description": "IP編號", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_2"] = {"description": "IP登入次數", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniquestr_1"] = {"description": "IP", "datatype": "string", "hidden": "true"}
        return {}, columnInfoMap