from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic


class TableInfo_15000(TableInfoBasic):
    def __init__(self):
        pass

    @classmethod
    def getBUReport15009Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME15009"
            , "tableNumber": "15009"
            , "dataName": "好友細項"
            , "memo": "好友細項"
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
        columnInfoMap["uniqueint_1"] = {"description": "昨日是否上線", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniquestr_1"] = {"description": "好友角色ID", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_2"] = {"description": "好友角色名稱", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_3"] = {"description": "好友群組", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_4"] = {"description": "flag", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquetime_1"] = {"description": "好友創立時間", "datatype": "datetime", "hidden": "false"}
        columnInfoMap["uniquetime_2"] = {"description": "最後修改時間", "datatype": "datetime", "hidden": "false"}
        return {}, columnInfoMap

    def getBUReport15109Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME15109"
            , "tableNumber": "15109"
            , "dataName": "公會細項"
            , "memo": "公會細項"
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

        columnInfoMap["commondata_8"] = {"description": "公會ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "公會名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "公會成員職位", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "公會成員貢獻(起)", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "公會成員貢獻(迄)", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_4"] = {"description": "公會成員貢獻(差異)", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_5"] = {"description": "昨日是否上線", "datatype": "integer", "hidden": "false"}
        return {}, columnInfoMap