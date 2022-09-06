from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic

class TableInfo_17000(TableInfoBasic):
    def __init__(self):
        pass

    @classmethod
    def getBUReport17001Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU17001"
            , "tableNumber": "17001"
            , "dataName": "聯盟戰地"
            , "memo": "聯盟戰地"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "unionlevel", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_2"] = {"description": "uniondps", "datatype": "integer", "hidden": "true"}
        return {}, columnInfoMap

    @classmethod
    def getBUReport17002Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU17002"
            , "tableNumber": "17002"
            , "dataName": "武陵道館"
            , "memo": "武陵道館"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "time_data", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_2"] = {"description": "floor_data", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_3"] = {"description": "clear_data", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_4"] = {"description": "job_data", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_5"] = {"description": "dojangRankJob_data", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_6"] = {"description": "dojangRank0_data", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_7"] = {"description": "dojangRank2_data", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_8"] = {"description": "rankrwd_data", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_9"] = {"description": "type_data", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_10"] = {"description": "percent_data", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_11"] = {"description": "rwd_data", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniquestr_1"] = {"description": "A_100465_data", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_2"] = {"description": "A_100472_data", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_3"] = {"description": "A_100467_data", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquetime_1"] = {"description": "lastrank_data", "datatype": "datetime", "hidden": "true"}
        columnInfoMap["uniquetime_2"] = {"description": "lastrwd_data", "datatype": "datetime", "hidden": "true"}
        return {}, columnInfoMap

