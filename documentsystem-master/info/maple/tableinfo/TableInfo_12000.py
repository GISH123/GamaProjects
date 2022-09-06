from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic

class TableInfo_12000(TableInfoBasic):
    def __init__(self):
        pass

    @classmethod
    def getBUReport12051Info(self, makeInfo):
        tableInfo = {
            "dataCode": "GD12051"
            , "tableNumber": "12051"
            , "dataName": "經驗值等級表"
            , "memo": "帳號登入"
            , "dataType": "GameData"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "maple", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "true", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "等級", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_2"] = {"description": "該等級升級所需經驗", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_3"] = {"description": "該等級升級累積經驗", "datatype": "integer", "hidden": "true"}
        return {}, columnInfoMap

