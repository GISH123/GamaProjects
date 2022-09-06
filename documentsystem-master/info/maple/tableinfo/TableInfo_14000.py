from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic


class TableInfo_15000(TableInfoBasic):
    def __init__(self):
        pass

    @classmethod
    def getBUReport14051Info(self, makeInfo):
        tableInfo = {
            "dataCode": "GD14051"
            , "tableNumber": "14051"
            , "dataName": "任務列表"
            , "memo": "任務列表"
            , "dataType": "GameData"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期(非每日)", "hidden": "false", "datatype": "string_to_date", "memo": "資料日期 請使用最大日期為最新資料"}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": "產品名稱"}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": " 伺服器代碼"}
        columnInfoMap["uniquestr_1"] = {"description": "任務編號", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_2"] = {"description": "任務類別", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_3"] = {"description": "任務名稱", "datatype": "string", "hidden": "false"}
        return {}, columnInfoMap
