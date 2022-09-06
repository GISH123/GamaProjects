from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic

class TableInfo_13000(TableInfoBasic):
    def __init__(self):
        pass

    @classmethod
    def getBUReport13051Info(self, makeInfo):
        tableInfo = {
            "dataCode": "FT13051"
            , "tableNumber": "13051"
            , "dataName": "是否裝備防掉%裝備"
            , "memo": "是否裝備防掉%裝備"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "maple", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "true", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "item_1662072", "datatype": "integer", "hidden": "true", "memo": "當天是否有裝備道具1662072"}
        columnInfoMap["uniqueint_2"] = {"description": "item_1662073", "datatype": "integer", "hidden": "true", "memo": "當天是否有裝備道具1662073"}
        return {}, columnInfoMap

    @classmethod
    def getBUReport13052Info(self, makeInfo):
        tableInfo = {
            "dataCode": "FT13052"
            , "tableNumber": "13052"
            , "dataName": "是否有經驗加倍道具"
            , "memo": "是否有經驗加倍道具"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "maple", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniqueint_1"] = {"description": "道具ID", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniquetime_1"] = {"description": "開始日期", "datatype": "datetime", "hidden": "false"}
        columnInfoMap["uniquetime_2"] = {"description": "結束日期", "datatype": "datetime", "hidden": "false"}
        return {}, columnInfoMap
    
    @classmethod
    def getBUReport13053Info(self, makeInfo):
        tableInfo = {
            "dataCode": "FT13053"
            , "tableNumber": "13053"
            , "dataName": "真實用戶模型串聯時裝穿著資料"
            , "memo": "真實用戶模型串聯時裝穿著資料"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "maple", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_10"] = {"description": "道具ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_11"] = {"description": "道具名稱", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "真實用戶分析(原始類別)", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_2"] = {"description": "真實用戶分析(重要度類別)", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniquestr_1"] = {"description": "真實用戶分析(大類)", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_2"] = {"description": "真實用戶分析(細類)", "datatype": "string", "hidden": "false"}
        return {}, columnInfoMap

    @classmethod
    def getBUReport13054Info(self, makeInfo):
        tableInfo = {
            "dataCode": "FT13054"
            , "tableNumber": "13054"
            , "dataName": "真實用戶模型串聯\n(非活動取得)時裝穿著資料"
            , "memo": "真實用戶模型串聯\n(非活動取得)時裝穿著資料"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "maple", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_10"] = {"description": "道具ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_11"] = {"description": "道具名稱", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "真實用戶分析(原始類別)", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_2"] = {"description": "真實用戶分析(重要度類別)", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniquestr_1"] = {"description": "真實用戶分析(大類)", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_2"] = {"description": "真實用戶分析(細類)", "datatype": "string", "hidden": "false"}
        return {}, columnInfoMap

    @classmethod
    def getBUReport13056Info(self, makeInfo):
        tableInfo = {
            "dataCode": "FT13056"
            , "tableNumber": "13056"
            , "dataName": "全玩家時裝對應bf活動道具"
            , "memo": "全玩家時裝對應bf活動道具"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "maple", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_5"] = {"description": "點數帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "國家", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "道具ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_10"] = {"description": "道具顏色", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquedbl_1"] = {"description": "道具顏色重要度占比", "hidden": "false", "datatype": "real"}
        columnInfoMap["uniquestr_1"] = {"description": "道具顏色", "datatype": "string", "hidden": "false"}

    @classmethod
    def getBUReport13091Info(self, makeInfo):
        tableInfo = {
            "dataCode": "FT13091"
            , "tableNumber": "13091"
            , "dataName": "道具對照表"
            , "memo": "道具對照表"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期(20210407)", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "maple", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_7"] = {"description": "表格版本", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "道具ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "道具名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "道具ID", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniquestr_1"] = {"description": "道具名稱", "datatype": "string", "hidden": "false"}

        return {}, columnInfoMap


    @classmethod
    def getBUReport13092Info(self, makeInfo):
        tableInfo = {
            "dataCode": "FT13092"
            , "tableNumber": "13092"
            , "dataName": "時裝對照表(去除活動時裝)"
            , "memo": "時裝對照表_V234(去除活動時裝)"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期(20210407)", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "maple", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_1"] = {"description": "道具ID", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniquestr_1"] = {"description": "道具名稱", "datatype": "string", "hidden": "false"}
        return {}, columnInfoMap


    @classmethod
    def getBUReport13095Info(self, makeInfo):
        tableInfo = {
            "dataCode": "FT13095"
            , "tableNumber": "13095"
            , "dataName": "時裝主要顏色"
            , "memo": "時裝主要顏色，以RBG/LAB表示"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期(20210515)", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "maple", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "道具ID", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniquestr_1"] = {"description": "類別", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_2"] = {"description": "道具顏色重要度排序", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquedbl_1"] = {"description": "道具顏色重要度占比", "hidden": "false", "datatype": "real"}
        columnInfoMap["uniqueint_1"] = {"description": "R/RGB", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_2"] = {"description": "G/RGB", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_3"] = {"description": "B/RGB", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_4"] = {"description": "L/lab", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_5"] = {"description": "a/Lab", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_6"] = {"description": "b/Lab", "datatype": "integer", "hidden": "false"}
        return {}, columnInfoMap

    @classmethod
    def getBUReport13096Info(self, makeInfo):
        tableInfo = {
            "dataCode": "FT13096"
            , "tableNumber": "13096"
            , "dataName": "時裝主要顏色比例(粉紅、紫、黃、灰)"
            , "memo": "時裝主要顏色之LAB距離"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期(20210515)", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "maple", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "道具ID", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniquestr_1"] = {"description": "類別", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_2"] = {"description": "道具顏色重要度排序", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_3"] = {"description": "最接近之顏色(無設定門檻)", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_4"] = {"description": "最接近之顏色(有設定門檻)", "datatype": "string", "hidden": "false"}


        columnInfoMap["uniquedbl_1"] = {"description": "道具顏色重要度占比", "hidden": "false", "datatype": "real"}
        columnInfoMap["uniquedbl_2"] = {"description": "與粉紅色之距離", "hidden": "false", "datatype": "real"}
        columnInfoMap["uniquedbl_3"] = {"description": "與紫色之距離", "hidden": "false", "datatype": "real"}
        columnInfoMap["uniquedbl_4"] = {"description": "與黃色之距離", "hidden": "false", "datatype": "real"}
        columnInfoMap["uniquedbl_5"] = {"description": "與灰色之距離", "hidden": "false", "datatype": "real"}

        return {}, columnInfoMap
