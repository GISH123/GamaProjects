from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic
import datetime

class TableInfo_16000(TableInfoBasic):

    @classmethod
    def getBUReport16001Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU16001"
            , "tableNumber": "16001"
            , "dataName": "現金儲值細項"
            , "memo": "現金儲值細項"
            , "dataType": "Login+"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_2"] = {"description": "帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_8"] = {"description": "商品ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_9"] = {"description": "商品名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_10"] = {"description": "平台", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniqueint_1"] = {"description": "(保留位)", "datatype": "integer", "hidden": "true","memo":"道具總價保留位"}
        columnInfoMap["uniqueint_2"] = {"description": "消費次數", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_3"] = {"description": "(保留位)", "datatype": "integer", "hidden": "true","memo":"道具單價保留位"}
        columnInfoMap["uniqueint_11"] = {"description": "付費平台ID", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_12"] = {"description": "付費狀態ID", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniquestr_1"] = {"description": "貨幣類別", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_2"] = {"description": "付費平台", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_3"] = {"description": "付費狀態", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquedbl_1"] = {"description": "道具總價", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_2"] = {"description": "道具單價", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquetime_1"] = {"description": "消費時間(台灣)", "datatype": "datetime", "hidden": "true"}
        return {}, columnInfoMap

    @classmethod
    def getBUReport16002Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU16002"
            , "tableNumber": "16002"
            , "dataName": "遊戲商城消費細項"
            , "memo": "遊戲商城購買紀錄有正負號請注意"
            , "dataType": "Login+"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_2"] = {"description": "帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_3"] = {"description": "角色ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_4"] = {"description": "角色名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_6"] = {"description": "平台openID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_8"] = {"description": "商品ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_9"] = {"description": "商品名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_10"] = {"description": "平台", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniqueint_1"] = {"description": "(保留位)", "datatype": "integer", "hidden": "true","memo":"道具總價保留位"}
        columnInfoMap["uniqueint_2"] = {"description": "消費次數", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_3"] = {"description": "(保留位)", "datatype": "integer", "hidden": "true","memo":"道具單價保留位"}
        columnInfoMap["UniqueInt_11"] = {"description": "貨幣類型ID", "datatype": "integer", "hidden": "true"}
        columnInfoMap["UniqueInt_12"] = {"description": "得到或消耗", "datatype": "integer", "hidden": "true" ,"memo":"1=得到 2=消耗"}
        columnInfoMap["uniquestr_1"] = {"description": "貨幣類別", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_11"] = {"description": "資料類別", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_12"] = {"description": "商城ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquedbl_1"] = {"description": "貨幣消耗", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_2"] = {"description": "道具單價", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquetime_1"] = {"description": "消費時間", "datatype": "datetime", "hidden": "true"}
        columnInfoMap["uniquejson_1"] = {"description": "原始資料", "datatype": "datetime", "hidden": "true"}
        return {}, columnInfoMap