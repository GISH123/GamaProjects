from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic

class TableInfo_16000(TableInfoBasic):
    def __init__(self):
        pass

    @classmethod
    def getBUReport16009Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU16009"
            , "tableNumber": "16009"
            , "dataName": "商城購買"
            , "memo": "商城購買詳細資料"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}


        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "商品ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "道具名稱", "hidden": "false", "datatype": "string", "memo": "沒有道具名稱會以道具ID替代"}
        columnInfoMap["commondata_10"] = {"description": "道具ID", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniqueint_1"] = {"description": "(保留位)", "hidden": "true", "datatype": "integer", "memo": "消費金額保留位"}
        columnInfoMap["uniqueint_2"] = {"description": "消費次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "(保留位)", "hidden": "true", "datatype": "integer", "memo": "道具單價保留位"}
        columnInfoMap["uniqueint_4"] = {"description": "一組商品含道具數量", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_11"] = {"description": "actionID", "hidden": "false", "datatype": "integer","memo": ""}
        columnInfoMap["uniquestr_1"] = {"description": "計費方式", "hidden": "false", "datatype": "string", "memo": "可能有台灣香港 請看6009"}
        columnInfoMap["uniquestr_11"] = {"description": "動作中文", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquedbl_1"] = {"description": "消費金額", "datatype": "real", "hidden": "false"}
        columnInfoMap["uniquedbl_2"] = {"description": "道具單價", "datatype": "real", "hidden": "false"}
        
        columnInfoMap["uniquetime_1"] = {"description": "購買時間", "datatype": "datetime", "hidden": "false"}
        return {}, columnInfoMap
    
    @classmethod
    def getBUReport16301Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME16301"
            , "tableNumber": "16301"
            , "dataName": "非現金消費"
            , "memo": "非現金消費(里程及楓點購買)"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "商品ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "商品名稱", "hidden": "false", "datatype": "string", "memo": "沒有道具名稱會以道具ID替代"}
        columnInfoMap["commondata_10"] = {"description": "道具ID", "hidden": "false", "datatype": "string",  "memo": ""}

        columnInfoMap["uniqueint_1"] = {"description": "(保留位)", "hidden": "true", "datatype": "integer","memo": "消費金額保留位"}
        columnInfoMap["uniqueint_2"] = {"description": "消費次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "(保留位)", "hidden": "true", "datatype": "integer", "memo": "道具單價保留位"}
        columnInfoMap["uniqueint_4"] = {"description": "一組商品含道具數量", "hidden": "false", "datatype": "integer","memo": ""}
        columnInfoMap["uniqueint_11"] = {"description": "actionID", "hidden": "false", "datatype": "integer","memo": ""}

        columnInfoMap["uniquestr_11"] = {"description": "動作中文", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquedbl_1"] = {"description": "消費金額", "datatype": "real", "hidden": "false"}
        columnInfoMap["uniquedbl_2"] = {"description": "道具單價", "datatype": "real", "hidden": "false"}

        columnInfoMap["uniquetime_1"] = {"description": "購買時間", "datatype": "datetime", "hidden": "false"}
        return {}, columnInfoMap

    @classmethod
    def getBUReport16401Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME16401"
            , "tableNumber": "16401"
            , "dataName": "隨機箱產出"
            , "memo": "隨機箱產出(包含各類隨機箱)"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_8"] = {"description": "商品ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "商品名稱", "hidden": "false", "datatype": "string", "memo": "沒有道具名稱會以道具ID替代"}
        columnInfoMap["commondata_10"] = {"description": "產出之道具ID", "hidden": "false", "datatype": "string",  "memo": ""}

        columnInfoMap["uniqueint_1"] = {"description": "(保留位)", "hidden": "true", "datatype": "integer","memo": "消費金額保留位"}
        columnInfoMap["uniqueint_2"] = {"description": "消費次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "(保留位)", "hidden": "true", "datatype": "integer", "memo": "道具單價保留位"}
        columnInfoMap["uniqueint_4"] = {"description": "一組商品含道具數量", "hidden": "false", "datatype": "integer","memo": ""}
        columnInfoMap["uniqueint_11"] = {"description": "actionID", "hidden": "false", "datatype": "integer","memo": ""}

        columnInfoMap["uniquestr_11"] = {"description": "動作中文", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquedbl_1"] = {"description": "消費金額", "datatype": "real", "hidden": "false"}
        columnInfoMap["uniquedbl_2"] = {"description": "道具單價", "datatype": "real", "hidden": "false"}

        columnInfoMap["uniquetime_1"] = {"description": "抽取時間", "datatype": "datetime", "hidden": "false"}
        return {}, columnInfoMap

    @classmethod
    def getBUReport16451Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME16451"
            , "tableNumber": "16451"
            , "dataName": "時尚隨機箱產出資料"
            , "memo": "時尚隨機箱產出資料對照"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}


        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_6"] = {"description": "主標時裝名稱", "hidden": "false", "datatype": "string", "memo": "非主標時裝則為NULL"}
        columnInfoMap["commondata_7"] = {"description": "主標時裝部位", "hidden": "false", "datatype": "string", "memo": "非主標時裝則為NULL"}

        columnInfoMap["commondata_8"] = {"description": "商品ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "商品名稱", "hidden": "false", "datatype": "string", "memo": "沒有道具名稱會以道具ID替代"}
        columnInfoMap["commondata_10"] = {"description": "產出之道具ID", "hidden": "false", "datatype": "string",  "memo": ""}

        columnInfoMap["uniquestr_1"] = {"description": "大師標籤", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "IP合作", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "漂浮特效", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "暗色系", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_5"] = {"description": "螢光系", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_6"] = {"description": "柔和色系", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_7"] = {"description": "可愛風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_8"] = {"description": "制服風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_9"] = {"description": "貴族風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_10"] = {"description": "動物風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_11"] = {"description": "怪物風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_12"] = {"description": "角色風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_13"] = {"description": "自然風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_14"] = {"description": "重裝風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_15"] = {"description": "運動風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_16"] = {"description": "搞怪風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_17"] = {"description": "不擋身腿", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_18"] = {"description": "不擋住名字", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_19"] = {"description": "渲染特效", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniquedbl_1"] = {"description": "抽中機率", "datatype": "real", "hidden": "false"}

        columnInfoMap["uniquetime_1"] = {"description": "抽獎時間", "datatype": "datetime", "hidden": "false"}
        columnInfoMap["uniquetime_2"] = {"description": "物品檔期開始時間", "datatype": "datetime", "hidden": "false"}
        columnInfoMap["uniquetime_3"] = {"description": "物品檔期結束時間", "datatype": "datetime", "hidden": "false"}
        columnInfoMap["otherstr_1"] = {"description": "檔期開始時間", "datatype": "string", "hidden": "false"}

        return {}, columnInfoMap
    
    @classmethod
    def getBUReport16452Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME16452"
            , "tableNumber": "16452"
            , "dataName": "時尚隨機箱後續使用"
            , "memo": "時尚隨機箱後續使用(穿著、一對一交易、轉賣)"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_6"] = {"description": "主標時裝名稱", "hidden": "false", "datatype": "string", "memo": "非主標時裝則為NULL"}
        columnInfoMap["commondata_7"] = {"description": "主標時裝部位", "hidden": "false", "datatype": "string", "memo": "非主標時裝則為NULL"}

        columnInfoMap["commondata_8"] = {"description": "商品ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "商品ID", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniqueint_1"] = {"description": "商品ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_2"] = {"description": "是否穿著", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "是否拍賣場販賣", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_4"] = {"description": "是否一對一給予", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_5"] = {"description": "是否拍賣場購買", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniqueint_6"] = {"description": "是否一對一接受", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniquestr_1"] = {"description": "大師標籤", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "IP合作", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "漂浮特效", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "暗色系", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_5"] = {"description": "螢光系", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_6"] = {"description": "柔和色系", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_7"] = {"description": "可愛風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_8"] = {"description": "制服風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_9"] = {"description": "貴族風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_10"] = {"description": "動物風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_11"] = {"description": "怪物風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_12"] = {"description": "角色風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_13"] = {"description": "自然風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_14"] = {"description": "重裝風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_15"] = {"description": "運動風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_16"] = {"description": "搞怪風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_17"] = {"description": "不擋身腿", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_18"] = {"description": "不擋住名字", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_19"] = {"description": "渲染特效", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniquedbl_1"] = {"description": "抽中機率", "datatype": "real", "hidden": "false"}

        columnInfoMap["uniquetime_2"] = {"description": "物品檔期開始時間", "datatype": "datetime", "hidden": "false"}
        columnInfoMap["uniquetime_3"] = {"description": "物品檔期結束時間", "datatype": "datetime", "hidden": "false"}
        return {}, columnInfoMap

    @classmethod
    def getBUReport16453Info(self, makeInfo):
        tableInfo = {
            "dataCode": "ME16453"
            , "tableNumber": "16453"
            , "dataName": "時尚隨機箱獲得方法"
            , "memo": "時尚隨機箱獲得方法(購買/里程/送禮)"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_1"] = {"description": "服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_8"] = {"description": "商品ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "商品名稱", "hidden": "false", "datatype": "string", "memo": "沒有道具名稱會以道具ID替代"}
        columnInfoMap["commondata_10"] = {"description": "道具ID", "hidden": "false", "datatype": "string",  "memo": ""}

        columnInfoMap["uniqueint_1"] = {"description": "(保留位)", "hidden": "true", "datatype": "integer","memo": "消費金額保留位"}
        columnInfoMap["uniqueint_2"] = {"description": "消費次數", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniqueint_3"] = {"description": "(保留位)", "hidden": "true", "datatype": "integer", "memo": "道具單價保留位"}

        columnInfoMap["uniqueint_4"] = {"description": "一組商品含道具數量", "hidden": "false", "datatype": "integer","memo": ""}
        columnInfoMap["uniqueint_11"] = {"description": "actionID", "hidden": "false", "datatype": "integer","memo": ""}

        columnInfoMap["uniquestr_11"] = {"description": "動作中文", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquedbl_1"] = {"description": "消費金額", "datatype": "real", "hidden": "false"}
        columnInfoMap["uniquedbl_2"] = {"description": "道具單價", "datatype": "real", "hidden": "false"}

        columnInfoMap["uniquetime_1"] = {"description": "購買時間", "datatype": "datetime", "hidden": "false"}
        columnInfoMap["uniquetime_2"] = {"description": "活動時間", "datatype": "datetime", "hidden": "false"}

        return {}, columnInfoMap

    @classmethod
    def getBUReport16508Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU16508"
            , "tableNumber": "16508"
            , "dataName": "交易所(賣)"
            , "memo": "交易所(賣)"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "販賣服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "販賣遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "販賣遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "販賣遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "道具ID", "datatype": "string", "hidden": "false"}
        columnInfoMap["commondata_9"] = {"description": "道具名稱", "datatype": "string", "hidden": "false", "memo": "無道具名稱則用ID顯示"}
        columnInfoMap["uniqueint_1"] = {"description": "sn", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_2"] = {"description": "auctionid", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_3"] = {"description": "initprice", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_4"] = {"description": "directprice", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_5"] = {"description": "auctiontype", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_6"] = {"description": "itemtype", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_7"] = {"description": "state", "datatype": "integer", "hidden": "false"}
        # columnInfoMap["uniquestr_1"] = {"description": "購買服務帳號(遊戲帳號)", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_2"] = {"description": "購買遊戲帳號ID", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_3"] = {"description": "購買遊戲角色ID", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_4"] = {"description": "購買遊戲角色名稱", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquetime_1"] = {"description": "交易時間", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquetime_2"] = {"description": "上架時間", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquetime_2"] = {"description": "拍賣結束時間", "hidden": "false", "datatype": "integer", "memo": ""}
        return {}, columnInfoMap

    @classmethod
    def getBUReport16509Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU16509"
            , "tableNumber": "16509"
            , "dataName": "交易所交易"
            , "memo": "交易所交易"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "購買服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "購買遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "購買遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "購買遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "道具ID", "datatype": "string", "hidden": "false"}
        columnInfoMap["commondata_9"] = {"description": "道具名稱", "datatype": "string", "hidden": "false", "memo": "無道具名稱則用ID顯示"}
        columnInfoMap["uniqueint_1"] = {"description": "sn", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_2"] = {"description": "auctionid", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_3"] = {"description": "initprice", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_4"] = {"description": "directprice", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_5"] = {"description": "auctiontype", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_6"] = {"description": "itemtype", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_7"] = {"description": "state", "datatype": "integer", "hidden": "false"}
        # columnInfoMap["uniquestr_1"] = {"description": "販賣服務帳號(遊戲帳號)", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_2"] = {"description": "販賣遊戲帳號ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_3"] = {"description": "販賣遊戲角色ID", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_4"] = {"description": "販賣遊戲角色名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquetime_1"] = {"description": "交易時間", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquetime_2"] = {"description": "上架時間", "hidden": "false", "datatype": "integer", "memo": ""}
        columnInfoMap["uniquetime_2"] = {"description": "拍賣結束時間", "hidden": "false", "datatype": "integer", "memo": ""}
        return {}, columnInfoMap

    @classmethod
    def getBUReport16608Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU16608"
            , "tableNumber": "16608"
            , "dataName": "一對一交易"
            , "memo": "一對一交易"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "給予服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "給予遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "給予遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "給予遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "道具ID", "datatype": "string", "hidden": "false"}
        columnInfoMap["commondata_9"] = {"description": "道具名稱", "datatype": "string", "hidden": "false", "memo": "無道具名稱則用ID顯示"}
        columnInfoMap["uniqueint_1"] = {"description": "itemsn", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_2"] = {"description": "fieldid", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_3"] = {"description": "through", "datatype": "integer", "hidden": "false"}
        # columnInfoMap["uniquestr_1"] = {"description": "取得服務帳號(遊戲帳號)", "datatype": "string", "hidden": "true"}
        # columnInfoMap["uniquestr_2"] = {"description": "取得遊戲帳號ID", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_3"] = {"description": "取得遊戲角色ID", "datatype": "string", "hidden": "true"}
        # columnInfoMap["uniquestr_4"] = {"description": "取得遊戲角色名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquetime_1"] = {"description": "交易時間", "hidden": "false", "datatype": "integer", "memo": ""}
        return {}, columnInfoMap

    @classmethod
    def getBUReport16609Info(self, makeInfo):
        tableInfo = {
            "dataCode": "BU16609"
            , "tableNumber": "16609"
            , "dataName": "一對一交易"
            , "memo": "一對一交易"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "伺服器", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_1"] = {"description": "取得服務帳號(遊戲帳號)", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_2"] = {"description": "取得遊戲帳號ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_3"] = {"description": "取得遊戲角色ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_4"] = {"description": "取得遊戲角色名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "道具ID", "datatype": "string", "hidden": "false"}
        columnInfoMap["commondata_9"] = {"description": "道具名稱", "datatype": "string", "hidden": "false", "memo": "無道具名稱則用ID顯示"}
        columnInfoMap["uniqueint_2"] = {"description": "交易數量", "datatype": "integer", "hidden": "false"}
        # columnInfoMap["uniquestr_1"] = {"description": "給予服務帳號(遊戲帳號)", "datatype": "string", "hidden": "true"}
        # columnInfoMap["uniquestr_2"] = {"description": "給予遊戲帳號ID", "datatype": "string", "hidden": "false"}
        columnInfoMap["uniquestr_3"] = {"description": "給予遊戲角色ID", "datatype": "string", "hidden": "true"}
        # columnInfoMap["uniquestr_4"] = {"description": "給予遊戲角色名稱", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquetime_1"] = {"description": "交易時間", "hidden": "false", "datatype": "integer", "memo": ""}
        return {}, columnInfoMap

    @classmethod
    def getBUReport16091Info(self, makeInfo):
        tableInfo = {
            "dataCode": "FT16091"
            , "tableNumber": "16091"
            , "dataName": "商城ID對照表"
            , "memo": "商城ID對照表"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_7"] = {"description": "表格版本", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "商品ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "時裝名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_10"] = {"description": "時裝部位", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniqueint_1"] = {"description": "Count", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_2"] = {"description": "Price", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_3"] = {"description": "Bonus", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_4"] = {"description": "Period", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_5"] = {"description": "Priority", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_6"] = {"description": "reqPOP", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_7"] = {"description": "ReqLEV", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_8"] = {"description": "Gender", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_9"] = {"description": "OnSale", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_10"] = {"description": "MaplePoint", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_11"] = {"description": "Meso", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_12"] = {"description": "Premium", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_13"] = {"description": "Class", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_14"] = {"description": "Limit", "datatype": "integer", "hidden": "false"}
        columnInfoMap["uniqueint_15"] = {"description": "Status", "datatype": "integer", "hidden": "false"}



        columnInfoMap["uniquestr_1"] = {"description": "TermStart", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "TermEnd", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "BombSale", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "Forced_Category", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_5"] = {"description": "Forced_SubCategory", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_6"] = {"description": "GameWorld", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_7"] = {"description": "LimitMax", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_8"] = {"description": "LimitQuestID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_9"] = {"description": "OriginalPrice", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_10"] = {"description": "Discount", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_11"] = {"description": "MileageRate", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_12"] = {"description": "zero", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_13"] = {"description": "CouponType", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_14"] = {"description": "onlyMileage", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_15"] = {"description": "ShowDiscount", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_16"] = {"description": "update ver", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_17"] = {"description": "comment", "hidden": "false", "datatype": "string", "memo": ""}


        columnInfoMap["otherstr_1"] = {"description": "報表分類", "datatype": "string", "hidden": "false"}
        columnInfoMap["otherstr_2"] = {"description": "扣點分類", "datatype": "string", "hidden": "false"}
        columnInfoMap["otherstr_3"] = {"description": "上市時間", "datatype": "string", "hidden": "false"}
        columnInfoMap["otherstr_4"] = {"description": "大分類", "datatype": "string", "hidden": "false"}
        columnInfoMap["otherstr_5"] = {"description": "小分類", "datatype": "string", "hidden": "false"}
        columnInfoMap["otherstr_6"] = {"description": "備註", "datatype": "string", "hidden": "false"}
        columnInfoMap["otherstr_7"] = {"description": "Price.1", "datatype": "string", "hidden": "false"}
        columnInfoMap["otherstr_8"] = {"description": "活動", "datatype": "string", "hidden": "false"}
        columnInfoMap["otherstr_9"] = {"description": "season", "datatype": "string", "hidden": "false"}

        return {}, columnInfoMap

    @classmethod
    def getBUReport16092Info(self, makeInfo):
        tableInfo = {
            "dataCode": "FT16092"
            , "tableNumber": "16092"
            , "dataName": "時尚隨機箱特徵對照表"
            , "memo": "時尚隨機箱特徵對照表"
            , "dataType": "FrontTable"
        }
        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        columnInfoMap["dt"] = {"description": "日期", "hidden": "false", "datatype": "string_to_date", "memo": ""}
        columnInfoMap["game"] = {"description": "遊戲", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["world"] = {"description": "COMMON", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["commondata_7"] = {"description": "表格版本", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_8"] = {"description": "商品ID", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_9"] = {"description": "時裝名稱", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["commondata_10"] = {"description": "時裝部位", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniquestr_1"] = {"description": "大師標籤", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_2"] = {"description": "IP合作", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_3"] = {"description": "漂浮特效", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_4"] = {"description": "暗色系", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_5"] = {"description": "螢光系", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_6"] = {"description": "柔和色系", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_7"] = {"description": "可愛風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_8"] = {"description": "制服風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_9"] = {"description": "貴族風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_10"] = {"description": "動物風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_11"] = {"description": "怪物風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_12"] = {"description": "角色風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_13"] = {"description": "自然風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_14"] = {"description": "重裝風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_15"] = {"description": "運動風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_16"] = {"description": "搞怪風格", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_17"] = {"description": "不擋身腿", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_18"] = {"description": "不擋住名字", "hidden": "false", "datatype": "string", "memo": ""}
        columnInfoMap["uniquestr_19"] = {"description": "渲染特效", "hidden": "false", "datatype": "string", "memo": ""}

        columnInfoMap["uniquedbl_1"] = {"description": "抽中機率", "datatype": "real", "hidden": "false"}

        columnInfoMap["uniquetime_1"] = {"description": "物品檔期開始時間", "datatype": "datetime", "hidden": "false"}
        columnInfoMap["uniquetime_2"] = {"description": "物品檔期結束時間", "datatype": "datetime", "hidden": "false"}

        columnInfoMap["otherstr_1"] = {"description": "時裝韓文名稱", "datatype": "string", "hidden": "false"}
        columnInfoMap["otherstr_2"] = {"description": "時裝韓文部位", "datatype": "string", "hidden": "false"}

        return {}, columnInfoMap
