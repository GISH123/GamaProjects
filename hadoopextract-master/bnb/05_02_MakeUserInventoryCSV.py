import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option('display.max_rows', None)
pandas.set_option("display.width",2000)

extracthCtrl = ExtracthCtrl()


#==================== 檔案設定 ====================

tdnWorldDF = pandas.read_csv("file/bnb_UserInventory_table.csv")
tableCheck = {
    # Transfer ALL, Not filtered
    "ChangeShadow":"尚未確定(小於500K) ALL ALL None None 1"
    , "CountSMS":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCAJJangRewardLog":"尚未確定(0M) ALL ALL None None 1"
    , "FxCart":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxEmotionInventory":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxLucciTranx":"尚未確定(0M) ALL ALL None None 1"
    , "FxPreInventory1202":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxPurchaseRecord":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxQCouponInventory":"尚未確定(0M) ALL ALL None None 1"
    , "FxSMS":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxTranx":"紀錄玩家買賣與收送道具紀錄(小於500K) ALL ALL None None 1"
    , "FxTranx_20090702":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxUpdatedNoticeSn":"尚未確定(0M) ALL ALL None None 1"
    , "FxUserDIYBomb":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxUserDailyMission":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxUserInventory00_200904016":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxUserPetInfo":"尚未確定(大於1M) ALL ALL None None 1"
    , "GM_Inventory_Deleted":"尚未確定(小於500K) ALL ALL None None 1"
    , "Log_Password":"尚未確定(0M) ALL ALL None None 1"
    , "LoveCountry0":"尚未確定(小於500K) ALL ALL None None 1"
    , "Lovecountry":"尚未確定(小於500K) ALL ALL None None 1"
    , "Sendreward":"尚未確定(小於500K) ALL ALL None None 1"
    , "Userid_Count":"尚未確定(小於500K) ALL ALL None None 1"
    , "cookiecomplete":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxMMSynthesisLog":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxDTTLog":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxTLShopBuyLog":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxMMProductionLog":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxCAItemCollectionMission": "尚未確定(小於500K) ALL ALL None None 1"
    , "FxCAItemCollectionRewardLog": "尚未確定(小於500K) ALL ALL None None 1"
    , "FxCAItemCollectionUserItem": "尚未確定(大於1M) ALL ALL None None 1"

    # Log file, filtered by logdate
    , "FxMMResolutionLog":"尚未確定(大於1G) LOG LOG logtime logtime 1"

    # Big file, filtered by key
    , "FxUserDailyMissionLog":"尚未確定(大於100M) user_name userid None None 1 bnb_extract.init_activeaccount None None"
    , "FxUserPurchaseAnalysis":"尚未確定(大於100M) user_name userid None None 1 bnb_extract.init_activeaccount None None"
    , "FxUserInventory":"紀錄玩家身上物品(大於1G) user_name userid None None 1 bnb_extract.init_activeaccount None None"
    , "FxPreInventory":"紀錄玩家所有未收下或拒絕的禮物(大於1G) id userid None None 1 bnb_extract.init_activeaccount None None"

    # Exclude File
    , "FxTranx_20180101_2":"尚未確定(大於100M) EXCLUDE EXCLUDE None None 1"

}
gamename = "bnb"
dbnname = "UserInventory"
startdate = "2019-12-01"
enddate = None
noFilterNameArray= [
    "_bk","_bak","_backup"
    ,"_tmp","_temp","temp"
    ,"old"
    ,"_201","_202"
    ,"jerry_accountlist", "gskillslot2_new" , "gskillslot3_new"
    ,"gitem_140128", "gitem_60006351",
]

#==================== 檔案製作 ====================

objectDF = extracthCtrl.makeExecuteCSV(gamename,dbnname,startdate,enddate,tdnWorldDF,tableCheck,noFilterNameArray)
objectDF.to_csv("file/bnb_UserInventory_extracttable.csv", index=False, encoding="utf_8_sig")
print(objectDF)




