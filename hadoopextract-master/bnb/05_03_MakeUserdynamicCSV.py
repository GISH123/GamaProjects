import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option('display.max_rows', None)
pandas.set_option("display.width",2000)

extracthCtrl = ExtracthCtrl()


#==================== 檔案設定 ====================

tdnWorldDF = pandas.read_csv("file/bnb_Userdynamic_table.csv")
tableCheck = {
    # Transfer ALL, Not filtered
    "FxAbuseCount":"尚未確定(0M) ALL ALL None None 1"
    , "FxBattlePoint":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxBookMarkMapList":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxCAJJangMatchLog":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangMatchUserInfo":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangPlacement":"尚未確定(0M) ALL ALL None None 1"
    , "FxCouple":"紀錄歷史結婚資料(大於10M) ALL ALL None None 1"
    , "FxCoupleRingName":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCpBonusRewardLimit":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxElecDisplayInfo":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxHCSingleStageInfo":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxHiddenCatchCoin":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxHiddenCatchSingleData":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxHiddenCatchSingleDataSU":"尚未確定(0M) ALL ALL None None 1"
    , "FxMatchingLog":"尚未確定(0M) ALL ALL None None 1"
    , "FxMatchingUserInfo":"尚未確定(0M) ALL ALL None None 1"
    , "FxMonRank":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxNormalMatchUserInfo":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxRewardLimit":"尚未確定(0M) ALL ALL None None 1"
    , "FxSpecialMark":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxTournamentDynamic":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxUserBasket":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxUserDTTInfo":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxUserGachaponBonusGauge":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxUserGachaponCoupon":"尚未確定(0M) ALL ALL None None 1"
    , "FxUserGachaponCoupon0113":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxUserGachaponCoupon20110908":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxUserGachaponCoupon20121220":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxUserGachaponUsedCount":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxUserHeartPoint":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxUserMarbleCoinUsedCount":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxUserSpendMoney":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxUserTLShopInfo":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxUserTLShopStock":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxUserTitle":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxUserTitleWord":"尚未確定(大於1M) ALL ALL None None 1"
    , "GMLog_UserDynamic":"尚未確定(0M) ALL ALL None None 1"
    , "GxSync":"尚未確定(0M) ALL ALL None None 1"
    , "LEVEL":"尚未確定(小於500K) ALL ALL None None 1"
    , "Log_Dynamic":"尚未確定(0M) ALL ALL None None 1"
    , "bir":"尚未確定(小於500K) ALL ALL None None 1"
    , "moneygift":"尚未確定(小於500K) ALL ALL None None 1"
    , "penguin":"尚未確定(小於500K) ALL ALL None None 1"
    , "snowmission":"尚未確定(小於500K) ALL ALL None None 1"
    , "zo":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxLevelUpRewardLog": "尚未確定(大於10M) ALL ALL None None 1"
    , "FxGameResultLog": "尚未確定(大於10M) ALL ALL None None 1"
    , "FxDynamicResetLog": "尚未確定(大於1M) ALL ALL None None 1"
    , "_FxUserMiscLog_20130801": "尚未確定(小於500K) ALL ALL None None 1"
    , "_FxUserMiscLog_20130808": "尚未確定(小於500K) ALL ALL None None 1"
    , "_FxUserMiscLog_20130815": "尚未確定(小於500K) ALL ALL None None 1"
    , "_FxUserMiscLog_20130822": "尚未確定(小於500K) ALL ALL None None 1"
    , "log": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxUserMiscLog": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxUserHeartPointLog": "尚未確定(大於10M) ALL ALL None None 1"
    , "FxUserDTTLog": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxEventItemRecord": "尚未確定(大於1M) ALL ALL None None 1"

    # Log file, filtered by logdate
    , "FxNormalMatchLog": "尚未確定(大於100M) LOG LOG logtime logtime 1"

    # Big file, filtered by key
    , "FxQuestLog": "記錄玩家進行任務狀況的資料(大於1G) user_name userid None None 1 bnb_extract.init_activeaccount None None"
    , "FxUserTitleList": "尚未確定(大於100M) user_name userid None None 1 bnb_extract.init_activeaccount None None"
    , "FxUserMapRank": "尚未確定(大於100M) user_name userid None None 1 bnb_extract.init_activeaccount None None"
    , "FxUserDynamic": "紀錄玩家角色資料(大於100M) id id None None 1 bnb_extract.init_activeaccount None None"
    , "FxPlayMapRecord": "尚未確定(大於100M) user_name userid None None 1 bnb_extract.init_activeaccount None None"
}
gamename = "bnb"
dbnname = "Userdynamic"
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
objectDF.to_csv("file/bnb_Userdynamic_extracttable.csv", index=False, encoding="utf_8_sig")
print(objectDF)




