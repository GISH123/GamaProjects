import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option('display.max_rows', None)
pandas.set_option("display.width",2000)

extracthCtrl = ExtracthCtrl()


#==================== 檔案設定 ====================

tdnWorldDF = pandas.read_csv("file/bnb_CAItem_table.csv")
tableCheck = {
    # Transfer ALL, Not filtered
    "CAItem_Desc":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxBasketInfo":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxBattlePoint":"尚未確定(0M) ALL ALL None None 1"
    , "FxBattleRewardItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxBattleRewardProb":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxBonusMap":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxBonusMapRewardItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxBundleGachapon":"水球箱道具數量(小於500K) ALL ALL None None 1"
    , "FxBundleGachaponWinnerList":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxCategories":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCategories2":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCoupleAnniversaryPresent":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCouponMap":"尚未確定(0M) ALL ALL None None 1"
    , "FxDTTGachaponItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxDTTPointRewardItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxDTTRemainLimitedItem":"尚未確定(0M) ALL ALL None None 1"
    , "FxDailyMissionStock":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxElecDisplayColorInfo":"尚未確定(0M) ALL ALL None None 1"
    , "FxExItemMap":"尚未確定(0M) ALL ALL None None 1"
    , "FxForceDeleteItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxGCouponAddAmount":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxGachaponBonusGaugeItem":"尚未確定(0M) ALL ALL None None 1"
    , "FxGachaponItemDisplay":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxGachaponItemList":"轉蛋、樂暴卡活動的概率表(大於1M) ALL ALL None None 1"
    , "FxGachaponItemListSet":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxItem":"遊戲中出現的所有道具資料(小於500K) ALL ALL None None 1"
    , "FxItemExtraAttr":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxItemExtraAttrmMap":"尚未確定(0M) ALL ALL None None 1"
    , "FxItemHistory":"尚未確定(0M) ALL ALL None None 1"
    , "FxItemNew":"尚未確定(0M) ALL ALL None None 1"
    , "FxLimitedItem":"尚未確定(0M) ALL ALL None None 1"
    , "FxLimitedItemWinnerList":"尚未確定(0M) ALL ALL None None 1"
    , "FxLimitedMagicBoxItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxLobbySquareDisplayItem":"尚未確定(0M) ALL ALL None None 1"
    , "FxMMProductionList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMMProductionReward":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMMProductionStuffList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMMResolutionExceptList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMMResolutionMenu":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMMResolutionReward":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMMResolutionRewardProb":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMMSynthesisExceptList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMMSynthesisReward":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMMSynthesisRewardProb":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMagicBookStock":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMagicBoxItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxNoTimeStopItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxOfflinePrizeList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxOfflinePrizeWinnerList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxPremiumList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxPresentList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxQCouponMap":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxRPGItemUpgrade":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxRPGItemUpgradeMaterial":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxRPGResellItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxRPGStatOverrideItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxRentItem":"尚未確定(0M) ALL ALL None None 1"
    , "FxRewardItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxRewardItemRenew":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxRewardLevelTable":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxRpgItemStat":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxSetItem":"尚未確定(0M) ALL ALL None None 1"
    , "FxShopCategory":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxShopItemInfo":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxStatOverrideItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxStockChangeLog":"尚未確定(0M) ALL ALL None None 1"
    , "FxStockNew":"尚未確定(0M) ALL ALL None None 1"
    , "FxStockReal":"尚未確定(0M) ALL ALL None None 1"
    , "FxString":"所有商品的說明(小於500K) ALL ALL None None 1"
    , "FxTLShopLimitedItem":"尚未確定(0M) ALL ALL None None 1"
    , "FxTLShopStock":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxTimeGiftItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "ShopOrderLog":"尚未確定(小於500K) ALL ALL None None 1"
    , "SpecialTournamentReward":"尚未確定(小於500K) ALL ALL None None 1"
    , "SpecialTournamentWinnerRecord":"尚未確定(小於500K) ALL ALL None None 1"
    , "__FxItem_2018_07_01":"尚未確定(小於500K) ALL ALL None None 1"
    , "__FxStock_2018_07_01":"尚未確定(小於500K) ALL ALL None None 1"
    , "fxFxpackagemap0110":"尚未確定(小於500K) ALL ALL None None 1"
    , "fxpackagemap":"套裝相關資料表(小於500K) ALL ALL None None 1"
    , "fxstock":"商城各項商品資料,代號對應資料表FxItem(小於500K) ALL ALL None None 1"
    , "fxstock_20123015":"尚未確定(小於500K) ALL ALL None None 1"

    # Log file, filtered by logdate


    # Big file, filtered by key
}
gamename = "bnb"
dbnname = "CAItem"
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
objectDF.to_csv("file/bnb_CAItem_extracttable.csv", index=False, encoding="utf_8_sig")
print(objectDF)




