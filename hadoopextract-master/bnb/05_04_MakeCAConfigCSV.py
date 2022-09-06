import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option('display.max_rows', None)
pandas.set_option("display.width",2000)

extracthCtrl = ExtracthCtrl()


#==================== 檔案設定 ====================

tdnWorldDF = pandas.read_csv("file/bnb_CAConfig_table.csv")
tableCheck = {
    # Transfer ALL, Not filtered
    "BillingConfig":"尚未確定(小於500K) ALL ALL None None 1"
    , "BillingSvr":"尚未確定(0M) ALL ALL None None 1"
    , "BillingSvr2":"尚未確定(小於500K) ALL ALL None None 1"
    , "CADBConfig":"尚未確定(0M) ALL ALL None None 1"
    , "CALoginSvr":"尚未確定(小於500K) ALL ALL None None 1"
    , "CAMultiSvr":"尚未確定(0M) ALL ALL None None 1"
    , "CAServerList":"尚未確定(0M) ALL ALL None None 1"
    , "FXLSConfigTable":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAdminPermissions":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAdminPermissionsRenew":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAdvertisement":"尚未確定(0M) ALL ALL None None 1"
    , "FxAdvertisement0310":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAntarcticRank":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxBadProcess":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAItemCollectionCategory":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCAItemCollectionEventList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCAItemCollectionItemIdx":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCAItemCollectionNormalList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCAItemCollectionRewardList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCAItemCollectionSubCategory":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCAJJangBalloonPoint":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangPenaltyConfig":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangRewardInfo":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangRewardList":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangSeasonInfo":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangSeasonSchedule":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangSectionPoint":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAMissionDailyConfig":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCAMissionNormalConfig":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCAMissionOption":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCAMissionSetting":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCaptionList":"尚未確定(0M) ALL ALL None None 1"
    , "FxChatEmotion":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxClientHashInfo":"client端的hashinformation(小於500K) ALL ALL None None 1"
    , "FxClientString":"遊戲中玩家收到的各項訊息(小於500K) ALL ALL None None 1"
    , "FxClientVersionInfo":"client透過比對版號決定是否更新版本或連線(小於500K) ALL ALL None None 1"
    , "FxColoringPattern":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxConfusedFightRank":"尚未確定(0M) ALL ALL None None 1"
    , "FxConfusedFightRankRenew":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCurseFilterString":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxDTTConfig":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxDTTLockCosts":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxDTTSlotConfig":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxDTTSlotGrade":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxDailyMissionConfig":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxDailyMissionDateSchedule":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxDailyMissionSchema":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxDailyMissionWeekSchedule":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxDynamicImg":"遊戲中所有廣宣的圖素設定(小於500K) ALL ALL None None 1"
    , "FxDynamicMapInfo":"遊戲中可使用的所有地圖設定(小於500K) ALL ALL None None 1"
    , "FxDynamicMapList":"遊戲進行時,可供玩家選擇的地圖列表(小於500K) ALL ALL None None 1"
    , "FxEventContents":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxEvents":"活動專用table(小於500K) ALL ALL None None 1"
    , "FxFishingBait":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxFishingLocation":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxFishingReward":"釣魚道具設定(小於500K) ALL ALL None None 1"
    , "FxFishingRewardFish":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxFishingReward_20190328":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxFishingRod":"釣竿設定(小於500K) ALL ALL None None 1"
    , "FxGSAdm":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxGetDifferentLucci":"尚未確定(0M) ALL ALL None None 1"
    , "FxGlobalButtonConfig":"工具列轉蛋按鍵的露出(小於500K) ALL ALL None None 1"
    , "FxGuildServerList":"尚未確定(0M) ALL ALL None None 1"
    , "FxHCMList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxHackAuthenIP":"尚未確定(0M) ALL ALL None None 1"
    , "FxHiddenCatchRankCalcInfo":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxHiddenCatchStageInfo":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxHiddenCatchStageInfoSU":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxItemSkill":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxLadderRestrictIP":"尚未確定(0M) ALL ALL None None 1"
    , "FxMatchingCommonConfig":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMatchingConfig":"尚未確定(0M) ALL ALL None None 1"
    , "FxMatchingFailedConfig":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMatchingMapList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMessage":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxQuestConfig":"任務設定列表(小於500K) ALL ALL None None 1"
    , "FxQuestString":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxRPGMiniMapInfo":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxRPGMiniMapList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxRPGShowMapInfo":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxRankMap":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxRewardLimitSetting":"尚未確定(0M) ALL ALL None None 1"
    , "FxSchoolZzang":"尚未確定(0M) ALL ALL None None 1"
    , "FxServerChannelName":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxServerConfigGroup":"頻道相關設定(小於500K) ALL ALL None None 1"
    , "FxServerConfigTable":"頻道相關設定:驚喜蛋在此設定(小於500K) ALL ALL None None 1"
    , "FxServerList":"列出目前現有的使用頻道(小於500K) ALL ALL None None 1"
    , "FxServerString":"活動及任務進行中回應給玩家的系統訊息(小於500K) ALL ALL None None 1"
    , "FxSquareMap":"尚未確定(0M) ALL ALL None None 1"
    , "FxStageMap":"地圖列表(小於500K) ALL ALL None None 1"
    , "FxTLShopConfig":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxTLShopRefreshProb":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxTLShopRefreshSchedule":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxTieInSaleInfo":"尚未確定(0M) ALL ALL None None 1"
    , "FxTitleContentList":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxUserOptions":"尚未確定(0M) ALL ALL None None 1"
    , "FxUserSystemInfo":"尚未確定(0M) ALL ALL None None 1"
    , "ItemHash":"尚未確定(0M) ALL ALL None None 1"
    , "PlayLog_SMS":"尚未確定(0M) ALL ALL None None 1"
    , "QMarkAllowedItemList":"尚未確定(小於500K) ALL ALL None None 1"
    , "TournamentSettings":"尚未確定(小於500K) ALL ALL None None 1"
    , "TournamentTimer":"尚未確定(小於500K) ALL ALL None None 1"
    , "TournamentTimer_20090723":"尚未確定(小於500K) ALL ALL None None 1"
    , "WebNotice":"尚未確定(0M) ALL ALL None None 1"
    , "WhiteProcessesLists":"尚未確定(0M) ALL ALL None None 1"
    , "_CACommuSvr":"尚未確定(0M) ALL ALL None None 1"

    # Log file, filtered by logdate

    # Big file, filtered by key
}
gamename = "bnb"
dbnname = "CAConfig"
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
objectDF.to_csv("file/bnb_CAConfig_extracttable.csv", index=False, encoding="utf_8_sig")
print(objectDF)




