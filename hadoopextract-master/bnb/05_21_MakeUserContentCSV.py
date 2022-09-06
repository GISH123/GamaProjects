import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option('display.max_rows', None)
pandas.set_option("display.width",2000)

extracthCtrl = ExtracthCtrl()


#==================== 檔案設定 ====================

tdnWorldDF = pandas.read_csv("file/bnb_UserContent_table.csv")
tableCheck = {
    # Transfer ALL, Not filtered
    "FxCAJJangPenaltyHistory":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangPenaltyJobLog":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangRank":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangRankHistory":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangRankRewardUserList":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangResetRankUserList":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangWebRank":"尚未確定(0M) ALL ALL None None 1"
    , "FxCAJJangWebRankFinal":"尚未確定(0M) ALL ALL None None 1"
    , "FxDailyMissionPoint":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxFishingRank":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxFishingRankConfig":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxFishingRankInitConfig":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxFishingRankKing":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxFishingRankMonthKing":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxFishingRankReward":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxOlympicOverallRank":"尚未確定(0M) ALL ALL None None 1"
    , "FxOlympicRankInfo":"尚未確定(0M) ALL ALL None None 1"
    , "FxOlympicTeamPoint":"尚未確定(0M) ALL ALL None None 1"
    , "FxUserCAMissionNormalData":"尚未確定(0M) ALL ALL None None 1"
    , "RewardUserList_160516":"尚未確定(小於500K) ALL ALL None None 1"
    , "RewardUserList_160621":"尚未確定(大於1M) ALL ALL None None 1"
    , "RewardUserList_171019":"尚未確定(小於500K) ALL ALL None None 1"
    , "RewardUserList_2018Ladder":"尚未確定(小於500K) ALL ALL None None 1"
    , "RewardUserList_201904Ladder":"尚未確定(小於500K) ALL ALL None None 1"
    , "RewardUserList_2019Ladder":"尚未確定(小於500K) ALL ALL None None 1"
    , "SurveyUserList":"尚未確定(小於500K) ALL ALL None None 1"
    , "_20130801_FxOlympicOverallRank":"尚未確定(大於1M) ALL ALL None None 1"
    , "_20130801_FxOlympicRankInfo":"尚未確定(大於1M) ALL ALL None None 1"
    , "_20130801_FxOlympicTeamPoint":"尚未確定(小於500K) ALL ALL None None 1"
    , "_20130808_FxOlympicOverallRank":"尚未確定(大於1M) ALL ALL None None 1"
    , "_20130808_FxOlympicRankInfo":"尚未確定(大於1M) ALL ALL None None 1"
    , "_20130808_FxOlympicTeamPoint":"尚未確定(小於500K) ALL ALL None None 1"
    , "_20130815_FxOlympicOverallRank":"尚未確定(大於1M) ALL ALL None None 1"
    , "_20130815_FxOlympicRankInfo":"尚未確定(大於1M) ALL ALL None None 1"
    , "_20130815_FxOlympicTeamPoint":"尚未確定(小於500K) ALL ALL None None 1"
    , "_20130822_FxOlympicOverallRank":"尚未確定(大於1M) ALL ALL None None 1"
    , "_20130822_FxOlympicRankInfo":"尚未確定(大於1M) ALL ALL None None 1"
    , "_20130822_FxOlympicTeamPoint":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxQuestLogForWeb": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxFishingRankRewardLog": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxFishingRankTotalLog": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxFishingRankLog": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxCAMissionRndSelectLog": "尚未確定(小於500K) ALL ALL None None 1"
    , "FxFishingItemHistory": "尚未確定(小於500K) ALL ALL None None 1"

    # Log file, filtered by logdate

    , "FxCafeRewardLog": "尚未確定(大於100M) LOG LOG logtime logtime 1"
    , "FxFishingItemHistoryLog": "釣魚系統釣到水球相關紀錄(大於100M) LOG LOG logtime logtime 1"

    # Big file, filtered by key
}
gamename = "bnb"
dbnname = "UserContent"
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
objectDF.to_csv("file/bnb_UserContent_extracttable.csv", index=False, encoding="utf_8_sig")
print(objectDF)




