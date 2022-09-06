import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option('display.max_rows', None)
pandas.set_option("display.width",2000)

extracthCtrl = ExtracthCtrl()


#==================== 檔案設定 ====================

tdnWorldDF = pandas.read_csv("file/bnb_LadderInfo_table.csv")
tableCheck = {
    # Transfer ALL, Not filtered
    "FxAllLadderDynamic": "尚未確定(0M) ALL ALL None None 1"
    , "FxAllLadderDynamicOld": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxAllLadderDynamicOld20130627": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxAllLadderDynamicOld20130926": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxAllLadderRank": "尚未確定(0M) ALL ALL None None 1"
    , "FxAllLadderRankOld": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxAllLadderRankOld20130627": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxAllLadderRankOld20130926": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxDropWPConfig": "尚未確定(0M) ALL ALL None None 1"
    , "FxGradeConfig": "尚未確定(0M) ALL ALL None None 1"
    , "FxLadderDynamic": "玩家人物段位相關資料(大於1M) ALL ALL None None 1"
    , "FxLadderDynamic190704": "尚未確定(小於500K) ALL ALL None None 1"
    , "FxLadderDynamic190930": "尚未確定(小於500K) ALL ALL None None 1"
    , "FxLadderDynamic20130627": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxLadderDynamic20130926": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxLadderDynamic20181004": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxLadderDynamic20190103": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxLadderDynamic20190403": "尚未確定(小於500K) ALL ALL None None 1"
    , "FxLadderGenerated": "尚未確定(0M) ALL ALL None None 1"
    , "FxLadderPenalty": "尚未確定(0M) ALL ALL None None 1"
    , "FxLadderRankChanges": "尚未確定(0M) ALL ALL None None 1"
    , "FxLadderRankOld2": "尚未確定(0M) ALL ALL None None 1"
    , "FxLadderRankUpdated": "尚未確定(小於500K) ALL ALL None None 1"
    , "FxLadderSeasonInfo": "段位賽季節列表(小於500K) ALL ALL None None 1"
    , "FxLadderSeasonInfo2": "尚未確定(小於500K) ALL ALL None None 1"
    , "fxLadderHighRank": "尚未確定(0M) ALL ALL None None 1"
    , "FxLadderDynamicResetLog": "尚未確定(大於1M) ALL ALL None None 1"

    # Log file, filtered by logdate

    # Big file, filtered by key
    , "FxLadderRankOld20130627": "尚未確定(大於100M) user_name userid None None 1 bnb_extract.init_activeaccount None None"
    , "FxLadderRankOld": "尚未確定(大於100M) user_name userid None None 1 bnb_extract.init_activeaccount None None"
}
gamename = "bnb"
dbnname = "LadderInfo"
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
objectDF.to_csv("file/bnb_LadderInfo_extracttable.csv", index=False, encoding="utf_8_sig")
print(objectDF)




