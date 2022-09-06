import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option('display.max_rows', None)
pandas.set_option("display.width",2000)

extracthCtrl = ExtracthCtrl()


#==================== 檔案設定 ====================

tdnWorldDF = pandas.read_csv("file/bnb_LadderRank_table.csv")
tableCheck = {
    # Transfer ALL, Not filtered
    "FxAllLadderDynamic":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAllLadderDynamicOld":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAllLadderDynamicOld0107":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxAllLadderDynamicOld_20181218":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxAllLadderDynamic_20130815":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxAllLadderDynamic_20130822":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxAllLadderDynamic_20181220":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAllLadderDynamic_20190328":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAllLadderDynamic_20190627":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAllLadderDynamic_20190926":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAllLadderRank":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAllLadderRankOld":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAllLadderRankOld0107":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxAllLadderRankOld_20181218":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxAllLadderRank_20130815":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxAllLadderRank_20130822":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxAllLadderRank_20181220":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAllLadderRank_20190328":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAllLadderRank_20190627":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxAllLadderRank_20190926":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxLadderRankUpdatedold":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxLadderSeasonInfo":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxLadderSeasonInfo2":"尚未確定(小於500K) ALL ALL None None 1"
    , "fxLadderHighRank":"尚未確定(0M) ALL ALL None None 1"

    # Log file, filtered by logdate

    # Big file, filtered by key
    , "FxLadderMonthLog": "尚未確定(大於100M) user_name userid None None 1 bnb_extract.init_activeaccount None None"
}
gamename = "bnb"
dbnname = "LadderRank"
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
objectDF.to_csv("file/bnb_LadderRank_extracttable.csv", index=False, encoding="utf_8_sig")
print(objectDF)




