import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option('display.max_rows', None)
pandas.set_option("display.width",2000)

extracthCtrl = ExtracthCtrl()


#==================== 檔案設定 ====================

tdnWorldDF = pandas.read_csv("file/bnb_CARankey_table.csv")
tableCheck = {
    # Transfer ALL, Not filtered
    "CA_Rank":"尚未確定(0M) ALL ALL None None 1"
    , "CA_Rank_GameType":"尚未確定(0M) ALL ALL None None 1"
    , "CA_Rank_InSchool":"尚未確定(0M) ALL ALL None None 1"
    , "CA_Rank_Ladder":"尚未確定(0M) ALL ALL None None 1"
    , "CA_Rank_School":"尚未確定(0M) ALL ALL None None 1"
    , "CA_Rank_Theme":"尚未確定(0M) ALL ALL None None 1"
    , "ExceptID":"尚未確定(0M) ALL ALL None None 1"
    , "FxUserDynamic":"尚未確定(0M) ALL ALL None None 1"
    , "LevelMap":"尚未確定(0M) ALL ALL None None 1"
    , "Rank_IDListToConvert":"尚未確定(0M) ALL ALL None None 1"
    , "SchoolCode":"尚未確定(0M) ALL ALL None None 1"
    , "Sys_Config":"尚未確定(0M) ALL ALL None None 1"
    , "Sys_ExceptID":"尚未確定(0M) ALL ALL None None 1"
    , "Sys_ExceptSchool":"尚未確定(0M) ALL ALL None None 1"
    , "Sys_LevelMap":"尚未確定(0M) ALL ALL None None 1"
    , "Sys_RankDate":"尚未確定(0M) ALL ALL None None 1"
    , "Sys_SchoolCode":"尚未確定(0M) ALL ALL None None 1"
    , "TTT":"尚未確定(0M) ALL ALL None None 1"
    , "UserAccount":"尚未確定(0M) ALL ALL None None 1"

    # Log file, filtered by logdate

    # Big file, filtered by key
}
gamename = "bnb"
dbnname = "CARankey"
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
objectDF.to_csv("file/bnb_CARankey_extracttable.csv", index=False, encoding="utf_8_sig")
print(objectDF)




