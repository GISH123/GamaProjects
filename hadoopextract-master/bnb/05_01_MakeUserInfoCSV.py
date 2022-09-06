import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option('display.max_rows', None)
pandas.set_option("display.width",2000)

extracthCtrl = ExtracthCtrl()


#==================== 檔案設定 ====================

tdnWorldDF = pandas.read_csv("file/bnb_UserInfo_table.csv")
tableCheck = {
    # Transfer ALL, Not filtered
    "FallData01":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxBlacklist":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCouple":"尚未確定(0M) ALL ALL None None 1"
    , "FxKartEvent":"尚未確定(0M) ALL ALL None None 1"
    , "FxRegulation":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxSchoolZzang":"尚未確定(0M) ALL ALL None None 1"
    , "FxSpeakerLog":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxUserBlocking":"尚未確定(0M) ALL ALL None None 1"
    , "FxUserCAMissionDailyData":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxUserCAMissionNormalData":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxUserComments":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxUserConn":"尚未確定(大於10M) ALL ALL None None 1"
    , "GLoveFlower01":"尚未確定(小於500K) ALL ALL None None 1"
    , "Log_Password":"尚未確定(大於10M) ALL ALL None None 1"
    , "LoveCountry01":"尚未確定(小於500K) ALL ALL None None 1"
    , "LoveFlower01":"尚未確定(小於500K) ALL ALL None None 1"
    , "Pin01":"尚未確定(小於500K) ALL ALL None None 1"
    , "Pingu01":"尚未確定(小於500K) ALL ALL None None 1"
    , "badnickname":"尚未確定(小於500K) ALL ALL None None 1"
    , "fall01":"尚未確定(小於500K) ALL ALL None None 1"
    , "homerun01":"尚未確定(小於500K) ALL ALL None None 1"
    , "mission":"尚未確定(小於500K) ALL ALL None None 1"

    # Log file, filtered by logdate

    # Big file, filtered by key
    , "FxUserAccount":"紀錄所有玩家帳號(密碼紀錄於GASH端,GASH端同時擁有帳號與密碼)(大於100M) id id None None 1 bnb_extract.init_activeaccount None None"
}
gamename = "bnb"
dbnname = "UserInfo"
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
objectDF.to_csv("file/bnb_UserInfo_extracttable.csv", index=False, encoding="utf_8_sig")
print(objectDF)




