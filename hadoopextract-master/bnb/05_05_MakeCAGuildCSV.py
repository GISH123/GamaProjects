import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option('display.max_rows', None)
pandas.set_option("display.width",2000)

extracthCtrl = ExtracthCtrl()


#==================== 檔案設定 ====================

tdnWorldDF = pandas.read_csv("file/bnb_CAGuild_table.csv")
tableCheck = {
    # Transfer ALL, Not filtered
    "FxGuildAccount1113":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxGuildChangeNameHistory":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxGuildDeleted":"公會刪除情報(大於10M) ALL ALL None None 1"
    , "FxGuildMessage":"尚未確定(0M) ALL ALL None None 1"
    , "FxGuildNotice":"公會公告討論版(大於10M) ALL ALL None None 1"
    , "FxGuildUserAccount1113":"尚未確定(大於10M) ALL ALL None None 1"
    , "FxGuildUserQuestLog":"公會任務(0M) ALL ALL None None 1"
    , "FxPreGuildUserAccount":"玩家加入公會時的第二層條件內容限制(大於10M) ALL ALL None None 1"
    , "FxPreGuildUserAccount1113":"尚未確定(大於1M) ALL ALL None None 1"
    , "GA0206":"尚未確定(小於500K) ALL ALL None None 1"
    , "GUA0206":"尚未確定(小於500K) ALL ALL None None 1"
    , "MemberNum1113":"尚未確定(小於500K) ALL ALL None None 1"
    , "LogGuildCreated": "尚未確定(小於500K) ALL ALL None None 1"
    , "LogGuildMasterChanged": "公會會長變更紀錄(大於1M) ALL ALL None None 1"
    , "LogUserGPFailed": "尚未確定(大於1M) ALL ALL None None 1"
    , "FxGuildQuestLog": "尚未確定(小於500K) ALL ALL None None 1"
    , "FxGuildUserAccount": "公會會員情報(大於100M) ALL ALL None None 1"

    # Log file, filtered by logdate
    , "FxGuildBBS": "公會討論版(大於1G) LOG LOG datecreated datecreated 1"

    # Big file, filtered by key
    , "FxGuildUserDeleted": "尚未確定(大於100M) user_name id None None 1 bnb_extract.init_activeaccount None None"
    , "FxGuildAccount":"公會情報(大於100M) user_name name None None 1 bnb_extract.init_activeaccount None None"
}
gamename = "bnb"
dbnname = "CAGuild"
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
objectDF.to_csv("file/bnb_CAGuild_extracttable.csv", index=False, encoding="utf_8_sig")
print(objectDF)




