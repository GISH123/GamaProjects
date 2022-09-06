import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option('display.max_rows', None)
pandas.set_option("display.width",2000)

extracthCtrl = ExtracthCtrl()


#==================== 檔案設定 ====================

tdnWorldDF = pandas.read_csv("file/bnb_CATaiwanStat_table.csv")
tableCheck = {
    # Transfer ALL, Not filtered
    "bnb_ccu":"尚未確定(小於500K) ALL ALL None None 1"
    , "bnb_ccu_month":"尚未確定(小於500K) ALL ALL None None 1"
    , "bnb_ccu_week":"尚未確定(小於500K) ALL ALL None None 1"
    , "user_stat":"尚未確定(大於1M) ALL ALL None None 1"
    , "user_stat_day":"尚未確定(小於500K) ALL ALL None None 1"

    # Log file, filtered by logdate

    # Big file, filtered by key
}
gamename = "bnb"
dbnname = "CATaiwanStat"
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
objectDF.to_csv("file/bnb_CATaiwanStat_extracttable.csv", index=False, encoding="utf_8_sig")
print(objectDF)




