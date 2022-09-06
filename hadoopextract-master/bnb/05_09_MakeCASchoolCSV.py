import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option('display.max_rows', None)
pandas.set_option("display.width",2000)

extracthCtrl = ExtracthCtrl()


#==================== 檔案設定 ====================

tdnWorldDF = pandas.read_csv("file/bnb_CASchool_table.csv")
tableCheck = {
    # Transfer ALL, Not filtered
    "MajorCode":"尚未確定(0M) ALL ALL None None 1"
    , "SchoolCode":"尚未確定(0M) ALL ALL None None 1"
    , "SchoolCode_":"尚未確定(0M) ALL ALL None None 1"
    , "UnivMajorList":"尚未確定(0M) ALL ALL None None 1"
    , "tblSchool":"尚未確定(0M) ALL ALL None None 1"
    , "tblSchool_20050603":"尚未確定(0M) ALL ALL None None 1"

    # Log file, filtered by logdate

    # Big file, filtered by key
}
gamename = "bnb"
dbnname = "CASchool"
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
objectDF.to_csv("file/bnb_CASchool_extracttable.csv", index=False, encoding="utf_8_sig")
print(objectDF)




