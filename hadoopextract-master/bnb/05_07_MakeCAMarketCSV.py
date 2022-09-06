import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.extracth.extracthCtrl import ExtracthCtrl
import pandas

pandas.set_option("display.max_columns", None)
pandas.set_option('display.max_rows', None)
pandas.set_option("display.width",2000)

extracthCtrl = ExtracthCtrl()


#==================== 檔案設定 ====================

tdnWorldDF = pandas.read_csv("file/bnb_CAMarket_table.csv")
tableCheck = {
    # Transfer ALL, Not filtered
    "FxCAPoint":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxCAPoint20131128":"尚未確定(大於1M) ALL ALL None None 1"
    , "FxCAPoint20140109":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxItem":"從商城道具copy過來的副本(小於500K) ALL ALL None None 1"
    , "FxLimitedMagicBoxItem":"尚未確定(0M) ALL ALL None None 1"
    , "FxMagicBookStock":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMagicBoxExcludeItem":"尚未確定(0M) ALL ALL None None 1"
    , "FxMagicBoxItem":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMarketSellMagicBooks":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMarketSellMagicBooks20131114":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxMarketSellMagicBooks20131226":"尚未確定(小於500K) ALL ALL None None 1"
    , "FxUserMagicBooks":"尚未確定(大於10M) ALL ALL None None 1"
    , "MarketErrorCode":"紀錄買賣錯誤時的LOG(小於500K) ALL ALL None None 1"
    , "MarketExcludeItem":"禁止拍賣的名單(此名單中的SN會配排程將CAMarket中FXItem的相同SN刪除(小於500K) ALL ALL None None 1"
    , "MarketItem":"商品的拍賣位置(小於500K) ALL ALL None None 1"
    , "MarketSellAllItems":"尚未確定(大於10M) ALL ALL None None 1"
    , "MarketSellItemStatus":"尚未確定(小於500K) ALL ALL None None 1"
    , "MarketSoldItemNotify":"尚未確定(大於1M) ALL ALL None None 1"
    , "MarketUser":"拍賣玩家資料,於第一次登入後,即會在此保留UserSN(並不會與UserAccount同步)(大於10M) ALL ALL None None 1"
    , "MarketBuyItemLog": "玩家(買方)購買紀錄(大於10M) ALL ALL None None 1"

    # Log file, filtered by logdate
    , "MarketUserLucciLog": "尚未確定(大於100M) LOG LOG datecreated datecreated 1"
    , "MarketErrorLog": "給MarketErrorLog對應SPErrNo的系統錯誤的訊息(大於10G) LOG LOG datecreated datecreated 1"
    , "FxMagicBookMarketBuyLog": "尚未確定(大於1M) LOG LOG datecreated datecreated 1"
    , "FxMagicBoxUseLog": "尚未確定(大於1M) LOG LOG logtime logtime 1"
    , "MarketSellItems": "玩家拍賣的商品登入(購買拍賣卷並登入後即會產生)(大於100M) LOG LOG datecreated datecreated 1"


    # Big file, filtered by key

}
gamename = "bnb"
dbnname = "CAMarket"
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
objectDF.to_csv("file/bnb_CAMarket_extracttable.csv", index=False, encoding="utf_8_sig")
print(objectDF)




