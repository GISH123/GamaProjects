from package.common.database.mssqlCtrl import MSSQLCtrl
from dotenv import load_dotenv
import datetime
import os

load_dotenv(dotenv_path="env\GAME_BAK_INFO.env")

def Main() :
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeTime = nowZeroTime
    makeTimeNoLineStr = makeTime.strftime("%Y%m%d")
    MakeTableInfo("10.10.99.146","1433","bnbbd",os.getenv("MAIN_RESTORE_PASSWORD"),"{}_CAConfig".format(makeTimeNoLineStr),"bnb_CAConfig")
    MakeTableInfo("10.10.99.146","1433","bnbbd",os.getenv("MAIN_RESTORE_PASSWORD"),"{}_CAGuild".format(makeTimeNoLineStr),"bnb_CAGuild")
    MakeTableInfo("10.10.99.146","1433","bnbbd",os.getenv("MAIN_RESTORE_PASSWORD"),"{}_CAItem".format(makeTimeNoLineStr),"bnb_CAItem")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_CAMarket".format(makeTimeNoLineStr), "bnb_CAMarket")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_CARankey".format(makeTimeNoLineStr), "bnb_CARankey")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_CASchool".format(makeTimeNoLineStr), "bnb_CASchool")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_CATaiwanStat".format(makeTimeNoLineStr), "bnb_CATaiwanStat")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_CAUtils".format(makeTimeNoLineStr), "bnb_CAUtils")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_FxMessage".format(makeTimeNoLineStr), "bnb_FxMessage")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_LadderInfo".format(makeTimeNoLineStr), "bnb_LadderInfo")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_LadderRank".format(makeTimeNoLineStr), "bnb_LadderRank")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_RPGDynamic".format(makeTimeNoLineStr), "bnb_RPGDynamic")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_RPGInventory".format(makeTimeNoLineStr), "bnb_RPGInventory")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_ServerLogDB".format(makeTimeNoLineStr), "bnb_ServerLogDB")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_TranxLog".format(makeTimeNoLineStr), "bnb_TranxLog")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_UserContent".format(makeTimeNoLineStr), "bnb_UserContent")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_UserEditLog".format(makeTimeNoLineStr), "bnb_UserEditLog")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_WebGuild".format(makeTimeNoLineStr), "bnb_WebGuild")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_UserInfo01".format(makeTimeNoLineStr), "bnb_UserInfo")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_UserInventory00".format(makeTimeNoLineStr), "bnb_UserInventory")
    MakeTableInfo("10.10.99.146", "1433", "bnbbd", os.getenv("MAIN_RESTORE_PASSWORD"),"{}_Userdynamic03".format(makeTimeNoLineStr), "bnb_Userdynamic")

def MakeTableInfo(host,port,user,password,db,xlsname):
    print("{} {} {} {} {}".format(host,port,user,password,db,xlsname))
    mssqlCtrl = MSSQLCtrl(host, port, user,password,db)
    tableDF = mssqlCtrl.searchSQL(getDBTableSizeSql())
    tableDF.to_csv("file/"+xlsname+"_table_size.csv", encoding="utf-8-sig", index=0)

def getDBTableSizeSql():
    return """
    SELECT 
        s.Name AS schemaname
        , t.NAME AS tablename
        , MAX(p.rows) AS rowcounts
        , SUM(a.total_pages) * 8 /1024 AS totalspacemb
        , SUM(a.used_pages) * 8 /1024 AS usedspacemb
        , (SUM(a.total_pages) - SUM(a.used_pages)) * 8 /1024 AS unusedspacemb
    FROM sys.tables t
    INNER JOIN sys.indexes i ON t.OBJECT_ID = i.object_id
    INNER JOIN sys.partitions p ON i.object_id = p.OBJECT_ID AND i.index_id = p.index_id
    INNER JOIN sys.allocation_units a ON p.partition_id = a.container_id
    LEFT OUTER JOIN sys.schemas s ON t.schema_id = s.schema_id
    WHERE 1 = 1
        AND t.NAME NOT LIKE 'dt%' 
        AND t.is_ms_shipped = 0
        AND i.OBJECT_ID > 255 
    GROUP BY 
        t.Name
        , s.Name
    ORDER BY 
        CASE 
            WHEN t.NAME in ('[:TableNameStr]') THEN 0
            ELSE 1
        END
        , totalspacemb ASC
    """

if __name__ == "__main__":
    Main()