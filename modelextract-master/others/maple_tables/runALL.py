import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
from package.common.database.sqlTool import SqlTool
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import datetime as dt

load_dotenv(dotenv_path="env/hive.env")
hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)
load_dotenv(dotenv_path="env/hdfs.env")
hdfsCtrl = HDFSCtrl(
      url=os.getenv("HDFS_HOST")
    , user=os.getenv("HDFS_USER")
    , password=os.getenv("HDFS_PASSWD")
    , filePath=os.getenv("HDFS_PATH")
)


def getfileName(keyword, dirList):
    return [s for s in dirList if s.find(keyword) >0][0]

def getFileDF(path, fileName , sheetNo = 0 ):
    filePath = path + '/' +fileName
    if fileName.split('.')[-1] in ('xlsx', 'xls'):
        df = pd.read_excel(filePath,sheet_index = sheetNo )
    else:
        df = pd.read_csv(filePath)
    return df

def getNewCol(tableName):
    
    colList =  [f'c{i+1}' for i in range(15)] +\
        [f'u{i+1}' for i in range(15)] +\
        [f's{i+1}' for i in range(20)] +\
        [f'd{i+1}' for i in range(20)] +\
        [f't{i+1}' for i in range(3)] +\
        [f'o{i+1}' for i in range(10)] 
    replacement ={}
    if tableName == 'commodity':
        return  ['c1','c2','c3','c4','c5',
                   'c6','table_version','ItemID','Name','SN',
                   'c11','c12','c13','c14','c15',
                   
                   'Count','Price','Bonus','Period','Priority',
                   'reqPOP', 'ReqLEV', 'Gender', 'OnSale', 'MaplePoint',
                   'Meso', 'Premium', 'Class', 'Limit', 'Status',
                   
                   'TermStart','TermEnd', 'BombSale', 'Forced\nCategory', 'Forced\nSubCategory', 
                   'GameWorld(구분자:"/")', 'LimitMax','LimitQuestID', 'OriginalPrice', 'Discount', 
                   'MileageRate', 'zero', 'CouponType', 'onlyMileage', 'ShowDiscount', 
                   'update ver', 'comment', 's18', 's19', 's20'
                   
                   'd1','d2','d3','d4','d5',
                   'd1','d7','d8','d9','d10',
                   'd11','d12','d13','d14','d15',
                   'd11','d17','d18','d19','d20',
                   'T1','T2', 'T3',
                   
                   '報表分類', '扣點分類', '上市時間', '大分類', '小分類',
                   '備註', 'Price.1', '活動', 'season', 'o10' ] 
    if tableName == 'itemID':
        replacement = {'u1' : 'ItemID', 
                       's1' : 'ItemName', 
                       'c7' :'table_version',
                       'c8' : 'ItemID', 
                       'c9' : 'ItemName'}
        
    if tableName == 'fashionBox':
        replacement = { 'c7' :'table_version',
                        'c8' : 'ItemID', 
                        'c9' : 'Name', 
                        'c10': 'Part_cht',
                        'd1' : 'Prob.',
                        't1' : 'Date',
                        't2' : 'endDate',
                        's1' : '大師標籤',     's2' : 'IP合作',   's3' : '漂浮特效',   's4' : '暗色系',   's5' : '螢光系', 
                        's6' : '柔和色系',     's7' : '可愛風格',   's8' : '制服風格',   's9' : '貴族風格',   's10' : '動物風格',  
                        's11' : '怪物風格',     's12' : '角色風格',   's13' : '自然風格',   's14' : '重裝風格',   's15' : '運動風格', 
                        's16' : '搞怪風格',     's17' : '不擋身/腿',   's18' : '不擋住名字',   's19' : '渲染特效', 
                        'o1': 'KR Name',
                        'o2': 'Part'
                       
                       }

    return [ replacement.get(n,n) for n in  colList]

def uploadTable(tableName,outputFileName):
    game = 'maple'
    
    tableNumberDict = {'commodity':  '16091',
                       'itemID':  '13091',
                       'fashionBox':  '16092',
                       }
    tablenumber = tableNumberDict[tableName]
    d_str = dt.datetime.now().strftime('%Y%m%d')
    
    hdfsdPath = f'/user/GTW_PD/DB/ModelExtract/modelextract/game={game}/dt={d_str}/world=COMMON/tablenumber={tablenumber}/'
    hdfsCtrl.upload(hdfsdPath + tableName,outputFileName)
    
    
    partition = f"game='{game}', dt='{d_str}', world = 'COMMON', tablenumber = '{tablenumber}'"
    dropPartitionSql = f"ALTER TABLE gtwpd.modelextract_modelextract DROP IF EXISTS PARTITION ({partition})"
    alterPartitionSql = f"ALTER TABLE gtwpd.modelextract_modelextract ADD IF NOT EXISTS PARTITION ({partition}) LOCATION '{hdfsdPath}'"
    hiveCtrl.executeSQL(dropPartitionSql)
    hiveCtrl.executeSQL(alterPartitionSql)
    print(f"{tableName} have uploaded to {hdfsdPath} ")
    return


def CommdityMain(parametersData = {}):
    
    path = 'others/maple_tables/files/'
    outputPath = 'others/maple_tables/upload/'
    tableName = 'commodity'

    dirList = [dr.lower() for dr in os.listdir(path)]
    dir_names = max(dirList )
    fileList = [dr.lower() for dr in os.listdir( path + dir_names)]     
    sheetNo = 1 

    outputFileName = outputPath + f'{tableName}.csv'
        
    #找出commodity
    fileName = getfileName( 'commodity', dirList = fileList)
    df = getFileDF(path = path + dir_names, fileName =  fileName, sheetNo =  sheetNo )
    df['table_version'] = dir_names
    
    df_out = df.reindex(columns = getNewCol(tableName))
    
    df_out.to_csv(outputFileName,sep = '\t',header = None , index = None, float_format= '%.0f')    
    # 上傳
    uploadTable(tableName,outputFileName)

    

def ItemIDMain(parametersData = {}):
    
    path = 'others/maple_tables/files/'
    outputPath = 'others/maple_tables/upload/'    
    
    tableName = 'itemID'
    outputFileName = outputPath + f'{tableName}.csv'
    
    dirList = [dr.lower() for dr in os.listdir(path)]
    dir_names = max(dirList )
    fileList = [dr.lower() for dr in os.listdir( path + dir_names)] 
        
    #找出道具編號表
    fileName = getfileName('道具編號',fileList)
    sheetNo = 0 
    df = getFileDF(path = path + dir_names, fileName =  fileName, sheetNo =  sheetNo )
    df['table_version'] = dir_names
    
    df_out = df.reindex(columns = getNewCol(tableName))
    
    df_out.to_csv(outputFileName,sep = '\t',header = None , index = None, float_format= '%.0f')

    
    # 上傳
    uploadTable(tableName,outputFileName)

    
def FashionBoxMain(parametersData = {}):
    
    path = 'others/maple_tables/files/'
    outputPath = 'others/maple_tables/upload/'
    
    tableName = 'fashionBox'
    outputFileName = outputPath + f'{tableName}.csv'
    
    dirList = [dr.lower() for dr in os.listdir(path)]
    dir_names = max(dirList )
    fileList = [dr.lower() for dr in os.listdir( path + dir_names)] 
    
    
    
    fileName = getfileName('時裝',fileList)
    sheetNo = 0
    df = getFileDF(path = path + dir_names, fileName =  fileName, sheetNo =  sheetNo )
    df['table_version'] = dir_names
    
    
    dateTable = df['Date'].unique()

    dateDF= pd.DataFrame({'Date':dateTable,'endDate':list(dateTable[1:]) + [dateTable[-1] +np.timedelta64(14,'D')]})
    df['Date'].dtype
    
    df = pd.merge(df,dateDF,on ='Date')
    
    
    partTable = df['Part'].unique()
    partDF= pd.DataFrame({'Part':partTable ,'Part_cht':['帽子','連體衣','鞋子','披風','武器','臉飾','戒指','手套','眼飾','耳飾','披風']})
    df = pd.merge(df,partDF,on ='Part')
    
    
    df[df.columns[7:26]] = df[df.columns[7:26]].fillna(0).replace('V',1)
    df_out = df.reindex(columns = getNewCol(tableName))
    
    df_out.to_csv(outputFileName,sep = '\t',header = None , index = None,
                  float_format= '%.4f', date_format = '%Y-%m-%d %T')

    
    # 上傳
    uploadTable(tableName,outputFileName)

    
    
if __name__ == "__main__":
    CommdityMain()
    ItemIDMain()
    FashionBoxMain()
