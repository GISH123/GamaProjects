import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
from package.common.database.hiveCtrl import HiveCtrl
from dotenv import load_dotenv
import pandas , random
import re

load_dotenv(dotenv_path="env/hive.env")
hiveCtrl = HiveCtrl(
    host=os.getenv("HIVE_HOST")
    , port=int(os.getenv("HIVE_PORT"))
    , user=os.getenv("HIVE_USER")
    , password=os.getenv("HIVE_PASS")
    , database='default'
    , auth_mechanism='PLAIN'
)

pandas.set_option('display.max_columns', None)
pandas.set_option('display.max_rows', None)
pandas.set_option('display.width', 1000)
cloumnStringArr =[
    ['end', 'end_']
    ,['world', 'world_']
    ,['dt', 'dt_']
    ,['map', 'map_']
]
dataTypeArr =[
    ['date', 'date']
    ,['datetime', 'timestamp']
    ,['smalldatetime','timestamp']
    ,['text','String']
    ,['nchar','String']
    ,['nvarchar','String']
    ,['ntext','String']
    ,['varbinary','String']
    ,['varchar','String']
    ,['image','String']
    ,['binary','String']
    ,['bit','String']
    ,['char','String']
    ,['uniqueidentifier','String']
    ,['tinyint','int']
    ,['int','int']
    ,['smallint','int']
    ,['bigint','bigint']
    ,['smallmoney','bigint']
    ,['money','bigint']
    ,['float','decimal']
    ,['real','decimal']
    ,['decimal','decimal']
    ,['numeric','decimal']
]

def  PartitionMake(inputXlsName,gameWorldArrMap,dateStrArrMap,tableFilterArray,alterPartitionCodeInit,dropPartitionCodeInit,schmeaNameInit,filePathInit,partitionedInit):
    runList = []
    tableDataFrame = pandas.read_csv("file/maple/info_tables/" + inputXlsName + "_table.csv")
    dataFrameTableFilter = tableDataFrame
    for tableFilter in tableFilterArray:
        dataFrameTableMask = dataFrameTableFilter[tableFilter[0]].str.contains(tableFilter[1])
        if tableFilter[2] == True:
            dataFrameTableFilter = dataFrameTableFilter[dataFrameTableMask]
        else:
            dataFrameTableFilter = dataFrameTableFilter[~dataFrameTableMask]

    for dateStrArr in dateStrArrMap:
        for tableIndex, tableRow in dataFrameTableFilter.iterrows():
            tableName_ori = tableRow["TABLE_NAME"]
            tableName = tableRow["TABLE_NAME"].lower()
            for gameWorldArr in gameWorldArrMap:
                tableFullName = schmeaNameInit + tableName
                partitionedStr = partitionedInit.replace("[:DateNoLine]", dateStrArr[0]).replace("[:WorldNumber]",gameWorldArr[0])
                filePathStr = filePathInit.replace("[:DBName]", gameWorldArr[1]).replace("[:DateNoLine]",dateStrArr[0]).replace("[:TableName]", tableName_ori)
                alterPartitionCode = alterPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr).replace("[:FilePath]", filePathStr)
                dropPartitionCode = dropPartitionCodeInit.replace("[:TableName]", tableFullName).replace("[:Partitioned]", partitionedStr)

                print(dropPartitionCode)
                runList.append(f"{dropPartitionCode};{alterPartitionCode};")

    return runList
def createPartition(dropSQL, alterSQL):
    print(dropSQL)
    try:
        hiveCtrl.executeSQL(dropSQL)
        print("Info:Table Partition Successfully Dropped")
    except:
        print("Info: Fail to Drop Table Partition ")
    print(alterSQL)
    try:
        hiveCtrl.executeSQL(alterSQL)
        print("Info:Table Partition Successfully Altered")
    except:
        print("Info: Fail to Alter Table Partition ")


#----------------------------------------------------------------------------------------------------

def getDBTableSizeSql():
    return """
    SELECT 
        s.Name AS SchemaName
        , t.NAME AS TableName
        , MAX(p.rows) AS RowCounts
        , SUM(a.total_pages) * 8 /1024 AS TotalSpaceMB
        , SUM(a.used_pages) * 8 /1024 AS UsedSpaceMB
        , (SUM(a.total_pages) - SUM(a.used_pages)) * 8 /1024 AS UnusedSpaceMB
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
        , TotalSpaceMB ASC
    """