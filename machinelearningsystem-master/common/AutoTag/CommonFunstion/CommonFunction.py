import os
from package.modeling.common.common.CommonFunction import CommonFunction as ParentCommonFunction
from package.common.database.hiveCtrl import HiveCtrl
from dotenv import load_dotenv

class CommonFunction(ParentCommonFunction):

    def __init__(self):
        pass

    @classmethod
    def getAutoTagTextDF(self, makeInfo):
        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )
        textProduct = makeInfo['TextProduct']
        textProject = makeInfo['TextProject']
        textVersion = makeInfo['TextVersion']
        textStep = makeInfo['TextStep']
        textDateNoLine = makeInfo['TextDateNoLine']
        sql = """
            SELECT 
                commondata_001, commondata_002, commondata_003, commondata_004, commondata_005
                , commondata_006, commondata_007, commondata_008, commondata_009, commondata_010
                , commondata_011, commondata_012, commondata_013, commondata_014, commondata_015
                , uniquefloat_001, uniquefloat_002, uniquefloat_003, uniquefloat_004, uniquefloat_005
                , uniquefloat_006, uniquefloat_007, uniquefloat_008, uniquefloat_009, uniquefloat_010
                , uniquefloat_011, uniquefloat_012, uniquefloat_013, uniquefloat_014, uniquefloat_015
                , uniquefloat_016, uniquefloat_017, uniquefloat_018, uniquefloat_019, uniquefloat_020
                , uniquefloat_021, uniquefloat_022, uniquefloat_023, uniquefloat_024, uniquefloat_025
                , uniquefloat_026, uniquefloat_027, uniquefloat_028, uniquefloat_029, uniquefloat_030
                , uniquefloat_031, uniquefloat_032, uniquefloat_033, uniquefloat_034, uniquefloat_035
                , uniquefloat_036, uniquefloat_037, uniquefloat_038, uniquefloat_039, uniquefloat_040
                , uniquefloat_041, uniquefloat_042, uniquefloat_043, uniquefloat_044, uniquefloat_045
                , uniquefloat_046, uniquefloat_047, uniquefloat_048, uniquefloat_049, uniquefloat_050
                , uniquefloat_051, uniquefloat_052, uniquefloat_053, uniquefloat_054, uniquefloat_055
                , uniquefloat_056, uniquefloat_057, uniquefloat_058, uniquefloat_059, uniquefloat_060
                , uniquefloat_061, uniquefloat_062, uniquefloat_063, uniquefloat_064, uniquefloat_065
                , uniquefloat_066, uniquefloat_067, uniquefloat_068, uniquefloat_069, uniquefloat_070
                , uniquefloat_071, uniquefloat_072, uniquefloat_073, uniquefloat_074, uniquefloat_075
                , uniquefloat_076, uniquefloat_077, uniquefloat_078, uniquefloat_079, uniquefloat_080
                , uniquefloat_081, uniquefloat_082, uniquefloat_083, uniquefloat_084, uniquefloat_085
                , uniquefloat_086, uniquefloat_087, uniquefloat_088, uniquefloat_089, uniquefloat_090
                , uniquefloat_091, uniquefloat_092, uniquefloat_093, uniquefloat_094, uniquefloat_095
                , uniquefloat_096, uniquefloat_097, uniquefloat_098, uniquefloat_099, uniquefloat_100
                , uniquefloat_101, uniquefloat_102, uniquefloat_103, uniquefloat_104, uniquefloat_105
                , uniquefloat_106, uniquefloat_107, uniquefloat_108, uniquefloat_109, uniquefloat_110
                , uniquefloat_111, uniquefloat_112, uniquefloat_113, uniquefloat_114, uniquefloat_115
                , uniquefloat_116, uniquefloat_117, uniquefloat_118, uniquefloat_119, uniquefloat_120
                , uniquefloat_121, uniquefloat_122, uniquefloat_123, uniquefloat_124, uniquefloat_125
                , uniquefloat_126, uniquefloat_127, uniquefloat_128, uniquefloat_129, uniquefloat_130
                , uniquefloat_131, uniquefloat_132, uniquefloat_133, uniquefloat_134, uniquefloat_135
                , uniquefloat_136, uniquefloat_137, uniquefloat_138, uniquefloat_139, uniquefloat_140
                , uniquefloat_141, uniquefloat_142, uniquefloat_143, uniquefloat_144, uniquefloat_145
                , uniquefloat_146, uniquefloat_147, uniquefloat_148, uniquefloat_149, uniquefloat_150
                , uniquefloat_151, uniquefloat_152, uniquefloat_153, uniquefloat_154, uniquefloat_155
                , uniquefloat_156, uniquefloat_157, uniquefloat_158, uniquefloat_159, uniquefloat_160
                , uniquefloat_161, uniquefloat_162, uniquefloat_163, uniquefloat_164, uniquefloat_165
                , uniquefloat_166, uniquefloat_167, uniquefloat_168, uniquefloat_169, uniquefloat_170
                , uniquefloat_171, uniquefloat_172, uniquefloat_173, uniquefloat_174, uniquefloat_175
                , uniquefloat_176, uniquefloat_177, uniquefloat_178, uniquefloat_179, uniquefloat_180
                , uniquefloat_181, uniquefloat_182, uniquefloat_183, uniquefloat_184, uniquefloat_185
                , uniquefloat_186, uniquefloat_187, uniquefloat_188, uniquefloat_189, uniquefloat_190
                , uniquefloat_191, uniquefloat_192, uniquefloat_193, uniquefloat_194, uniquefloat_195
                , uniquefloat_196, uniquefloat_197, uniquefloat_198, uniquefloat_199, uniquefloat_200
                , uniquejson_001
            FROM gtwpd.model_usedata 
            WHERE 1 = 1 
                AND product = '[:TextProduct]' 
                AND project = '[:TextProject]' 
                AND version = '[:TextVersion]' 
                AND step = '[:TextStep]'
                AND dt = '[:TextDateNoLine]' 
            ORDER BY 
                cast(commondata_013 as BIGINT)        
            LIMIT 100000
        """.replace("[:TextProduct]", textProduct) \
            .replace("[:TextProject]", textProject) \
            .replace("[:TextVersion]", textVersion) \
            .replace("[:TextStep]", textStep) \
            .replace("[:TextDateNoLine]", textDateNoLine)
        print(sql)
        df = hiveCtrl.searchSQL(sql)
        return df

    @classmethod
    def getLimitTextSQL(self,makeInfo):
        floatColumnArr = self.getUseModelFloatColumnArr()
        textInfo = makeInfo['textInfo']
        textDF = self.getAutoTagTextDF(textInfo)
        sqlArr = []
        textSize = len(textDF)
        for dataCount in range (1 , textSize//200 + 2 ) :
            columnsStr = ""
            min =  (dataCount- 1) * 200 + 1
            max =  dataCount * 200 + 1

            print(textDF)

            for columnCount in range(min,max):
                if columnCount <= textSize :
                    dataCountDF = textDF[textDF['commondata_013'] == str(columnCount)]
                    columnStr = "\n                    , MAX( CASE WHEN AA.[:CompareColumn] <= [:UpLimitNumber] AND AA.[:CompareColumn] > [:LowerLimitNumber] THEN 1 ELSE 0 END ) as [:ColumnName] "
                    columnStr = columnStr.replace("[:UpLimitNumber]", str(dataCountDF['uniquefloat_001'].tolist()[0]))
                    columnStr = columnStr.replace("[:LowerLimitNumber]",str(dataCountDF['uniquefloat_002'].tolist()[0]))
                    columnStr = columnStr.replace("[:CompareColumn]",makeInfo['CompareColumn'])
                else :
                    columnStr = "\n                    , null as [:ColumnName] "
                columnStr = columnStr.replace("[:ColumnName]",floatColumnArr[columnCount-min])
                columnsStr = columnsStr + columnStr
            sql = """
                INSERT [:IsOverWrite] TABLE gtwpd.model_usedata PARTITION (product='[:ProductName]',project='[:Project]',version='[:TargetVersion]',dt ='[:DateNoLine]',step ='[:TargetStep]')
                SELECT 
                    AA.commondata_001 as commondata_001
                    , AA.commondata_002 as commondata_002
                    , AA.commondata_003 as commondata_003
                    , AA.commondata_004 as commondata_004
                    , AA.commondata_005 as commondata_005
                    , AA.commondata_006 as commondata_006
                    , AA.commondata_007 as commondata_007
                    , AA.commondata_008 as commondata_008
                    , AA.commondata_009 as commondata_009
                    , AA.commondata_010 as commondata_010
                    , null as commondata_011
                    , null as commondata_012
                    , '[:DataOrderNumber]' as commondata_013
                    , null as commondata_014
                    , null as commondata_015 [:ColumnsStr]
                    , NULL AS UniqueJson_001 
                FROM gtwpd.model_usedata AA 
                WHERE 1 = 1
                    AND AA.product= '[:ProductName]'
                    AND AA.project= '[:Project]'
                    AND AA.version= '[:SourceVersion]'
                    AND AA.step= '[:SourceStep]'
                    AND AA.dt= '[:DateNoLine]' ; 
            """.replace("[:ColumnsStr]",columnsStr) \
                .replace("[:DataOrderNumber]", str(dataCount))
            if len(sqlArr) == 0:
                sql = sql.replace("[:IsOverWrite]", "OVERWRITE")
            else :
                sql = sql.replace("[:IsOverWrite]", "INTO")
            sqlArr.append(sql)
        return sqlArr

