import os
import pandas
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl

class UseModel_P02AdvancedExample() :

    @classmethod
    def MakeUseModel_P02AdvancedExample_M1_0_2(self, makeInfo):
        """
        MakeUseModelOrderSQLInsert 方法說明
            會撈取 return useModelType , useModelObject 的 useModelObject 裡面放置SQL陣列
            就會將 useModelObject 裡的SQL陣列丟置HIVE執行
        相關可輸入參數
            ["[:ProductName]", productName]                                         # 產品名稱
            ["[:Project]", project]                                                 # 專案名稱
            ["[:ModelVersion]", modelVersion]                                       # 整體模型版本編號
            ["[:UseModelVersion]", usemodelVersion]                                 # 本次UseModel版本編號
            ["[:PreProcessVersion]", preprocessVersion]                             # 本次PreProcess版本編號
            ["[:RawDataVersion]", rawdataVersion]                                   # 本次RawData版本編號
            ["[:DateLine]", makeTime.strftime("%Y-%m-%d")]                          # 資料製作日期 格式為%Y-%m-%d
            ["[:DateNoLine]", makeTime.strftime("%Y%m%d")]                          #  資料製作日期 格式為%Y%m%d
            ["[:DateLineFirstMonth]", firstMonthTime.strftime("%Y-%m-%d")]          # 資料製作日期這個月的第一天 格式為 %Y-%m-%d
            ["[:DateLineLastMonth]", lastMonthTime.strftime("%Y-%m-%d")]            # 資料製作日期這個月的最後一天 格式為 %Y-%m-%d
            ["[:DateNoLineFirstMonth]", firstMonthTime.strftime("%Y%m%d")]          # 資料製作日期這個月的第一天 格式為 %Y%m%d
            ["[:DateNoLineLastMonth]", lastMonthTime.strftime("%Y%m%d")]            # 資料製作日期這個月的最後一天 格式為 %Y%m%d
        有些需要特定製作的日期，請在本Function先做好replace到SQL中
        """
        orderSQL1 = """
              WITH maintable AS (
                  SELECT   
                      commondata_1
                      , commondata_2
                      , commondata_3
                      , commondata_4
                      , commondata_5
                      , commondata_6
                      , commondata_7
                      , NULL
                      , SUBSTR(dt,1,6) AS commondata_9
                      , uniquestr_1         
                      , tablenumber  
                  FROM gtwpd.modelextract_modelextract
                  WHERE 1 = 1  
                      AND game = 'maple'
                      AND dt >= [:DateNoLine] 
                      AND dt <= [:DateNoLine]  
                      AND tablenumber IN ( 16508 )
              ) INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = 'ExampleProduct' , project = 'P02AdvancedExample' , step = 'UseModel', version = 'M1_0_2' , dt = '[:DateNoLine]'  )
              SELECT 
                  commondata_1 AS commondata_1
                  , commondata_2 AS commondata_2
                  , commondata_3 AS commondata_3
                  , commondata_4 AS commondata_4
                  , MAX ( commondata_5 ) AS commondata_5
                  , MAX ( commondata_6 ) AS commondata_6
                  , MAX ( commondata_7 ) AS commondata_7
                  , MAX ( NULL ) AS commondata_8
                  , commondata_9 AS commondata_9
                  , MAX ( NULL ) AS commondata_10
                  , MAX ( NULL ) AS commondata_11
                  , MAX ( NULL ) AS commondata_12
                  , MAX ( NULL ) AS commondata_13
                  , MAX ( NULL ) AS commondata_14
                  , MAX ( NULL ) AS commondata_15
                  , SUM ( CASE WHEN tablenumber = 16508 THEN 1 ELSE NULL END ) AS UniqueFloat_001
                  , NULL AS UniqueFloat_002
                  , NULL AS UniqueFloat_003
                  , NULL AS UniqueFloat_004
                  , NULL AS UniqueFloat_005
                  , NULL AS UniqueFloat_006
                  , NULL AS UniqueFloat_007
                  , NULL AS UniqueFloat_008
                  , NULL AS UniqueFloat_009
                  , NULL AS UniqueFloat_010
                  , NULL AS UniqueFloat_011
                  , NULL AS UniqueFloat_012
                  , NULL AS UniqueFloat_013
                  , NULL AS UniqueFloat_014
                  , NULL AS UniqueFloat_015
                  , NULL AS UniqueFloat_016
                  , NULL AS UniqueFloat_017
                  , NULL AS UniqueFloat_018
                  , NULL AS UniqueFloat_019
                  , NULL AS UniqueFloat_020
                  , NULL AS UniqueFloat_021
                  , NULL AS UniqueFloat_022
                  , NULL AS UniqueFloat_023
                  , NULL AS UniqueFloat_024
                  , NULL AS UniqueFloat_025
                  , NULL AS UniqueFloat_026
                  , NULL AS UniqueFloat_027
                  , NULL AS UniqueFloat_028
                  , NULL AS UniqueFloat_029
                  , NULL AS UniqueFloat_030
                  , NULL AS UniqueFloat_031
                  , NULL AS UniqueFloat_032
                  , NULL AS UniqueFloat_033
                  , NULL AS UniqueFloat_034
                  , NULL AS UniqueFloat_035
                  , NULL AS UniqueFloat_036
                  , NULL AS UniqueFloat_037
                  , NULL AS UniqueFloat_038
                  , NULL AS UniqueFloat_039
                  , NULL AS UniqueFloat_040
                  , NULL AS UniqueFloat_041
                  , NULL AS UniqueFloat_042
                  , NULL AS UniqueFloat_043
                  , NULL AS UniqueFloat_044
                  , NULL AS UniqueFloat_045
                  , NULL AS UniqueFloat_046
                  , NULL AS UniqueFloat_047
                  , NULL AS UniqueFloat_048
                  , NULL AS UniqueFloat_049
                  , NULL AS UniqueFloat_050
                  , NULL AS UniqueFloat_051
                  , NULL AS UniqueFloat_052
                  , NULL AS UniqueFloat_053
                  , NULL AS UniqueFloat_054
                  , NULL AS UniqueFloat_055
                  , NULL AS UniqueFloat_056
                  , NULL AS UniqueFloat_057
                  , NULL AS UniqueFloat_058
                  , NULL AS UniqueFloat_059
                  , NULL AS UniqueFloat_060
                  , NULL AS UniqueFloat_061
                  , NULL AS UniqueFloat_062
                  , NULL AS UniqueFloat_063
                  , NULL AS UniqueFloat_064
                  , NULL AS UniqueFloat_065
                  , NULL AS UniqueFloat_066
                  , NULL AS UniqueFloat_067
                  , NULL AS UniqueFloat_068
                  , NULL AS UniqueFloat_069
                  , NULL AS UniqueFloat_070
                  , NULL AS UniqueFloat_071
                  , NULL AS UniqueFloat_072
                  , NULL AS UniqueFloat_073
                  , NULL AS UniqueFloat_074
                  , NULL AS UniqueFloat_075
                  , NULL AS UniqueFloat_076
                  , NULL AS UniqueFloat_077
                  , NULL AS UniqueFloat_078
                  , NULL AS UniqueFloat_079
                  , NULL AS UniqueFloat_080
                  , NULL AS UniqueFloat_081
                  , NULL AS UniqueFloat_082
                  , NULL AS UniqueFloat_083
                  , NULL AS UniqueFloat_084
                  , NULL AS UniqueFloat_085
                  , NULL AS UniqueFloat_086
                  , NULL AS UniqueFloat_087
                  , NULL AS UniqueFloat_088
                  , NULL AS UniqueFloat_089
                  , NULL AS UniqueFloat_090
                  , NULL AS UniqueFloat_091
                  , NULL AS UniqueFloat_092
                  , NULL AS UniqueFloat_093
                  , NULL AS UniqueFloat_094
                  , NULL AS UniqueFloat_095
                  , NULL AS UniqueFloat_096
                  , NULL AS UniqueFloat_097
                  , NULL AS UniqueFloat_098
                  , NULL AS UniqueFloat_099
                  , NULL AS UniqueFloat_100
                  , NULL AS UniqueFloat_101
                  , NULL AS UniqueFloat_102
                  , NULL AS UniqueFloat_103
                  , NULL AS UniqueFloat_104
                  , NULL AS UniqueFloat_105
                  , NULL AS UniqueFloat_106
                  , NULL AS UniqueFloat_107
                  , NULL AS UniqueFloat_108
                  , NULL AS UniqueFloat_109
                  , NULL AS UniqueFloat_110
                  , NULL AS UniqueFloat_111
                  , NULL AS UniqueFloat_112
                  , NULL AS UniqueFloat_113
                  , NULL AS UniqueFloat_114
                  , NULL AS UniqueFloat_115
                  , NULL AS UniqueFloat_116
                  , NULL AS UniqueFloat_117
                  , NULL AS UniqueFloat_118
                  , NULL AS UniqueFloat_119
                  , NULL AS UniqueFloat_120
                  , NULL AS UniqueFloat_121
                  , NULL AS UniqueFloat_122
                  , NULL AS UniqueFloat_123
                  , NULL AS UniqueFloat_124
                  , NULL AS UniqueFloat_125
                  , NULL AS UniqueFloat_126
                  , NULL AS UniqueFloat_127
                  , NULL AS UniqueFloat_128
                  , NULL AS UniqueFloat_129
                  , NULL AS UniqueFloat_130
                  , NULL AS UniqueFloat_131
                  , NULL AS UniqueFloat_132
                  , NULL AS UniqueFloat_133
                  , NULL AS UniqueFloat_134
                  , NULL AS UniqueFloat_135
                  , NULL AS UniqueFloat_136
                  , NULL AS UniqueFloat_137
                  , NULL AS UniqueFloat_138
                  , NULL AS UniqueFloat_139
                  , NULL AS UniqueFloat_140
                  , NULL AS UniqueFloat_141
                  , NULL AS UniqueFloat_142
                  , NULL AS UniqueFloat_143
                  , NULL AS UniqueFloat_144
                  , NULL AS UniqueFloat_145
                  , NULL AS UniqueFloat_146
                  , NULL AS UniqueFloat_147
                  , NULL AS UniqueFloat_148
                  , NULL AS UniqueFloat_149
                  , NULL AS UniqueFloat_150
                  , NULL AS UniqueFloat_151
                  , NULL AS UniqueFloat_152
                  , NULL AS UniqueFloat_153
                  , NULL AS UniqueFloat_154
                  , NULL AS UniqueFloat_155
                  , NULL AS UniqueFloat_156
                  , NULL AS UniqueFloat_157
                  , NULL AS UniqueFloat_158
                  , NULL AS UniqueFloat_159
                  , NULL AS UniqueFloat_160
                  , NULL AS UniqueFloat_161
                  , NULL AS UniqueFloat_162
                  , NULL AS UniqueFloat_163
                  , NULL AS UniqueFloat_164
                  , NULL AS UniqueFloat_165
                  , NULL AS UniqueFloat_166
                  , NULL AS UniqueFloat_167
                  , NULL AS UniqueFloat_168
                  , NULL AS UniqueFloat_169
                  , NULL AS UniqueFloat_170
                  , NULL AS UniqueFloat_171
                  , NULL AS UniqueFloat_172
                  , NULL AS UniqueFloat_173
                  , NULL AS UniqueFloat_174
                  , NULL AS UniqueFloat_175
                  , NULL AS UniqueFloat_176
                  , NULL AS UniqueFloat_177
                  , NULL AS UniqueFloat_178
                  , NULL AS UniqueFloat_179
                  , NULL AS UniqueFloat_180
                  , NULL AS UniqueFloat_181
                  , NULL AS UniqueFloat_182
                  , NULL AS UniqueFloat_183
                  , NULL AS UniqueFloat_184
                  , NULL AS UniqueFloat_185
                  , NULL AS UniqueFloat_186
                  , NULL AS UniqueFloat_187
                  , NULL AS UniqueFloat_188
                  , NULL AS UniqueFloat_189
                  , NULL AS UniqueFloat_190
                  , NULL AS UniqueFloat_191
                  , NULL AS UniqueFloat_192
                  , NULL AS UniqueFloat_193
                  , NULL AS UniqueFloat_194
                  , NULL AS UniqueFloat_195
                  , NULL AS UniqueFloat_196
                  , NULL AS UniqueFloat_197
                  , NULL AS UniqueFloat_198
                  , NULL AS UniqueFloat_199
                  , NULL AS UniqueFloat_200
                  , NULL AS UniqueJson_001 
              FROM maintable
              GROUP BY 
                  commondata_1
                  , commondata_2
                  , commondata_3
                  , commondata_4
                  , commondata_9 ;
                  
              INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' , step = 'ModelResult', version = '[:ModelVersion]' , dt = '[:DateNoLine]'  )       
              SELECT 
                  AA.commondata_001 AS commondata_001
                  , AA.commondata_002 AS commondata_002
                  , AA.commondata_003 AS commondata_003
                  , AA.commondata_004 AS commondata_004
                  , AA.commondata_005 AS commondata_005
                  , AA.commondata_006 AS commondata_006
                  , AA.commondata_007 AS commondata_007
                  , NULL AS commondata_018
                  , AA.commondata_009 AS commondata_009
                  , NULL AS commondata_010
                  , NULL AS commondata_011
                  , NULL AS commondata_012
                  , NULL AS commondata_013
                  , NULL AS commondata_014
                  , NULL AS commondata_015
                  , AA.UniqueFloat_001 AS UniqueFloat_001
                  , NULL AS UniqueFloat_002
                  , NULL AS UniqueFloat_003
                  , NULL AS UniqueFloat_004
                  , NULL AS UniqueFloat_005
                  , NULL AS UniqueFloat_006
                  , NULL AS UniqueFloat_007
                  , NULL AS UniqueFloat_008
                  , NULL AS UniqueFloat_009
                  , NULL AS UniqueFloat_010
                  , NULL AS UniqueFloat_011
                  , NULL AS UniqueFloat_012
                  , NULL AS UniqueFloat_013
                  , NULL AS UniqueFloat_014
                  , NULL AS UniqueFloat_015
                  , NULL AS UniqueFloat_016
                  , NULL AS UniqueFloat_017
                  , NULL AS UniqueFloat_018
                  , NULL AS UniqueFloat_019
                  , NULL AS UniqueFloat_020
                  , NULL AS UniqueFloat_021
                  , NULL AS UniqueFloat_022
                  , NULL AS UniqueFloat_023
                  , NULL AS UniqueFloat_024
                  , NULL AS UniqueFloat_025
                  , NULL AS UniqueFloat_026
                  , NULL AS UniqueFloat_027
                  , NULL AS UniqueFloat_028
                  , NULL AS UniqueFloat_029
                  , NULL AS UniqueFloat_030
                  , NULL AS UniqueFloat_031
                  , NULL AS UniqueFloat_032
                  , NULL AS UniqueFloat_033
                  , NULL AS UniqueFloat_034
                  , NULL AS UniqueFloat_035
                  , NULL AS UniqueFloat_036
                  , NULL AS UniqueFloat_037
                  , NULL AS UniqueFloat_038
                  , NULL AS UniqueFloat_039
                  , NULL AS UniqueFloat_040
                  , NULL AS UniqueFloat_041
                  , NULL AS UniqueFloat_042
                  , NULL AS UniqueFloat_043
                  , NULL AS UniqueFloat_044
                  , NULL AS UniqueFloat_045
                  , NULL AS UniqueFloat_046
                  , NULL AS UniqueFloat_047
                  , NULL AS UniqueFloat_048
                  , NULL AS UniqueFloat_049
                  , NULL AS UniqueFloat_050
                  , NULL AS UniqueFloat_051
                  , NULL AS UniqueFloat_052
                  , NULL AS UniqueFloat_053
                  , NULL AS UniqueFloat_054
                  , NULL AS UniqueFloat_055
                  , NULL AS UniqueFloat_056
                  , NULL AS UniqueFloat_057
                  , NULL AS UniqueFloat_058
                  , NULL AS UniqueFloat_059
                  , NULL AS UniqueFloat_060
                  , NULL AS UniqueFloat_061
                  , NULL AS UniqueFloat_062
                  , NULL AS UniqueFloat_063
                  , NULL AS UniqueFloat_064
                  , NULL AS UniqueFloat_065
                  , NULL AS UniqueFloat_066
                  , NULL AS UniqueFloat_067
                  , NULL AS UniqueFloat_068
                  , NULL AS UniqueFloat_069
                  , NULL AS UniqueFloat_070
                  , NULL AS UniqueFloat_071
                  , NULL AS UniqueFloat_072
                  , NULL AS UniqueFloat_073
                  , NULL AS UniqueFloat_074
                  , NULL AS UniqueFloat_075
                  , NULL AS UniqueFloat_076
                  , NULL AS UniqueFloat_077
                  , NULL AS UniqueFloat_078
                  , NULL AS UniqueFloat_079
                  , NULL AS UniqueFloat_080
                  , NULL AS UniqueFloat_081
                  , NULL AS UniqueFloat_082
                  , NULL AS UniqueFloat_083
                  , NULL AS UniqueFloat_084
                  , NULL AS UniqueFloat_085
                  , NULL AS UniqueFloat_086
                  , NULL AS UniqueFloat_087
                  , NULL AS UniqueFloat_088
                  , NULL AS UniqueFloat_089
                  , NULL AS UniqueFloat_090
                  , NULL AS UniqueFloat_091
                  , NULL AS UniqueFloat_092
                  , NULL AS UniqueFloat_093
                  , NULL AS UniqueFloat_094
                  , NULL AS UniqueFloat_095
                  , NULL AS UniqueFloat_096
                  , NULL AS UniqueFloat_097
                  , NULL AS UniqueFloat_098
                  , NULL AS UniqueFloat_099
                  , NULL AS UniqueFloat_100
                  , NULL AS UniqueFloat_101
                  , NULL AS UniqueFloat_102
                  , NULL AS UniqueFloat_103
                  , NULL AS UniqueFloat_104
                  , NULL AS UniqueFloat_105
                  , NULL AS UniqueFloat_106
                  , NULL AS UniqueFloat_107
                  , NULL AS UniqueFloat_108
                  , NULL AS UniqueFloat_109
                  , NULL AS UniqueFloat_110
                  , NULL AS UniqueFloat_111
                  , NULL AS UniqueFloat_112
                  , NULL AS UniqueFloat_113
                  , NULL AS UniqueFloat_114
                  , NULL AS UniqueFloat_115
                  , NULL AS UniqueFloat_116
                  , NULL AS UniqueFloat_117
                  , NULL AS UniqueFloat_118
                  , NULL AS UniqueFloat_119
                  , NULL AS UniqueFloat_120
                  , NULL AS UniqueFloat_121
                  , NULL AS UniqueFloat_122
                  , NULL AS UniqueFloat_123
                  , NULL AS UniqueFloat_124
                  , NULL AS UniqueFloat_125
                  , NULL AS UniqueFloat_126
                  , NULL AS UniqueFloat_127
                  , NULL AS UniqueFloat_128
                  , NULL AS UniqueFloat_129
                  , NULL AS UniqueFloat_130
                  , NULL AS UniqueFloat_131
                  , NULL AS UniqueFloat_132
                  , NULL AS UniqueFloat_133
                  , NULL AS UniqueFloat_134
                  , NULL AS UniqueFloat_135
                  , NULL AS UniqueFloat_136
                  , NULL AS UniqueFloat_137
                  , NULL AS UniqueFloat_138
                  , NULL AS UniqueFloat_139
                  , NULL AS UniqueFloat_140
                  , NULL AS UniqueFloat_141
                  , NULL AS UniqueFloat_142
                  , NULL AS UniqueFloat_143
                  , NULL AS UniqueFloat_144
                  , NULL AS UniqueFloat_145
                  , NULL AS UniqueFloat_146
                  , NULL AS UniqueFloat_147
                  , NULL AS UniqueFloat_148
                  , NULL AS UniqueFloat_149
                  , NULL AS UniqueFloat_150
                  , NULL AS UniqueFloat_151
                  , NULL AS UniqueFloat_152
                  , NULL AS UniqueFloat_153
                  , NULL AS UniqueFloat_154
                  , NULL AS UniqueFloat_155
                  , NULL AS UniqueFloat_156
                  , NULL AS UniqueFloat_157
                  , NULL AS UniqueFloat_158
                  , NULL AS UniqueFloat_159
                  , NULL AS UniqueFloat_160
                  , NULL AS UniqueFloat_161
                  , NULL AS UniqueFloat_162
                  , NULL AS UniqueFloat_163
                  , NULL AS UniqueFloat_164
                  , NULL AS UniqueFloat_165
                  , NULL AS UniqueFloat_166
                  , NULL AS UniqueFloat_167
                  , NULL AS UniqueFloat_168
                  , NULL AS UniqueFloat_169
                  , NULL AS UniqueFloat_170
                  , NULL AS UniqueFloat_171
                  , NULL AS UniqueFloat_172
                  , NULL AS UniqueFloat_173
                  , NULL AS UniqueFloat_174
                  , NULL AS UniqueFloat_175
                  , NULL AS UniqueFloat_176
                  , NULL AS UniqueFloat_177
                  , NULL AS UniqueFloat_178
                  , NULL AS UniqueFloat_179
                  , NULL AS UniqueFloat_180
                  , NULL AS UniqueFloat_181
                  , NULL AS UniqueFloat_182
                  , NULL AS UniqueFloat_183
                  , NULL AS UniqueFloat_184
                  , NULL AS UniqueFloat_185
                  , NULL AS UniqueFloat_186
                  , NULL AS UniqueFloat_187
                  , NULL AS UniqueFloat_188
                  , NULL AS UniqueFloat_189
                  , NULL AS UniqueFloat_190
                  , NULL AS UniqueFloat_191
                  , NULL AS UniqueFloat_192
                  , NULL AS UniqueFloat_193
                  , NULL AS UniqueFloat_194
                  , NULL AS UniqueFloat_195
                  , NULL AS UniqueFloat_196
                  , NULL AS UniqueFloat_197
                  , NULL AS UniqueFloat_198
                  , NULL AS UniqueFloat_199
                  , NULL AS UniqueFloat_200
                  , NULL AS UniqueJson_001 
              FROM gtwpd.model_usedata  AA
              WHERE 1 = 1 
                  AND product = '[:ProductName]' 
                  AND project = '[:Project]' 
                  AND step = 'UseModel'
                  AND version = '[:UseModelVersion]' 
                  AND dt = '[:DateNoLine]' ;
            
              -- 正常來說ModelScore要做一些運算 但該範例只是呈現怎麼做  細節部分請自行處裡
              INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' , step = 'ModelScore', version = '[:ModelVersion]' , dt = '[:DateNoLine]'  )       
              SELECT 
                  AA.commondata_001 AS commondata_001
                  , AA.commondata_002 AS commondata_002
                  , AA.commondata_003 AS commondata_003
                  , AA.commondata_004 AS commondata_004
                  , AA.commondata_005 AS commondata_005
                  , AA.commondata_006 AS commondata_006
                  , AA.commondata_007 AS commondata_007
                  , NULL AS commondata_018
                  , AA.commondata_009 AS commondata_009
                  , NULL AS commondata_010
                  , NULL AS commondata_011
                  , NULL AS commondata_012
                  , NULL AS commondata_013
                  , NULL AS commondata_014
                  , NULL AS commondata_015
                  , AA.UniqueFloat_001 AS UniqueFloat_001
                  , NULL AS UniqueFloat_002
                  , NULL AS UniqueFloat_003
                  , NULL AS UniqueFloat_004
                  , NULL AS UniqueFloat_005
                  , NULL AS UniqueFloat_006
                  , NULL AS UniqueFloat_007
                  , NULL AS UniqueFloat_008
                  , NULL AS UniqueFloat_009
                  , NULL AS UniqueFloat_010
                  , NULL AS UniqueFloat_011
                  , NULL AS UniqueFloat_012
                  , NULL AS UniqueFloat_013
                  , NULL AS UniqueFloat_014
                  , NULL AS UniqueFloat_015
                  , NULL AS UniqueFloat_016
                  , NULL AS UniqueFloat_017
                  , NULL AS UniqueFloat_018
                  , NULL AS UniqueFloat_019
                  , NULL AS UniqueFloat_020
                  , NULL AS UniqueFloat_021
                  , NULL AS UniqueFloat_022
                  , NULL AS UniqueFloat_023
                  , NULL AS UniqueFloat_024
                  , NULL AS UniqueFloat_025
                  , NULL AS UniqueFloat_026
                  , NULL AS UniqueFloat_027
                  , NULL AS UniqueFloat_028
                  , NULL AS UniqueFloat_029
                  , NULL AS UniqueFloat_030
                  , NULL AS UniqueFloat_031
                  , NULL AS UniqueFloat_032
                  , NULL AS UniqueFloat_033
                  , NULL AS UniqueFloat_034
                  , NULL AS UniqueFloat_035
                  , NULL AS UniqueFloat_036
                  , NULL AS UniqueFloat_037
                  , NULL AS UniqueFloat_038
                  , NULL AS UniqueFloat_039
                  , NULL AS UniqueFloat_040
                  , NULL AS UniqueFloat_041
                  , NULL AS UniqueFloat_042
                  , NULL AS UniqueFloat_043
                  , NULL AS UniqueFloat_044
                  , NULL AS UniqueFloat_045
                  , NULL AS UniqueFloat_046
                  , NULL AS UniqueFloat_047
                  , NULL AS UniqueFloat_048
                  , NULL AS UniqueFloat_049
                  , NULL AS UniqueFloat_050
                  , NULL AS UniqueFloat_051
                  , NULL AS UniqueFloat_052
                  , NULL AS UniqueFloat_053
                  , NULL AS UniqueFloat_054
                  , NULL AS UniqueFloat_055
                  , NULL AS UniqueFloat_056
                  , NULL AS UniqueFloat_057
                  , NULL AS UniqueFloat_058
                  , NULL AS UniqueFloat_059
                  , NULL AS UniqueFloat_060
                  , NULL AS UniqueFloat_061
                  , NULL AS UniqueFloat_062
                  , NULL AS UniqueFloat_063
                  , NULL AS UniqueFloat_064
                  , NULL AS UniqueFloat_065
                  , NULL AS UniqueFloat_066
                  , NULL AS UniqueFloat_067
                  , NULL AS UniqueFloat_068
                  , NULL AS UniqueFloat_069
                  , NULL AS UniqueFloat_070
                  , NULL AS UniqueFloat_071
                  , NULL AS UniqueFloat_072
                  , NULL AS UniqueFloat_073
                  , NULL AS UniqueFloat_074
                  , NULL AS UniqueFloat_075
                  , NULL AS UniqueFloat_076
                  , NULL AS UniqueFloat_077
                  , NULL AS UniqueFloat_078
                  , NULL AS UniqueFloat_079
                  , NULL AS UniqueFloat_080
                  , NULL AS UniqueFloat_081
                  , NULL AS UniqueFloat_082
                  , NULL AS UniqueFloat_083
                  , NULL AS UniqueFloat_084
                  , NULL AS UniqueFloat_085
                  , NULL AS UniqueFloat_086
                  , NULL AS UniqueFloat_087
                  , NULL AS UniqueFloat_088
                  , NULL AS UniqueFloat_089
                  , NULL AS UniqueFloat_090
                  , NULL AS UniqueFloat_091
                  , NULL AS UniqueFloat_092
                  , NULL AS UniqueFloat_093
                  , NULL AS UniqueFloat_094
                  , NULL AS UniqueFloat_095
                  , NULL AS UniqueFloat_096
                  , NULL AS UniqueFloat_097
                  , NULL AS UniqueFloat_098
                  , NULL AS UniqueFloat_099
                  , NULL AS UniqueFloat_100
                  , NULL AS UniqueFloat_101
                  , NULL AS UniqueFloat_102
                  , NULL AS UniqueFloat_103
                  , NULL AS UniqueFloat_104
                  , NULL AS UniqueFloat_105
                  , NULL AS UniqueFloat_106
                  , NULL AS UniqueFloat_107
                  , NULL AS UniqueFloat_108
                  , NULL AS UniqueFloat_109
                  , NULL AS UniqueFloat_110
                  , NULL AS UniqueFloat_111
                  , NULL AS UniqueFloat_112
                  , NULL AS UniqueFloat_113
                  , NULL AS UniqueFloat_114
                  , NULL AS UniqueFloat_115
                  , NULL AS UniqueFloat_116
                  , NULL AS UniqueFloat_117
                  , NULL AS UniqueFloat_118
                  , NULL AS UniqueFloat_119
                  , NULL AS UniqueFloat_120
                  , NULL AS UniqueFloat_121
                  , NULL AS UniqueFloat_122
                  , NULL AS UniqueFloat_123
                  , NULL AS UniqueFloat_124
                  , NULL AS UniqueFloat_125
                  , NULL AS UniqueFloat_126
                  , NULL AS UniqueFloat_127
                  , NULL AS UniqueFloat_128
                  , NULL AS UniqueFloat_129
                  , NULL AS UniqueFloat_130
                  , NULL AS UniqueFloat_131
                  , NULL AS UniqueFloat_132
                  , NULL AS UniqueFloat_133
                  , NULL AS UniqueFloat_134
                  , NULL AS UniqueFloat_135
                  , NULL AS UniqueFloat_136
                  , NULL AS UniqueFloat_137
                  , NULL AS UniqueFloat_138
                  , NULL AS UniqueFloat_139
                  , NULL AS UniqueFloat_140
                  , NULL AS UniqueFloat_141
                  , NULL AS UniqueFloat_142
                  , NULL AS UniqueFloat_143
                  , NULL AS UniqueFloat_144
                  , NULL AS UniqueFloat_145
                  , NULL AS UniqueFloat_146
                  , NULL AS UniqueFloat_147
                  , NULL AS UniqueFloat_148
                  , NULL AS UniqueFloat_149
                  , NULL AS UniqueFloat_150
                  , NULL AS UniqueFloat_151
                  , NULL AS UniqueFloat_152
                  , NULL AS UniqueFloat_153
                  , NULL AS UniqueFloat_154
                  , NULL AS UniqueFloat_155
                  , NULL AS UniqueFloat_156
                  , NULL AS UniqueFloat_157
                  , NULL AS UniqueFloat_158
                  , NULL AS UniqueFloat_159
                  , NULL AS UniqueFloat_160
                  , NULL AS UniqueFloat_161
                  , NULL AS UniqueFloat_162
                  , NULL AS UniqueFloat_163
                  , NULL AS UniqueFloat_164
                  , NULL AS UniqueFloat_165
                  , NULL AS UniqueFloat_166
                  , NULL AS UniqueFloat_167
                  , NULL AS UniqueFloat_168
                  , NULL AS UniqueFloat_169
                  , NULL AS UniqueFloat_170
                  , NULL AS UniqueFloat_171
                  , NULL AS UniqueFloat_172
                  , NULL AS UniqueFloat_173
                  , NULL AS UniqueFloat_174
                  , NULL AS UniqueFloat_175
                  , NULL AS UniqueFloat_176
                  , NULL AS UniqueFloat_177
                  , NULL AS UniqueFloat_178
                  , NULL AS UniqueFloat_179
                  , NULL AS UniqueFloat_180
                  , NULL AS UniqueFloat_181
                  , NULL AS UniqueFloat_182
                  , NULL AS UniqueFloat_183
                  , NULL AS UniqueFloat_184
                  , NULL AS UniqueFloat_185
                  , NULL AS UniqueFloat_186
                  , NULL AS UniqueFloat_187
                  , NULL AS UniqueFloat_188
                  , NULL AS UniqueFloat_189
                  , NULL AS UniqueFloat_190
                  , NULL AS UniqueFloat_191
                  , NULL AS UniqueFloat_192
                  , NULL AS UniqueFloat_193
                  , NULL AS UniqueFloat_194
                  , NULL AS UniqueFloat_195
                  , NULL AS UniqueFloat_196
                  , NULL AS UniqueFloat_197
                  , NULL AS UniqueFloat_198
                  , NULL AS UniqueFloat_199
                  , NULL AS UniqueFloat_200
                  , NULL AS UniqueJson_001 
              FROM gtwpd.model_usedata AA
              WHERE 1 = 1 
                  AND product = '[:ProductName]' 
                  AND project = '[:Project]' 
                  AND step = 'UseModel'
                  AND version = '[:UseModelVersion]' 
                  AND dt = '[:DateNoLine]' ;  
              """
        return "MakeUseModelOrderSQLInsert", [orderSQL1] , {"DataCount":21123}

    @classmethod
    def MakeUseModel_P02AdvancedExample_M1_0_3(self, makeInfo):
        """
        MakeUseModelFileInsert 方法說明
            會撈取 return useModelType , useModelObject 的 useModelObject 裡面放置DF資料
            就會將 useModelObject 裡的DF上傳至HDFS上面，並且利用Hive建立與HDFS的Partition

            上傳路徑為 /user/GTW_PD/DB/Model/Usedata/product=[:ProductName]/project=[:Project]/version=[:Version]/dt=[:DateNoLine]/step=[:Step]
            像是該Function 就會上傳到/user/GTW_PD/DB/Model/Usedata/product=ExampleProduct/project=P02AdvancedExample/version=M1_0_3/dt=20211231/step=UseModel
        """
        df = pandas.read_csv("ExampleProduct/P02AdvancedExample/file/data/ExampleProduct_P02AdvancedExample_P1_0_3_20211231_rawdata_noallcolumn.csv", sep="\t")
        """
        # 模型建立............
        #
        #
        #
        #
        #
        """
        modelResultDF = df
        modelScoreDF = df
        return "MakeUseModelFileInsertOverwrite", [modelResultDF,modelScoreDF]  , {"DataCount":21123}

    @classmethod
    def MakeUseModel_P02AdvancedExample_M1_0_4(self, makeInfo):
        """
        MakeUseModelFreeFuction
            各種處理方式，隨意，最後請自行上傳到HIVE
        範例說明：
            以下是相關的自由Fuction資料製作
        """

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

        # 以下是採用撈取資料的方式製作，但可以自己看要怎麼使用
        # 自由發揮
        df = pandas.read_csv("ExampleProduct/P02AdvancedExample/file/data/ExampleProduct_P02AdvancedExample_P1_0_4_20211231_rawdata_noallcolumn.csv", sep="\t")
        """
        # 模型建立............
        #
        #
        #
        #
        #
        """
        modelResultDF = df
        modelScoreDF = df

        # 自由發揮 結束點
        productName = makeInfo["productName"]
        project = makeInfo["project"]
        modelVersion = makeInfo["modelVersion"]
        rawdataVersion = makeInfo["rawdataVersion"]
        preprocessVersion = makeInfo["preprocessVersion"]
        usemodelVersion = makeInfo["usemodelVersion"]
        dt = datetime.datetime.strptime(makeInfo["makeTime"], "%Y-%m-%d").strftime("%Y%m%d")

        columns = [
            "commondata_001", "commondata_002", "commondata_003", "commondata_004", "commondata_005"
            , "commondata_006", "commondata_007", "commondata_008", "commondata_009", "commondata_010"
            , "commondata_011", "commondata_012", "commondata_013", "commondata_014", "commondata_015"
            , "uniquefloat_001", "uniquefloat_002", "uniquefloat_003", "uniquefloat_004", "uniquefloat_005"
            , "uniquefloat_006", "uniquefloat_007", "uniquefloat_008", "uniquefloat_009", "uniquefloat_010"
            , "uniquefloat_011", "uniquefloat_012", "uniquefloat_013", "uniquefloat_014", "uniquefloat_015"
            , "uniquefloat_016", "uniquefloat_017", "uniquefloat_018", "uniquefloat_019", "uniquefloat_020"
            , "uniquefloat_021", "uniquefloat_022", "uniquefloat_023", "uniquefloat_024", "uniquefloat_025"
            , "uniquefloat_026", "uniquefloat_027", "uniquefloat_028", "uniquefloat_029", "uniquefloat_030"
            , "uniquefloat_031", "uniquefloat_032", "uniquefloat_033", "uniquefloat_034", "uniquefloat_035"
            , "uniquefloat_036", "uniquefloat_037", "uniquefloat_038", "uniquefloat_039", "uniquefloat_040"
            , "uniquefloat_041", "uniquefloat_042", "uniquefloat_043", "uniquefloat_044", "uniquefloat_045"
            , "uniquefloat_046", "uniquefloat_047", "uniquefloat_048", "uniquefloat_049", "uniquefloat_050"
            , "uniquefloat_051", "uniquefloat_052", "uniquefloat_053", "uniquefloat_054", "uniquefloat_055"
            , "uniquefloat_056", "uniquefloat_057", "uniquefloat_058", "uniquefloat_059", "uniquefloat_060"
            , "uniquefloat_061", "uniquefloat_062", "uniquefloat_063", "uniquefloat_064", "uniquefloat_065"
            , "uniquefloat_066", "uniquefloat_067", "uniquefloat_068", "uniquefloat_069", "uniquefloat_070"
            , "uniquefloat_071", "uniquefloat_072", "uniquefloat_073", "uniquefloat_074", "uniquefloat_075"
            , "uniquefloat_076", "uniquefloat_077", "uniquefloat_078", "uniquefloat_079", "uniquefloat_080"
            , "uniquefloat_081", "uniquefloat_082", "uniquefloat_083", "uniquefloat_084", "uniquefloat_085"
            , "uniquefloat_086", "uniquefloat_087", "uniquefloat_088", "uniquefloat_089", "uniquefloat_090"
            , "uniquefloat_091", "uniquefloat_092", "uniquefloat_093", "uniquefloat_094", "uniquefloat_095"
            , "uniquefloat_096", "uniquefloat_097", "uniquefloat_098", "uniquefloat_099", "uniquefloat_100"
            , "uniquefloat_101", "uniquefloat_102", "uniquefloat_103", "uniquefloat_104", "uniquefloat_105"
            , "uniquefloat_106", "uniquefloat_107", "uniquefloat_108", "uniquefloat_109", "uniquefloat_110"
            , "uniquefloat_111", "uniquefloat_112", "uniquefloat_113", "uniquefloat_114", "uniquefloat_115"
            , "uniquefloat_116", "uniquefloat_117", "uniquefloat_118", "uniquefloat_119", "uniquefloat_120"
            , "uniquefloat_121", "uniquefloat_122", "uniquefloat_123", "uniquefloat_124", "uniquefloat_125"
            , "uniquefloat_126", "uniquefloat_127", "uniquefloat_128", "uniquefloat_129", "uniquefloat_130"
            , "uniquefloat_131", "uniquefloat_132", "uniquefloat_133", "uniquefloat_134", "uniquefloat_135"
            , "uniquefloat_136", "uniquefloat_137", "uniquefloat_138", "uniquefloat_139", "uniquefloat_140"
            , "uniquefloat_141", "uniquefloat_142", "uniquefloat_143", "uniquefloat_144", "uniquefloat_145"
            , "uniquefloat_146", "uniquefloat_147", "uniquefloat_148", "uniquefloat_149", "uniquefloat_150"
            , "uniquefloat_151", "uniquefloat_152", "uniquefloat_153", "uniquefloat_154", "uniquefloat_155"
            , "uniquefloat_156", "uniquefloat_157", "uniquefloat_158", "uniquefloat_159", "uniquefloat_160"
            , "uniquefloat_161", "uniquefloat_162", "uniquefloat_163", "uniquefloat_164", "uniquefloat_165"
            , "uniquefloat_166", "uniquefloat_167", "uniquefloat_168", "uniquefloat_169", "uniquefloat_170"
            , "uniquefloat_171", "uniquefloat_172", "uniquefloat_173", "uniquefloat_174", "uniquefloat_175"
            , "uniquefloat_176", "uniquefloat_177", "uniquefloat_178", "uniquefloat_179", "uniquefloat_180"
            , "uniquefloat_181", "uniquefloat_182", "uniquefloat_183", "uniquefloat_184", "uniquefloat_185"
            , "uniquefloat_186", "uniquefloat_187", "uniquefloat_188", "uniquefloat_189", "uniquefloat_190"
            , "uniquefloat_191", "uniquefloat_192", "uniquefloat_193", "uniquefloat_194", "uniquefloat_195"
            , "uniquefloat_196", "uniquefloat_197", "uniquefloat_198", "uniquefloat_199", "uniquefloat_200"
            , "uniquejson_001"]
        for column in columns:
            if column not in modelResultDF.columns:
                modelResultDF[column] = None
        modelResultUploadDF = modelResultDF[columns]

        for column in columns:
            if column not in modelScoreDF.columns:
                modelScoreDF[column] = None
        modelScoreUploadDF = modelScoreDF[columns]

        insertInfoArr = [
            ["UseModel",usemodelVersion ,modelResultUploadDF]
            , ["ModelResult",modelVersion, modelResultUploadDF]
            , ["ModelScore" ,modelVersion,modelScoreUploadDF]
        ]

        for insertInfos in insertInfoArr :
            step = insertInfos[0]
            version = insertInfos[1]
            df = insertInfos[2]
            hdfsPath = "/user/GTW_PD/DB/Model/Usedata/product=[:ProductName]/project=[:Project]/step=[:Step]/version=[:Version]/dt=[:DateNoLine]" \
                .replace("[:ProductName]", productName) \
                .replace("[:Project]", project) \
                .replace("[:Version]", version) \
                .replace("[:DateNoLine]", dt) \
                .replace("[:Step]", step)
            hdfsFile = "[:ProductName]_[:Project]_[:Version]_[:DateNoLine]_[:Step].csv" \
                .replace("[:ProductName]", productName) \
                .replace("[:Project]", project) \
                .replace("[:Version]", version) \
                .replace("[:DateNoLine]", dt) \
                .replace("[:Step]", step)
            alterDropStr = "ALTER TABLE gtwpd.model_usedata DROP IF EXISTS PARTITION (product = '[:ProductName]' , project = '[:Project]' , version = '[:Version]' , dt = '[:DateNoLine]' , step = '[:Step]'  ) " \
                .replace("[:ProductName]", productName) \
                .replace("[:Project]", project) \
                .replace("[:Version]", version) \
                .replace("[:DateNoLine]", dt) \
                .replace("[:Step]", step)
            alterAddStr = "ALTER TABLE gtwpd.model_usedata ADD PARTITION (product = '[:ProductName]' , project = '[:Project]' , version = '[:Version]' , dt = '[:DateNoLine]' , step = '[:Step]'  ) location '[:HDFSPath]'" \
                .replace("[:ProductName]", productName) \
                .replace("[:Project]", project) \
                .replace("[:Version]", version) \
                .replace("[:DateNoLine]", dt) \
                .replace("[:HDFSPath]", hdfsPath) \
                .replace("[:Step]", step)

            df.to_csv("{}/{}/file/HDFSUploadTempFile/{}".format(productName,project,hdfsFile), sep="\t", index=None, header=None)
            hdfsCtrl.deleteDirs(hdfsPath)
            hdfsCtrl.upload(hdfsPath + "/" + hdfsFile, "{}/{}/file/HDFSUploadTempFile/{}".format(productName,project,hdfsFile))
            hiveCtrl.executeSQL_TCByCount(alterDropStr, 2)
            hiveCtrl.executeSQL_TCByCount(alterAddStr, 2)

        return "MakeUseModelFreeFuction", None , {"DataCount":21123}