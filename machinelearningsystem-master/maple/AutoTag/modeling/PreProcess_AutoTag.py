import pandas
from package.common.common.RawPreModel import RawPreModel
import datetime
import math
import numpy as np

class PreProcess_AutoTag() :

    # 輸入資料
    @classmethod
    def MakePreProcess_AutoTag_P0_1_1(self, makeInfo):
        orderSQL1 = """
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:PreProcessVersion]' , dt = '[:DateNoLine]' , step = 'PreProcess')
            SELECT 
                AA.commondata_001 as commondata_001
                , null as commondata_002
                , null as commondata_003
                , null as commondata_004
                , AA.commondata_005 as commondata_005
                , AA.commondata_006 as commondata_006
                , NULL AS commondata_007
                , NULL AS commondata_008
                , NULL AS commondata_009
                , NULL AS commondata_010
                , NULL AS commondata_011
                , NULL AS commondata_012
                , NULL AS commondata_013
                , NULL AS commondata_014
                , NULL AS commondata_015
                , (AA.uniquefloat_001 + AA.uniquefloat_002 + AA.uniquefloat_003 + AA.uniquefloat_004 + AA.uniquefloat_005 + AA.uniquefloat_006 + AA.uniquefloat_007 + AA.uniquefloat_008
                + AA.uniquefloat_145 + AA.uniquefloat_146 + AA.uniquefloat_147 + AA.uniquefloat_148 + AA.uniquefloat_149 + AA.uniquefloat_150 + AA.uniquefloat_151 + AA.uniquefloat_152)/3600/2/8 as UniqueFloat_001     
                , (AA.uniquefloat_009 + AA.uniquefloat_010 + AA.uniquefloat_011 + AA.uniquefloat_012 + AA.uniquefloat_013 + AA.uniquefloat_014 + AA.uniquefloat_015 + AA.uniquefloat_016  
                + AA.uniquefloat_153 + AA.uniquefloat_154 + AA.uniquefloat_155 + AA.uniquefloat_156 + AA.uniquefloat_157 + AA.uniquefloat_158 + AA.uniquefloat_159 + AA.uniquefloat_160)/3600/2/8 as UniqueFloat_002   
                , (AA.uniquefloat_017 + AA.uniquefloat_018 + AA.uniquefloat_019 + AA.uniquefloat_020 + AA.uniquefloat_021 + AA.uniquefloat_022 + AA.uniquefloat_023 + AA.uniquefloat_024 
                + AA.uniquefloat_161 + AA.uniquefloat_162 + AA.uniquefloat_163 + AA.uniquefloat_164 + AA.uniquefloat_165 + AA.uniquefloat_166 + AA.uniquefloat_167 + AA.uniquefloat_168)/3600/2/8 as UniqueFloat_003  
                , (AA.uniquefloat_025 + AA.uniquefloat_026 + AA.uniquefloat_027 + AA.uniquefloat_028 + AA.uniquefloat_029 + AA.uniquefloat_030 + AA.uniquefloat_031 + AA.uniquefloat_032
                + AA.uniquefloat_049 + AA.uniquefloat_050 + AA.uniquefloat_051 + AA.uniquefloat_052 + AA.uniquefloat_053 + AA.uniquefloat_054 + AA.uniquefloat_055 + AA.uniquefloat_056  
                + AA.uniquefloat_073 + AA.uniquefloat_074 + AA.uniquefloat_075 + AA.uniquefloat_076 + AA.uniquefloat_077 + AA.uniquefloat_078 + AA.uniquefloat_079 + AA.uniquefloat_080 
                + AA.uniquefloat_097 + AA.uniquefloat_098 + AA.uniquefloat_099 + AA.uniquefloat_100 + AA.uniquefloat_101 + AA.uniquefloat_102 + AA.uniquefloat_103 + AA.uniquefloat_104 
                + AA.uniquefloat_121 + AA.uniquefloat_122 + AA.uniquefloat_123 + AA.uniquefloat_124 + AA.uniquefloat_125 + AA.uniquefloat_126 + AA.uniquefloat_127 + AA.uniquefloat_128)/3600/5/8 as UniqueFloat_004  
                , (AA.uniquefloat_033 + AA.uniquefloat_034 + AA.uniquefloat_035 + AA.uniquefloat_036 + AA.uniquefloat_037 + AA.uniquefloat_038 + AA.uniquefloat_039 + AA.uniquefloat_040 
                + AA.uniquefloat_057 + AA.uniquefloat_058 + AA.uniquefloat_059 + AA.uniquefloat_060 + AA.uniquefloat_061 + AA.uniquefloat_062 + AA.uniquefloat_063 + AA.uniquefloat_064 
                + AA.uniquefloat_081 + AA.uniquefloat_082 + AA.uniquefloat_083 + AA.uniquefloat_084 + AA.uniquefloat_085 + AA.uniquefloat_086 + AA.uniquefloat_087 + AA.uniquefloat_088 
                + AA.uniquefloat_105 + AA.uniquefloat_106 + AA.uniquefloat_107 + AA.uniquefloat_108 + AA.uniquefloat_109 + AA.uniquefloat_110 + AA.uniquefloat_111 + AA.uniquefloat_112 
                + AA.uniquefloat_129 + AA.uniquefloat_130 + AA.uniquefloat_131 + AA.uniquefloat_132 + AA.uniquefloat_133 + AA.uniquefloat_134 + AA.uniquefloat_135 + AA.uniquefloat_136)/3600/5/8 as UniqueFloat_005
                , (AA.uniquefloat_041 + AA.uniquefloat_042 + AA.uniquefloat_043 + AA.uniquefloat_044 + AA.uniquefloat_045 + AA.uniquefloat_046 + AA.uniquefloat_047 + AA.uniquefloat_048 
                + AA.uniquefloat_065 + AA.uniquefloat_066 + AA.uniquefloat_067 + AA.uniquefloat_068 + AA.uniquefloat_069 + AA.uniquefloat_070 + AA.uniquefloat_071 + AA.uniquefloat_072 
                + AA.uniquefloat_089 + AA.uniquefloat_090 + AA.uniquefloat_091 + AA.uniquefloat_092 + AA.uniquefloat_093 + AA.uniquefloat_094 + AA.uniquefloat_095 + AA.uniquefloat_096 
                + AA.uniquefloat_113 + AA.uniquefloat_114 + AA.uniquefloat_115 + AA.uniquefloat_116 + AA.uniquefloat_117 + AA.uniquefloat_118 + AA.uniquefloat_119 + AA.uniquefloat_120 
                + AA.uniquefloat_137 + AA.uniquefloat_138 + AA.uniquefloat_139 + AA.uniquefloat_140 + AA.uniquefloat_141 + AA.uniquefloat_142 + AA.uniquefloat_143 + AA.uniquefloat_144)/3600/5/8 as UniqueFloat_006
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
                AND AA.product = '[:ProductName]' 
                AND AA.project = '[:Project]' 
                AND AA.version = '[:RawDataVersion]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.step = 'RawData'; 
        """
        return "MakePreProcessOrderSQLInsert", [orderSQL1]

    # 標籤文本
    @classmethod
    def MakePreProcess_AutoTag_P0_1_2(self, makeInfo):
        # df = ""
        tagDataMap = {
            'weekdays' : {
                'enname' : 'weekdays'
                , 'cnname' : '平日型'
                , 'tagorder' : '1'
                , 'memo' : '平日上線的玩家'
                , 'jsonmessage' : '{}'
                , 'tagshape':  [ 0, 0, 0, 1, 1, 1 ]
            }, 'holiday' : {
                'enname' : 'holiday'
                , 'cnname' : '假日型'
                , 'tagorder' : '2'
                , 'memo' : '平日上線的玩家'
                , 'jsonmessage' : '{}'
                , 'tagshape':  [ 1, 1, 1, 0, 0, 0 ]
            }, 'work' : {
                'enname' : 'work'
                , 'cnname' : '工作型'
                , 'tagorder' : '3'
                , 'memo' : '平常工作上課，休息時間上線的玩家'
                , 'jsonmessage' : '{}'
                , 'tagshape':  [ 0, 0, 1, 0, 1, 1 ]
            }, 'night' : {
                'enname' : 'night'
                , 'cnname' : '夜晚型'
                , 'tagorder' : '4'
                , 'memo' : '夜晚上線的玩家'
                , 'jsonmessage' : '{}'
                , 'tagshape':  [ 1, 0, 1, 1, 0, 1 ]
            }, 'full' : {
                'enname' : 'full'
                , 'cnname' : '全勤型'
                , 'tagorder' : '5'
                , 'memo' : '每天都會上的玩家'
                , 'jsonmessage' : '{}'
                , 'tagshape':  [ 1, 1, 1, 1, 1, 1 ]
            }
        }

        tagDictionarys = []
        for key in tagDataMap.keys() :
            tagData = tagDataMap [key]
            tagDictionary = []
            tagDictionary.append(tagData['enname'])
            tagDictionary.append(tagData['cnname'])
            tagDictionary.append(tagData['tagorder'])
            tagDictionary.append(tagData['memo'])
            tagDictionary.append(tagData['jsonmessage'])
            tagDictionary.append(tagData['tagshape'][0])
            tagDictionary.append(tagData['tagshape'][1])
            tagDictionary.append(tagData['tagshape'][2])
            tagDictionary.append(tagData['tagshape'][3])
            tagDictionary.append(tagData['tagshape'][4])
            tagDictionary.append(tagData['tagshape'][5])
            tagDictionarys.append(tagDictionary)

        df = pandas.DataFrame(tagDictionarys)

        df.columns = [
            "commondata_001"  # 標籤名稱(英)
            , "commondata_002"  # 標籤名稱(中)
            , "commondata_003"  # 標籤順序
            , "commondata_004"  # 標籤說明
            , "commondata_005"  # 其他說明JSON
            , "uniquefloat_001"  # 假日凌晨
            , "uniquefloat_002"  # 假日早上
            , "uniquefloat_003"  # 假日晚上
            , "uniquefloat_004"  # 平日凌晨
            , "uniquefloat_005"  # 平日早上
            , "uniquefloat_006"  # 平日晚上
        ]

        return "MakePreProcessFileInsertOverwrite", df

    # 內積結果
    @classmethod
    def MakePreProcess_AutoTag_P0_1_3(self, makeInfo):
        orderSQL1 = """
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:PreProcessVersion]' , dt = '[:DateNoLine]' , step = 'PreProcess')
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
                , BB.commondata_001 as commondata_011
                , BB.commondata_002 as commondata_012
                , BB.commondata_003 as commondata_013 
                , BB.commondata_004 as commondata_014
                , BB.commondata_005 as commondata_015 
                , AA.uniquefloat_001 as uniquefloat_001
                , AA.uniquefloat_002 as uniquefloat_002
                , AA.uniquefloat_003 as uniquefloat_003
                , AA.uniquefloat_004 as uniquefloat_004
                , AA.uniquefloat_005 as uniquefloat_005
                , AA.uniquefloat_006 as uniquefloat_006
                , AA.uniquefloat_007 as uniquefloat_007
                , AA.uniquefloat_008 as uniquefloat_008
                , AA.uniquefloat_009 as uniquefloat_009
                , AA.uniquefloat_010 as uniquefloat_010
                , AA.uniquefloat_011 as uniquefloat_011
                , AA.uniquefloat_012 as uniquefloat_012
                , AA.uniquefloat_013 as uniquefloat_013
                , AA.uniquefloat_014 as uniquefloat_014
                , AA.uniquefloat_015 as uniquefloat_015
                , AA.uniquefloat_016 as uniquefloat_016
                , AA.uniquefloat_017 as uniquefloat_017
                , AA.uniquefloat_018 as uniquefloat_018
                , AA.uniquefloat_019 as uniquefloat_019
                , AA.uniquefloat_020 as uniquefloat_020
                , AA.uniquefloat_021 as uniquefloat_021
                , AA.uniquefloat_022 as uniquefloat_022
                , AA.uniquefloat_023 as uniquefloat_023
                , AA.uniquefloat_024 as uniquefloat_024
                , AA.uniquefloat_025 as uniquefloat_025
                , AA.uniquefloat_026 as uniquefloat_026
                , AA.uniquefloat_027 as uniquefloat_027
                , AA.uniquefloat_028 as uniquefloat_028
                , AA.uniquefloat_029 as uniquefloat_029
                , AA.uniquefloat_030 as uniquefloat_030
                , AA.uniquefloat_031 as uniquefloat_031
                , AA.uniquefloat_032 as uniquefloat_032
                , AA.uniquefloat_033 as uniquefloat_033
                , AA.uniquefloat_034 as uniquefloat_034
                , AA.uniquefloat_035 as uniquefloat_035
                , AA.uniquefloat_036 as uniquefloat_036
                , AA.uniquefloat_037 as uniquefloat_037
                , AA.uniquefloat_038 as uniquefloat_038
                , AA.uniquefloat_039 as uniquefloat_039
                , AA.uniquefloat_040 as uniquefloat_040
                , AA.uniquefloat_041 as uniquefloat_041
                , AA.uniquefloat_042 as uniquefloat_042
                , AA.uniquefloat_043 as uniquefloat_043
                , AA.uniquefloat_044 as uniquefloat_044
                , AA.uniquefloat_045 as uniquefloat_045
                , AA.uniquefloat_046 as uniquefloat_046
                , AA.uniquefloat_047 as uniquefloat_047
                , AA.uniquefloat_048 as uniquefloat_048
                , AA.uniquefloat_049 as uniquefloat_049
                , AA.uniquefloat_050 as uniquefloat_050
                , AA.uniquefloat_001 * BB.uniquefloat_001 as uniquefloat_051
                , AA.uniquefloat_002 * BB.uniquefloat_002 as uniquefloat_052
                , AA.uniquefloat_003 * BB.uniquefloat_003 as uniquefloat_053
                , AA.uniquefloat_004 * BB.uniquefloat_004 as uniquefloat_054
                , AA.uniquefloat_005 * BB.uniquefloat_005 as uniquefloat_055
                , AA.uniquefloat_006 * BB.uniquefloat_006 as uniquefloat_056
                , AA.uniquefloat_007 * BB.uniquefloat_007 as uniquefloat_057
                , AA.uniquefloat_008 * BB.uniquefloat_008 as uniquefloat_058
                , AA.uniquefloat_009 * BB.uniquefloat_009 as uniquefloat_059
                , AA.uniquefloat_010 * BB.uniquefloat_010 as uniquefloat_060
                , AA.uniquefloat_011 * BB.uniquefloat_011 as uniquefloat_061
                , AA.uniquefloat_012 * BB.uniquefloat_012 as uniquefloat_062
                , AA.uniquefloat_013 * BB.uniquefloat_013 as uniquefloat_063
                , AA.uniquefloat_014 * BB.uniquefloat_014 as uniquefloat_064
                , AA.uniquefloat_015 * BB.uniquefloat_015 as uniquefloat_065
                , AA.uniquefloat_016 * BB.uniquefloat_016 as uniquefloat_066
                , AA.uniquefloat_017 * BB.uniquefloat_017 as uniquefloat_067
                , AA.uniquefloat_018 * BB.uniquefloat_018 as uniquefloat_068
                , AA.uniquefloat_019 * BB.uniquefloat_019 as uniquefloat_069
                , AA.uniquefloat_020 * BB.uniquefloat_020 as uniquefloat_070
                , AA.uniquefloat_021 * BB.uniquefloat_021 as uniquefloat_071
                , AA.uniquefloat_022 * BB.uniquefloat_022 as uniquefloat_072
                , AA.uniquefloat_023 * BB.uniquefloat_023 as uniquefloat_073
                , AA.uniquefloat_024 * BB.uniquefloat_024 as uniquefloat_074
                , AA.uniquefloat_025 * BB.uniquefloat_025 as uniquefloat_075
                , AA.uniquefloat_026 * BB.uniquefloat_026 as uniquefloat_076
                , AA.uniquefloat_027 * BB.uniquefloat_027 as uniquefloat_077
                , AA.uniquefloat_028 * BB.uniquefloat_028 as uniquefloat_078
                , AA.uniquefloat_029 * BB.uniquefloat_029 as uniquefloat_079
                , AA.uniquefloat_030 * BB.uniquefloat_030 as uniquefloat_080
                , AA.uniquefloat_031 * BB.uniquefloat_031 as uniquefloat_081
                , AA.uniquefloat_032 * BB.uniquefloat_032 as uniquefloat_082
                , AA.uniquefloat_033 * BB.uniquefloat_033 as uniquefloat_083
                , AA.uniquefloat_034 * BB.uniquefloat_034 as uniquefloat_084
                , AA.uniquefloat_035 * BB.uniquefloat_035 as uniquefloat_085
                , AA.uniquefloat_036 * BB.uniquefloat_036 as uniquefloat_086
                , AA.uniquefloat_037 * BB.uniquefloat_037 as uniquefloat_087
                , AA.uniquefloat_038 * BB.uniquefloat_038 as uniquefloat_088
                , AA.uniquefloat_039 * BB.uniquefloat_039 as uniquefloat_089
                , AA.uniquefloat_040 * BB.uniquefloat_040 as uniquefloat_090
                , AA.uniquefloat_041 * BB.uniquefloat_041 as uniquefloat_091
                , AA.uniquefloat_042 * BB.uniquefloat_042 as uniquefloat_092
                , AA.uniquefloat_043 * BB.uniquefloat_043 as uniquefloat_093
                , AA.uniquefloat_044 * BB.uniquefloat_044 as uniquefloat_094
                , AA.uniquefloat_045 * BB.uniquefloat_045 as uniquefloat_095
                , AA.uniquefloat_046 * BB.uniquefloat_046 as uniquefloat_096
                , AA.uniquefloat_047 * BB.uniquefloat_047 as uniquefloat_097
                , AA.uniquefloat_048 * BB.uniquefloat_048 as uniquefloat_098
                , AA.uniquefloat_049 * BB.uniquefloat_049 as uniquefloat_099
                , AA.uniquefloat_050 * BB.uniquefloat_050 as uniquefloat_100
                , null as uniquefloat_101
                , null as uniquefloat_102
                , null as uniquefloat_103
                , null as uniquefloat_104
                , null as uniquefloat_105
                , null as uniquefloat_106
                , null as uniquefloat_107
                , null as uniquefloat_108
                , null as uniquefloat_109
                , null as uniquefloat_110
                , null as uniquefloat_111
                , null as uniquefloat_112
                , null as uniquefloat_113
                , null as uniquefloat_114
                , null as uniquefloat_115
                , null as uniquefloat_116
                , null as uniquefloat_117
                , null as uniquefloat_118
                , null as uniquefloat_119
                , null as uniquefloat_120
                , null as uniquefloat_121
                , null as uniquefloat_122
                , null as uniquefloat_123
                , null as uniquefloat_124
                , null as uniquefloat_125
                , null as uniquefloat_126
                , null as uniquefloat_127
                , null as uniquefloat_128
                , null as uniquefloat_129
                , null as uniquefloat_130
                , null as uniquefloat_131
                , null as uniquefloat_132
                , null as uniquefloat_133
                , null as uniquefloat_134
                , null as uniquefloat_135
                , null as uniquefloat_136
                , null as uniquefloat_137
                , null as uniquefloat_138
                , null as uniquefloat_139
                , null as uniquefloat_140
                , null as uniquefloat_141
                , null as uniquefloat_142
                , null as uniquefloat_143
                , null as uniquefloat_144
                , null as uniquefloat_145
                , null as uniquefloat_146
                , null as uniquefloat_147
                , null as uniquefloat_148
                , null as uniquefloat_149
                , null as uniquefloat_150
                , null as uniquefloat_151
                , null as uniquefloat_152
                , null as uniquefloat_153
                , null as uniquefloat_154
                , null as uniquefloat_155
                , null as uniquefloat_156
                , null as uniquefloat_157
                , null as uniquefloat_158
                , null as uniquefloat_159
                , null as uniquefloat_160
                , null as uniquefloat_161
                , null as uniquefloat_162
                , null as uniquefloat_163
                , null as uniquefloat_164
                , null as uniquefloat_165
                , null as uniquefloat_166
                , null as uniquefloat_167
                , null as uniquefloat_168
                , null as uniquefloat_169
                , null as uniquefloat_170
                , null as uniquefloat_171
                , null as uniquefloat_172
                , null as uniquefloat_173
                , null as uniquefloat_174
                , null as uniquefloat_175
                , null as uniquefloat_176
                , null as uniquefloat_177
                , null as uniquefloat_178
                , null as uniquefloat_179
                , null as uniquefloat_180
                , null as uniquefloat_181
                , null as uniquefloat_182
                , null as uniquefloat_183
                , null as uniquefloat_184
                , null as uniquefloat_185
                , null as uniquefloat_186
                , null as uniquefloat_187
                , null as uniquefloat_188
                , null as uniquefloat_189
                , null as uniquefloat_190
                , coalesce(AA.uniquefloat_001 * BB.uniquefloat_001 ,0 ) + coalesce(AA.uniquefloat_002 * BB.uniquefloat_002 ,0 ) + coalesce(AA.uniquefloat_003 * BB.uniquefloat_003 ,0 ) + coalesce(AA.uniquefloat_004 * BB.uniquefloat_004 ,0 ) + coalesce(AA.uniquefloat_005 * BB.uniquefloat_005 ,0 ) + coalesce(AA.uniquefloat_006 * BB.uniquefloat_006 ,0 ) + coalesce(AA.uniquefloat_007 * BB.uniquefloat_007 ,0 ) + coalesce(AA.uniquefloat_008 * BB.uniquefloat_008 ,0 ) + coalesce(AA.uniquefloat_009 * BB.uniquefloat_009 ,0 ) + coalesce(AA.uniquefloat_010 * BB.uniquefloat_010 ,0 ) + coalesce(AA.uniquefloat_011 * BB.uniquefloat_011 ,0 ) + coalesce(AA.uniquefloat_012 * BB.uniquefloat_012 ,0 ) + coalesce(AA.uniquefloat_013 * BB.uniquefloat_013 ,0 ) + coalesce(AA.uniquefloat_014 * BB.uniquefloat_014 ,0 ) + coalesce(AA.uniquefloat_015 * BB.uniquefloat_015 ,0 ) + coalesce(AA.uniquefloat_016 * BB.uniquefloat_016 ,0 ) + coalesce(AA.uniquefloat_017 * BB.uniquefloat_017 ,0 ) + coalesce(AA.uniquefloat_018 * BB.uniquefloat_018 ,0 ) + coalesce(AA.uniquefloat_019 * BB.uniquefloat_019 ,0 ) + coalesce(AA.uniquefloat_020 * BB.uniquefloat_020 ,0 ) + coalesce(AA.uniquefloat_021 * BB.uniquefloat_021 ,0 ) + coalesce(AA.uniquefloat_022 * BB.uniquefloat_022 ,0 ) + coalesce(AA.uniquefloat_023 * BB.uniquefloat_023 ,0 ) + coalesce(AA.uniquefloat_024 * BB.uniquefloat_024 ,0 ) + coalesce(AA.uniquefloat_025 * BB.uniquefloat_025 ,0 ) + coalesce(AA.uniquefloat_026 * BB.uniquefloat_026 ,0 ) + coalesce(AA.uniquefloat_027 * BB.uniquefloat_027 ,0 ) + coalesce(AA.uniquefloat_028 * BB.uniquefloat_028 ,0 ) + coalesce(AA.uniquefloat_029 * BB.uniquefloat_029 ,0 ) + coalesce(AA.uniquefloat_030 * BB.uniquefloat_030 ,0 ) + coalesce(AA.uniquefloat_031 * BB.uniquefloat_031 ,0 ) + coalesce(AA.uniquefloat_032 * BB.uniquefloat_032 ,0 ) + coalesce(AA.uniquefloat_033 * BB.uniquefloat_033 ,0 ) + coalesce(AA.uniquefloat_034 * BB.uniquefloat_034 ,0 ) + coalesce(AA.uniquefloat_035 * BB.uniquefloat_035 ,0 ) + coalesce(AA.uniquefloat_036 * BB.uniquefloat_036 ,0 ) + coalesce(AA.uniquefloat_037 * BB.uniquefloat_037 ,0 ) + coalesce(AA.uniquefloat_038 * BB.uniquefloat_038 ,0 ) + coalesce(AA.uniquefloat_039 * BB.uniquefloat_039 ,0 ) + coalesce(AA.uniquefloat_040 * BB.uniquefloat_040 ,0 ) + coalesce(AA.uniquefloat_041 * BB.uniquefloat_041 ,0 ) + coalesce(AA.uniquefloat_042 * BB.uniquefloat_042 ,0 ) + coalesce(AA.uniquefloat_043 * BB.uniquefloat_043 ,0 ) + coalesce(AA.uniquefloat_044 * BB.uniquefloat_044 ,0 ) + coalesce(AA.uniquefloat_045 * BB.uniquefloat_045 ,0 ) + coalesce(AA.uniquefloat_046 * BB.uniquefloat_046 ,0 ) + coalesce(AA.uniquefloat_047 * BB.uniquefloat_047 ,0 ) + coalesce(AA.uniquefloat_048 * BB.uniquefloat_048 ,0 ) + coalesce(AA.uniquefloat_049 * BB.uniquefloat_049 ,0 ) + coalesce(AA.uniquefloat_050 * BB.uniquefloat_050 ,0 )  as uniquefloat_191
                ,  sqrt(power(coalesce(AA.uniquefloat_001,0),2) + power(coalesce(AA.uniquefloat_002,0),2) + power(coalesce(AA.uniquefloat_003,0),2) + power(coalesce(AA.uniquefloat_004,0),2) + power(coalesce(AA.uniquefloat_005,0),2) + power(coalesce(AA.uniquefloat_006,0),2) + power(coalesce(AA.uniquefloat_007,0),2) + power(coalesce(AA.uniquefloat_008,0),2) + power(coalesce(AA.uniquefloat_009,0),2) + power(coalesce(AA.uniquefloat_010,0),2) + power(coalesce(AA.uniquefloat_011,0),2) + power(coalesce(AA.uniquefloat_012,0),2) + power(coalesce(AA.uniquefloat_013,0),2) + power(coalesce(AA.uniquefloat_014,0),2) + power(coalesce(AA.uniquefloat_015,0),2) + power(coalesce(AA.uniquefloat_016,0),2) + power(coalesce(AA.uniquefloat_017,0),2) + power(coalesce(AA.uniquefloat_018,0),2) + power(coalesce(AA.uniquefloat_019,0),2) + power(coalesce(AA.uniquefloat_020,0),2) + power(coalesce(AA.uniquefloat_021,0),2) + power(coalesce(AA.uniquefloat_022,0),2) + power(coalesce(AA.uniquefloat_023,0),2) + power(coalesce(AA.uniquefloat_024,0),2) + power(coalesce(AA.uniquefloat_025,0),2) + power(coalesce(AA.uniquefloat_026,0),2) + power(coalesce(AA.uniquefloat_027,0),2) + power(coalesce(AA.uniquefloat_028,0),2) + power(coalesce(AA.uniquefloat_029,0),2) + power(coalesce(AA.uniquefloat_030,0),2) + power(coalesce(AA.uniquefloat_031,0),2) + power(coalesce(AA.uniquefloat_032,0),2) + power(coalesce(AA.uniquefloat_033,0),2) + power(coalesce(AA.uniquefloat_034,0),2) + power(coalesce(AA.uniquefloat_035,0),2) + power(coalesce(AA.uniquefloat_036,0),2) + power(coalesce(AA.uniquefloat_037,0),2) + power(coalesce(AA.uniquefloat_038,0),2) + power(coalesce(AA.uniquefloat_039,0),2) + power(coalesce(AA.uniquefloat_040,0),2) + power(coalesce(AA.uniquefloat_041,0),2) + power(coalesce(AA.uniquefloat_042,0),2) + power(coalesce(AA.uniquefloat_043,0),2) + power(coalesce(AA.uniquefloat_044,0),2) + power(coalesce(AA.uniquefloat_045,0),2) + power(coalesce(AA.uniquefloat_046,0),2) + power(coalesce(AA.uniquefloat_047,0),2) + power(coalesce(AA.uniquefloat_048,0),2) + power(coalesce(AA.uniquefloat_049,0),2) + power(coalesce(AA.uniquefloat_050,0),2) ) as uniquefloat_192
                ,  sqrt(power(coalesce(BB.uniquefloat_001,0),2) + power(coalesce(BB.uniquefloat_002,0),2) + power(coalesce(BB.uniquefloat_003,0),2) + power(coalesce(BB.uniquefloat_004,0),2) + power(coalesce(BB.uniquefloat_005,0),2) + power(coalesce(BB.uniquefloat_006,0),2) + power(coalesce(BB.uniquefloat_007,0),2) + power(coalesce(BB.uniquefloat_008,0),2) + power(coalesce(BB.uniquefloat_009,0),2) + power(coalesce(BB.uniquefloat_010,0),2) + power(coalesce(BB.uniquefloat_011,0),2) + power(coalesce(BB.uniquefloat_012,0),2) + power(coalesce(BB.uniquefloat_013,0),2) + power(coalesce(BB.uniquefloat_014,0),2) + power(coalesce(BB.uniquefloat_015,0),2) + power(coalesce(BB.uniquefloat_016,0),2) + power(coalesce(BB.uniquefloat_017,0),2) + power(coalesce(BB.uniquefloat_018,0),2) + power(coalesce(BB.uniquefloat_019,0),2) + power(coalesce(BB.uniquefloat_020,0),2) + power(coalesce(BB.uniquefloat_021,0),2) + power(coalesce(BB.uniquefloat_022,0),2) + power(coalesce(BB.uniquefloat_023,0),2) + power(coalesce(BB.uniquefloat_024,0),2) + power(coalesce(BB.uniquefloat_025,0),2) + power(coalesce(BB.uniquefloat_026,0),2) + power(coalesce(BB.uniquefloat_027,0),2) + power(coalesce(BB.uniquefloat_028,0),2) + power(coalesce(BB.uniquefloat_029,0),2) + power(coalesce(BB.uniquefloat_030,0),2) + power(coalesce(BB.uniquefloat_031,0),2) + power(coalesce(BB.uniquefloat_032,0),2) + power(coalesce(BB.uniquefloat_033,0),2) + power(coalesce(BB.uniquefloat_034,0),2) + power(coalesce(BB.uniquefloat_035,0),2) + power(coalesce(BB.uniquefloat_036,0),2) + power(coalesce(BB.uniquefloat_037,0),2) + power(coalesce(BB.uniquefloat_038,0),2) + power(coalesce(BB.uniquefloat_039,0),2) + power(coalesce(BB.uniquefloat_040,0),2) + power(coalesce(BB.uniquefloat_041,0),2) + power(coalesce(BB.uniquefloat_042,0),2) + power(coalesce(BB.uniquefloat_043,0),2) + power(coalesce(BB.uniquefloat_044,0),2) + power(coalesce(BB.uniquefloat_045,0),2) + power(coalesce(BB.uniquefloat_046,0),2) + power(coalesce(BB.uniquefloat_047,0),2) + power(coalesce(BB.uniquefloat_048,0),2) + power(coalesce(BB.uniquefloat_049,0),2) + power(coalesce(BB.uniquefloat_050,0),2) ) as uniquefloat_193
                , null as uniquefloat_194
                , null as uniquefloat_195
                , null as uniquefloat_196
                , null as uniquefloat_197
                , null as uniquefloat_198
                , null as uniquefloat_199
                , null as uniquefloat_200
                , NULL AS UniqueJson_001 
            FROM gtwpd.model_usedata AA
            INNER JOIN gtwpd.model_usedata BB ON 1 = 1 
                AND BB.product = '[:ProductName]'
                AND BB.project = '[:Project]'
                AND BB.step = 'PreProcess'
                AND BB.version = '[:TagTextPreProcessVersion]'
                AND BB.dt = '[:DateNoLine]'
            WHERE 1 = 1 
                AND AA.product = '[:ProductName]'
                AND AA.project = '[:Project]'
                AND AA.step = 'PreProcess'
                AND AA.version = '[:TagDataPreProcessVersion]'
                AND AA.dt = '[:DateNoLine]';
        """

        parameter = makeInfo["parameter"]
        preProcessParameter = parameter["PreProcess"]
        sqlReplaceArr = preProcessParameter["sqlReplaceArr"]

        for sqlReplace in sqlReplaceArr :
            orderSQL1 = orderSQL1.replace(sqlReplace[0],sqlReplace[1])

        return "MakePreProcessOrderSQLInsert", [orderSQL1]

    # 標籤機率
    @classmethod
    def MakePreProcess_AutoTag_P0_1_4(self, makeInfo):
        orderSQL1 = """
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:PreProcessVersion]' , dt = '[:DateNoLine]' , step = 'PreProcess')
            SELECT 
 			    AAAA.commondata_001 
                , AAAA.commondata_002 
                , AAAA.commondata_003 
                , AAAA.commondata_004 
                , AAAA.commondata_005 
                , AAAA.commondata_006 
                , AAAA.commondata_007 
                , AAAA.commondata_008 
                , AAAA.commondata_009 
                , AAAA.commondata_010 
                , null as commondata_011 
                , null as commondata_012 
                , null as commondata_013 
                , null as commondata_014 
                , null as commondata_015 
                , AAAA.uniquefloat_001 as uniquefloat_001
                , AAAA.uniquefloat_002 as uniquefloat_002
                , AAAA.uniquefloat_003 as uniquefloat_003
                , AAAA.uniquefloat_004 as uniquefloat_004
                , AAAA.uniquefloat_005 as uniquefloat_005
                , AAAA.uniquefloat_006 as uniquefloat_006
                , AAAA.uniquefloat_007 as uniquefloat_007
                , AAAA.uniquefloat_008 as uniquefloat_008
                , AAAA.uniquefloat_009 as uniquefloat_009
                , AAAA.uniquefloat_010 as uniquefloat_010
                , AAAA.uniquefloat_011 as uniquefloat_011
                , AAAA.uniquefloat_012 as uniquefloat_012
                , AAAA.uniquefloat_013 as uniquefloat_013
                , AAAA.uniquefloat_014 as uniquefloat_014
                , AAAA.uniquefloat_015 as uniquefloat_015
                , AAAA.uniquefloat_016 as uniquefloat_016
                , AAAA.uniquefloat_017 as uniquefloat_017
                , AAAA.uniquefloat_018 as uniquefloat_018
                , AAAA.uniquefloat_019 as uniquefloat_019
                , AAAA.uniquefloat_020 as uniquefloat_020
                , AAAA.uniquefloat_021 as uniquefloat_021
                , AAAA.uniquefloat_022 as uniquefloat_022
                , AAAA.uniquefloat_023 as uniquefloat_023
                , AAAA.uniquefloat_024 as uniquefloat_024
                , AAAA.uniquefloat_025 as uniquefloat_025
                , AAAA.uniquefloat_026 as uniquefloat_026
                , AAAA.uniquefloat_027 as uniquefloat_027
                , AAAA.uniquefloat_028 as uniquefloat_028
                , AAAA.uniquefloat_029 as uniquefloat_029
                , AAAA.uniquefloat_030 as uniquefloat_030
                , AAAA.uniquefloat_031 as uniquefloat_031
                , AAAA.uniquefloat_032 as uniquefloat_032
                , AAAA.uniquefloat_033 as uniquefloat_033
                , AAAA.uniquefloat_034 as uniquefloat_034
                , AAAA.uniquefloat_035 as uniquefloat_035
                , AAAA.uniquefloat_036 as uniquefloat_036
                , AAAA.uniquefloat_037 as uniquefloat_037
                , AAAA.uniquefloat_038 as uniquefloat_038
                , AAAA.uniquefloat_039 as uniquefloat_039
                , AAAA.uniquefloat_040 as uniquefloat_040
                , AAAA.uniquefloat_041 as uniquefloat_041
                , AAAA.uniquefloat_042 as uniquefloat_042
                , AAAA.uniquefloat_043 as uniquefloat_043
                , AAAA.uniquefloat_044 as uniquefloat_044
                , AAAA.uniquefloat_045 as uniquefloat_045
                , AAAA.uniquefloat_046 as uniquefloat_046
                , AAAA.uniquefloat_047 as uniquefloat_047
                , AAAA.uniquefloat_048 as uniquefloat_048
                , AAAA.uniquefloat_049 as uniquefloat_049
                , AAAA.uniquefloat_050 as uniquefloat_050
                , AAAA.uniquefloat_051 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_051
                , AAAA.uniquefloat_052 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_052
                , AAAA.uniquefloat_053 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_053
                , AAAA.uniquefloat_054 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_054
                , AAAA.uniquefloat_055 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_055
                , AAAA.uniquefloat_056 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_056
                , AAAA.uniquefloat_057 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_057
                , AAAA.uniquefloat_058 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_058
                , AAAA.uniquefloat_059 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_059
                , AAAA.uniquefloat_060 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_060
                , AAAA.uniquefloat_061 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_061
                , AAAA.uniquefloat_062 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_062
                , AAAA.uniquefloat_063 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_063
                , AAAA.uniquefloat_064 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_064
                , AAAA.uniquefloat_065 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_065
                , AAAA.uniquefloat_066 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_066
                , AAAA.uniquefloat_067 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_067
                , AAAA.uniquefloat_068 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_068
                , AAAA.uniquefloat_069 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_069
                , AAAA.uniquefloat_070 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_070
                , AAAA.uniquefloat_071 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_071
                , AAAA.uniquefloat_072 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_072
                , AAAA.uniquefloat_073 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_073
                , AAAA.uniquefloat_074 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_074
                , AAAA.uniquefloat_075 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_075
                , AAAA.uniquefloat_076 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_076
                , AAAA.uniquefloat_077 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_077
                , AAAA.uniquefloat_078 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_078
                , AAAA.uniquefloat_079 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_079
                , AAAA.uniquefloat_080 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_080
                , AAAA.uniquefloat_081 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_081
                , AAAA.uniquefloat_082 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_082
                , AAAA.uniquefloat_083 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_083
                , AAAA.uniquefloat_084 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_084
                , AAAA.uniquefloat_085 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_085
                , AAAA.uniquefloat_086 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_086
                , AAAA.uniquefloat_087 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_087
                , AAAA.uniquefloat_088 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_088
                , AAAA.uniquefloat_089 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_089
                , AAAA.uniquefloat_090 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_090
                , AAAA.uniquefloat_091 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_091
                , AAAA.uniquefloat_092 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_092
                , AAAA.uniquefloat_093 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_093
                , AAAA.uniquefloat_094 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_094
                , AAAA.uniquefloat_095 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_095
                , AAAA.uniquefloat_096 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_096
                , AAAA.uniquefloat_097 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_097
                , AAAA.uniquefloat_098 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_098
                , AAAA.uniquefloat_099 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_099
                , AAAA.uniquefloat_100 / (AAAA.uniquefloat_051 + AAAA.uniquefloat_052 + AAAA.uniquefloat_053 + AAAA.uniquefloat_054 + AAAA.uniquefloat_055 + AAAA.uniquefloat_056 + AAAA.uniquefloat_057 + AAAA.uniquefloat_058 + AAAA.uniquefloat_059 + AAAA.uniquefloat_060 + AAAA.uniquefloat_061 + AAAA.uniquefloat_062 + AAAA.uniquefloat_063 + AAAA.uniquefloat_064 + AAAA.uniquefloat_065 + AAAA.uniquefloat_066 + AAAA.uniquefloat_067 + AAAA.uniquefloat_068 + AAAA.uniquefloat_069 + AAAA.uniquefloat_070 + AAAA.uniquefloat_071 + AAAA.uniquefloat_072 + AAAA.uniquefloat_073 + AAAA.uniquefloat_074 + AAAA.uniquefloat_075 + AAAA.uniquefloat_076 + AAAA.uniquefloat_077 + AAAA.uniquefloat_078 + AAAA.uniquefloat_079 + AAAA.uniquefloat_080 + AAAA.uniquefloat_081 + AAAA.uniquefloat_082 + AAAA.uniquefloat_083 + AAAA.uniquefloat_084 + AAAA.uniquefloat_085 + AAAA.uniquefloat_086 + AAAA.uniquefloat_087 + AAAA.uniquefloat_088 + AAAA.uniquefloat_089 + AAAA.uniquefloat_090 + AAAA.uniquefloat_091 + AAAA.uniquefloat_092 + AAAA.uniquefloat_093 + AAAA.uniquefloat_094 + AAAA.uniquefloat_095 + AAAA.uniquefloat_096 + AAAA.uniquefloat_097 + AAAA.uniquefloat_098 + AAAA.uniquefloat_099 + AAAA.uniquefloat_100) as uniquefloat_100
                , null as uniquefloat_101
                , null as uniquefloat_102
                , null as uniquefloat_103
                , null as uniquefloat_104
                , null as uniquefloat_105
                , null as uniquefloat_106
                , null as uniquefloat_107
                , null as uniquefloat_108
                , null as uniquefloat_109
                , null as uniquefloat_110
                , null as uniquefloat_111
                , null as uniquefloat_112
                , null as uniquefloat_113
                , null as uniquefloat_114
                , null as uniquefloat_115
                , null as uniquefloat_116
                , null as uniquefloat_117
                , null as uniquefloat_118
                , null as uniquefloat_119
                , null as uniquefloat_120
                , null as uniquefloat_121
                , null as uniquefloat_122
                , null as uniquefloat_123
                , null as uniquefloat_124
                , null as uniquefloat_125
                , null as uniquefloat_126
                , null as uniquefloat_127
                , null as uniquefloat_128
                , null as uniquefloat_129
                , null as uniquefloat_130
                , null as uniquefloat_131
                , null as uniquefloat_132
                , null as uniquefloat_133
                , null as uniquefloat_134
                , null as uniquefloat_135
                , null as uniquefloat_136
                , null as uniquefloat_137
                , null as uniquefloat_138
                , null as uniquefloat_139
                , null as uniquefloat_140
                , null as uniquefloat_141
                , null as uniquefloat_142
                , null as uniquefloat_143
                , null as uniquefloat_144
                , null as uniquefloat_145
                , null as uniquefloat_146
                , null as uniquefloat_147
                , null as uniquefloat_148
                , null as uniquefloat_149
                , null as uniquefloat_150
                , null as uniquefloat_151
                , null as uniquefloat_152
                , null as uniquefloat_153
                , null as uniquefloat_154
                , null as uniquefloat_155
                , null as uniquefloat_156
                , null as uniquefloat_157
                , null as uniquefloat_158
                , null as uniquefloat_159
                , null as uniquefloat_160
                , null as uniquefloat_161
                , null as uniquefloat_162
                , null as uniquefloat_163
                , null as uniquefloat_164
                , null as uniquefloat_165
                , null as uniquefloat_166
                , null as uniquefloat_167
                , null as uniquefloat_168
                , null as uniquefloat_169
                , null as uniquefloat_170
                , null as uniquefloat_171
                , null as uniquefloat_172
                , null as uniquefloat_173
                , null as uniquefloat_174
                , null as uniquefloat_175
                , null as uniquefloat_176
                , null as uniquefloat_177
                , null as uniquefloat_178
                , null as uniquefloat_179
                , null as uniquefloat_180
                , null as uniquefloat_181
                , null as uniquefloat_182
                , null as uniquefloat_183
                , null as uniquefloat_184
                , null as uniquefloat_185
                , null as uniquefloat_186
                , null as uniquefloat_187
                , null as uniquefloat_188
                , null as uniquefloat_189
                , null as uniquefloat_190
                , null as uniquefloat_191
                , null as uniquefloat_192
                , null as uniquefloat_193
                , null as uniquefloat_194
                , null as uniquefloat_195
                , null as uniquefloat_196
                , null as uniquefloat_197
                , null as uniquefloat_198
                , null as uniquefloat_199
                , null as uniquefloat_200
                , NULL AS UniqueJson_001 
            FROM ( 
                SELECT 
                    AAA.commondata_001 
                    , AAA.commondata_002 
                    , AAA.commondata_003 
                    , AAA.commondata_004 
                    , AAA.commondata_005 
                    , AAA.commondata_006 
                    , AAA.commondata_007 
                    , AAA.commondata_008 
                    , AAA.commondata_009 
                    , AAA.commondata_010 
                    , AAA.uniquefloat_001 as uniquefloat_001
                    , AAA.uniquefloat_002 as uniquefloat_002
                    , AAA.uniquefloat_003 as uniquefloat_003
                    , AAA.uniquefloat_004 as uniquefloat_004
                    , AAA.uniquefloat_005 as uniquefloat_005
                    , AAA.uniquefloat_006 as uniquefloat_006
                    , AAA.uniquefloat_007 as uniquefloat_007
                    , AAA.uniquefloat_008 as uniquefloat_008
                    , AAA.uniquefloat_009 as uniquefloat_009
                    , AAA.uniquefloat_010 as uniquefloat_010
                    , AAA.uniquefloat_011 as uniquefloat_011
                    , AAA.uniquefloat_012 as uniquefloat_012
                    , AAA.uniquefloat_013 as uniquefloat_013
                    , AAA.uniquefloat_014 as uniquefloat_014
                    , AAA.uniquefloat_015 as uniquefloat_015
                    , AAA.uniquefloat_016 as uniquefloat_016
                    , AAA.uniquefloat_017 as uniquefloat_017
                    , AAA.uniquefloat_018 as uniquefloat_018
                    , AAA.uniquefloat_019 as uniquefloat_019
                    , AAA.uniquefloat_020 as uniquefloat_020
                    , AAA.uniquefloat_021 as uniquefloat_021
                    , AAA.uniquefloat_022 as uniquefloat_022
                    , AAA.uniquefloat_023 as uniquefloat_023
                    , AAA.uniquefloat_024 as uniquefloat_024
                    , AAA.uniquefloat_025 as uniquefloat_025
                    , AAA.uniquefloat_026 as uniquefloat_026
                    , AAA.uniquefloat_027 as uniquefloat_027
                    , AAA.uniquefloat_028 as uniquefloat_028
                    , AAA.uniquefloat_029 as uniquefloat_029
                    , AAA.uniquefloat_030 as uniquefloat_030
                    , AAA.uniquefloat_031 as uniquefloat_031
                    , AAA.uniquefloat_032 as uniquefloat_032
                    , AAA.uniquefloat_033 as uniquefloat_033
                    , AAA.uniquefloat_034 as uniquefloat_034
                    , AAA.uniquefloat_035 as uniquefloat_035
                    , AAA.uniquefloat_036 as uniquefloat_036
                    , AAA.uniquefloat_037 as uniquefloat_037
                    , AAA.uniquefloat_038 as uniquefloat_038
                    , AAA.uniquefloat_039 as uniquefloat_039
                    , AAA.uniquefloat_040 as uniquefloat_040
                    , AAA.uniquefloat_041 as uniquefloat_041
                    , AAA.uniquefloat_042 as uniquefloat_042
                    , AAA.uniquefloat_043 as uniquefloat_043
                    , AAA.uniquefloat_044 as uniquefloat_044
                    , AAA.uniquefloat_045 as uniquefloat_045
                    , AAA.uniquefloat_046 as uniquefloat_046
                    , AAA.uniquefloat_047 as uniquefloat_047
                    , AAA.uniquefloat_048 as uniquefloat_048
                    , AAA.uniquefloat_049 as uniquefloat_049
                    , AAA.uniquefloat_050 as uniquefloat_050
                    , CASE WHEN AAA.uniquefloat_001 = 0 Then 0 ELSE exp(AAA.uniquefloat_001) END as uniquefloat_051
                    , CASE WHEN AAA.uniquefloat_002 = 0 Then 0 ELSE exp(AAA.uniquefloat_002) END as uniquefloat_052
                    , CASE WHEN AAA.uniquefloat_003 = 0 Then 0 ELSE exp(AAA.uniquefloat_003) END as uniquefloat_053
                    , CASE WHEN AAA.uniquefloat_004 = 0 Then 0 ELSE exp(AAA.uniquefloat_004) END as uniquefloat_054
                    , CASE WHEN AAA.uniquefloat_005 = 0 Then 0 ELSE exp(AAA.uniquefloat_005) END as uniquefloat_055
                    , CASE WHEN AAA.uniquefloat_006 = 0 Then 0 ELSE exp(AAA.uniquefloat_006) END as uniquefloat_056
                    , CASE WHEN AAA.uniquefloat_007 = 0 Then 0 ELSE exp(AAA.uniquefloat_007) END as uniquefloat_057
                    , CASE WHEN AAA.uniquefloat_008 = 0 Then 0 ELSE exp(AAA.uniquefloat_008) END as uniquefloat_058
                    , CASE WHEN AAA.uniquefloat_009 = 0 Then 0 ELSE exp(AAA.uniquefloat_009) END as uniquefloat_059
                    , CASE WHEN AAA.uniquefloat_010 = 0 Then 0 ELSE exp(AAA.uniquefloat_010) END as uniquefloat_060
                    , CASE WHEN AAA.uniquefloat_011 = 0 Then 0 ELSE exp(AAA.uniquefloat_011) END as uniquefloat_061
                    , CASE WHEN AAA.uniquefloat_012 = 0 Then 0 ELSE exp(AAA.uniquefloat_012) END as uniquefloat_062
                    , CASE WHEN AAA.uniquefloat_013 = 0 Then 0 ELSE exp(AAA.uniquefloat_013) END as uniquefloat_063
                    , CASE WHEN AAA.uniquefloat_014 = 0 Then 0 ELSE exp(AAA.uniquefloat_014) END as uniquefloat_064
                    , CASE WHEN AAA.uniquefloat_015 = 0 Then 0 ELSE exp(AAA.uniquefloat_015) END as uniquefloat_065
                    , CASE WHEN AAA.uniquefloat_016 = 0 Then 0 ELSE exp(AAA.uniquefloat_016) END as uniquefloat_066
                    , CASE WHEN AAA.uniquefloat_017 = 0 Then 0 ELSE exp(AAA.uniquefloat_017) END as uniquefloat_067
                    , CASE WHEN AAA.uniquefloat_018 = 0 Then 0 ELSE exp(AAA.uniquefloat_018) END as uniquefloat_068
                    , CASE WHEN AAA.uniquefloat_019 = 0 Then 0 ELSE exp(AAA.uniquefloat_019) END as uniquefloat_069
                    , CASE WHEN AAA.uniquefloat_020 = 0 Then 0 ELSE exp(AAA.uniquefloat_020) END as uniquefloat_070
                    , CASE WHEN AAA.uniquefloat_021 = 0 Then 0 ELSE exp(AAA.uniquefloat_021) END as uniquefloat_071
                    , CASE WHEN AAA.uniquefloat_022 = 0 Then 0 ELSE exp(AAA.uniquefloat_022) END as uniquefloat_072
                    , CASE WHEN AAA.uniquefloat_023 = 0 Then 0 ELSE exp(AAA.uniquefloat_023) END as uniquefloat_073
                    , CASE WHEN AAA.uniquefloat_024 = 0 Then 0 ELSE exp(AAA.uniquefloat_024) END as uniquefloat_074
                    , CASE WHEN AAA.uniquefloat_025 = 0 Then 0 ELSE exp(AAA.uniquefloat_025) END as uniquefloat_075
                    , CASE WHEN AAA.uniquefloat_026 = 0 Then 0 ELSE exp(AAA.uniquefloat_026) END as uniquefloat_076
                    , CASE WHEN AAA.uniquefloat_027 = 0 Then 0 ELSE exp(AAA.uniquefloat_027) END as uniquefloat_077
                    , CASE WHEN AAA.uniquefloat_028 = 0 Then 0 ELSE exp(AAA.uniquefloat_028) END as uniquefloat_078
                    , CASE WHEN AAA.uniquefloat_029 = 0 Then 0 ELSE exp(AAA.uniquefloat_029) END as uniquefloat_079
                    , CASE WHEN AAA.uniquefloat_030 = 0 Then 0 ELSE exp(AAA.uniquefloat_030) END as uniquefloat_080
                    , CASE WHEN AAA.uniquefloat_031 = 0 Then 0 ELSE exp(AAA.uniquefloat_031) END as uniquefloat_081
                    , CASE WHEN AAA.uniquefloat_032 = 0 Then 0 ELSE exp(AAA.uniquefloat_032) END as uniquefloat_082
                    , CASE WHEN AAA.uniquefloat_033 = 0 Then 0 ELSE exp(AAA.uniquefloat_033) END as uniquefloat_083
                    , CASE WHEN AAA.uniquefloat_034 = 0 Then 0 ELSE exp(AAA.uniquefloat_034) END as uniquefloat_084
                    , CASE WHEN AAA.uniquefloat_035 = 0 Then 0 ELSE exp(AAA.uniquefloat_035) END as uniquefloat_085
                    , CASE WHEN AAA.uniquefloat_036 = 0 Then 0 ELSE exp(AAA.uniquefloat_036) END as uniquefloat_086
                    , CASE WHEN AAA.uniquefloat_037 = 0 Then 0 ELSE exp(AAA.uniquefloat_037) END as uniquefloat_087
                    , CASE WHEN AAA.uniquefloat_038 = 0 Then 0 ELSE exp(AAA.uniquefloat_038) END as uniquefloat_088
                    , CASE WHEN AAA.uniquefloat_039 = 0 Then 0 ELSE exp(AAA.uniquefloat_039) END as uniquefloat_089
                    , CASE WHEN AAA.uniquefloat_040 = 0 Then 0 ELSE exp(AAA.uniquefloat_040) END as uniquefloat_090
                    , CASE WHEN AAA.uniquefloat_041 = 0 Then 0 ELSE exp(AAA.uniquefloat_041) END as uniquefloat_091
                    , CASE WHEN AAA.uniquefloat_042 = 0 Then 0 ELSE exp(AAA.uniquefloat_042) END as uniquefloat_092
                    , CASE WHEN AAA.uniquefloat_043 = 0 Then 0 ELSE exp(AAA.uniquefloat_043) END as uniquefloat_093
                    , CASE WHEN AAA.uniquefloat_044 = 0 Then 0 ELSE exp(AAA.uniquefloat_044) END as uniquefloat_094
                    , CASE WHEN AAA.uniquefloat_045 = 0 Then 0 ELSE exp(AAA.uniquefloat_045) END as uniquefloat_095
                    , CASE WHEN AAA.uniquefloat_046 = 0 Then 0 ELSE exp(AAA.uniquefloat_046) END as uniquefloat_096
                    , CASE WHEN AAA.uniquefloat_047 = 0 Then 0 ELSE exp(AAA.uniquefloat_047) END as uniquefloat_097
                    , CASE WHEN AAA.uniquefloat_048 = 0 Then 0 ELSE exp(AAA.uniquefloat_048) END as uniquefloat_098
                    , CASE WHEN AAA.uniquefloat_049 = 0 Then 0 ELSE exp(AAA.uniquefloat_049) END as uniquefloat_099
                    , CASE WHEN AAA.uniquefloat_050 = 0 Then 0 ELSE exp(AAA.uniquefloat_050) END as uniquefloat_100
                FROM ( 
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
                        , SUM(CASE WHEN AA.commondata_013 = '1' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_001 
                        , SUM(CASE WHEN AA.commondata_013 = '2' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_002 
                        , SUM(CASE WHEN AA.commondata_013 = '3' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_003 
                        , SUM(CASE WHEN AA.commondata_013 = '4' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_004 
                        , SUM(CASE WHEN AA.commondata_013 = '5' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_005 
                        , SUM(CASE WHEN AA.commondata_013 = '6' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_006 
                        , SUM(CASE WHEN AA.commondata_013 = '7' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_007 
                        , SUM(CASE WHEN AA.commondata_013 = '8' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_008 
                        , SUM(CASE WHEN AA.commondata_013 = '9' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_009 
                        , SUM(CASE WHEN AA.commondata_013 = '10' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_010 
                        , SUM(CASE WHEN AA.commondata_013 = '11' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_011 
                        , SUM(CASE WHEN AA.commondata_013 = '12' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_012 
                        , SUM(CASE WHEN AA.commondata_013 = '13' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_013 
                        , SUM(CASE WHEN AA.commondata_013 = '14' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_014 
                        , SUM(CASE WHEN AA.commondata_013 = '15' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_015 
                        , SUM(CASE WHEN AA.commondata_013 = '16' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_016 
                        , SUM(CASE WHEN AA.commondata_013 = '17' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_017 
                        , SUM(CASE WHEN AA.commondata_013 = '18' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_018 
                        , SUM(CASE WHEN AA.commondata_013 = '19' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_019 
                        , SUM(CASE WHEN AA.commondata_013 = '20' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_020 
                        , SUM(CASE WHEN AA.commondata_013 = '21' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_021 
                        , SUM(CASE WHEN AA.commondata_013 = '22' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_022 
                        , SUM(CASE WHEN AA.commondata_013 = '23' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_023 
                        , SUM(CASE WHEN AA.commondata_013 = '24' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_024 
                        , SUM(CASE WHEN AA.commondata_013 = '25' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_025 
                        , SUM(CASE WHEN AA.commondata_013 = '26' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_026 
                        , SUM(CASE WHEN AA.commondata_013 = '27' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_027 
                        , SUM(CASE WHEN AA.commondata_013 = '28' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_028 
                        , SUM(CASE WHEN AA.commondata_013 = '29' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_029 
                        , SUM(CASE WHEN AA.commondata_013 = '30' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_030 
                        , SUM(CASE WHEN AA.commondata_013 = '31' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_031 
                        , SUM(CASE WHEN AA.commondata_013 = '32' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_032 
                        , SUM(CASE WHEN AA.commondata_013 = '33' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_033 
                        , SUM(CASE WHEN AA.commondata_013 = '34' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_034 
                        , SUM(CASE WHEN AA.commondata_013 = '35' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_035 
                        , SUM(CASE WHEN AA.commondata_013 = '36' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_036 
                        , SUM(CASE WHEN AA.commondata_013 = '37' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_037 
                        , SUM(CASE WHEN AA.commondata_013 = '38' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_038 
                        , SUM(CASE WHEN AA.commondata_013 = '39' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_039 
                        , SUM(CASE WHEN AA.commondata_013 = '40' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_040 
                        , SUM(CASE WHEN AA.commondata_013 = '41' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_041 
                        , SUM(CASE WHEN AA.commondata_013 = '42' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_042 
                        , SUM(CASE WHEN AA.commondata_013 = '43' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_043 
                        , SUM(CASE WHEN AA.commondata_013 = '44' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_044 
                        , SUM(CASE WHEN AA.commondata_013 = '45' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_045 
                        , SUM(CASE WHEN AA.commondata_013 = '46' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_046 
                        , SUM(CASE WHEN AA.commondata_013 = '47' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_047 
                        , SUM(CASE WHEN AA.commondata_013 = '48' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_048 
                        , SUM(CASE WHEN AA.commondata_013 = '49' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_049 
                        , SUM(CASE WHEN AA.commondata_013 = '50' AND AA.uniquefloat_192 != 0  THEN (AA.uniquefloat_191/(AA.uniquefloat_192*AA.uniquefloat_193)) ELSE 0 END) as uniquefloat_050 
                    FROM gtwpd.model_usedata AA
                    WHERE 1 = 1 
                        AND AA.product = '[:ProductName]'
                        AND AA.project = '[:Project]'
                        AND AA.step = 'PreProcess'
                        AND AA.version = '[:InnerDataPreProcessVersion]'
                        AND AA.dt = '[:DateNoLine]'
                    GROUP BY
                        AA.commondata_001 
                        , AA.commondata_002 
                        , AA.commondata_003 
                        , AA.commondata_004
                        , AA.commondata_005 
                        , AA.commondata_006 
                        , AA.commondata_007
                        , AA.commondata_008 
                        , AA.commondata_009
                        , AA.commondata_010 
                ) AAA 
        	) AAAA ;
            """

        parameter = makeInfo["parameter"]
        preProcessParameter = parameter["PreProcess"]
        sqlReplaceArr = preProcessParameter["sqlReplaceArr"]

        for sqlReplace in sqlReplaceArr:
            orderSQL1 = orderSQL1.replace(sqlReplace[0], sqlReplace[1])

        return "MakePreProcessOrderSQLInsert", [orderSQL1]

    # 標籤機率(tag2tag) - by accountid
    @classmethod
    def MakePreProcess_AutoTag_P0_2_2(self, makeInfo):

        RPM = RawPreModel()
        tagDataMap = RPM.tagMap4000
        makedate = datetime.datetime.strptime(makeInfo['makeTime'], '%Y-%m-%d')

        orderSQL1 = f"""
            WITH tb AS (
                SELECT DISTINCT 
                    commondata_1 AS commondata_001
                    ,commondata_2 AS commondata_002
                    ,commondata_3 AS commondata_003
                FROM  
                    gtwpd.modelextract_modelextract 
                 WHERE 1=1  
                 AND game='maple' 
                   AND dt >= {(makedate + datetime.timedelta(days=-30)).strftime('%Y%m%d')}
                   AND dt < {makedate.strftime('%Y%m%d')}
                    AND tablenumber IN (2001)
            ),
            tb2 AS( 
                SELECT 
                    commondata_3 AS commondata_003
                    ,uniquestr_1 AS uniquestr_001
                    ,uniquestr_2 AS uniquestr_002
                    ,uniquestr_3 AS uniquestr_003
                FROM  
                    gtwpd.modelextract_modelextract 
                 WHERE 1=1  
                 AND game='maple' 
                   AND dt >= {(makedate + datetime.timedelta(days=-30)).strftime('%Y%m%d')}
                   AND dt < {makedate.strftime('%Y%m%d')}
                    AND tablenumber IN (4009)
             ) ,
            tb3 AS (
                SELECT 
                     commondata_001    -- 標籤名稱(英)
                    ,  commondata_002    -- 標籤名稱(中)
                    ,  commondata_003    -- 標籤編號
                    ,  commondata_004    -- 標籤解釋
                    ,  commondata_005    -- 標籤適用Table
                    ,  commondata_006    -- 標籤註解
                    ,  commondata_015    -- 編碼長度
                    ,  uniquefloat_001    -- 標籤編碼1
                FROM gtwpd.model_usedata BB 
                where 1 = 1 
                    AND BB.product = 'maple'
                    AND BB.project = 'AutoTag'
                    AND BB.step = 'RawData'
                    AND BB.version = 'R0_2_2'
             ),
             tb4 AS (
                SELECT 
                     aa.commondata_001 as commondata_001    -- 標籤名稱(英)
                    , aa.commondata_004 as commondata_004    -- 標籤解釋
                    , RANK() OVER(PARTITION by aa.commondata_004 ORDER by aa.UniqueFloat_001) as rn
                FROM (
                    SELECT 
                         a.commondata_001 as commondata_001    -- 標籤名稱(英)
                        , c.commondata_001 as commondata_004    -- 標籤解釋
                        , count(c.uniquefloat_001) AS UniqueFloat_001 
                     FROM  tb a
                     LEFT JOIN tb2 b on a.commondata_003 = b.commondata_003
                     LEFT JOIN tb3 c on b.uniquestr_002 = c.uniquefloat_001
                     WHERE c.commondata_001 IS NOT NULL
                     GROUP BY a.commondata_001, c.commondata_001 
                 ) aa
             ),
            main AS (
                 SELECT 
                    aa.commondata_001
                    , [:tagColumnDistinct]
                 FROM (
                     SELECT 
                        commondata_001
                        , [:tagNewColumn]
                     FROM tb4
                 ) aa
                 GROUP BY commondata_001
            ),
            main_max AS (
                 SELECT 
                    [:tagMaxColumnDistinct]
                 FROM main aa
            )
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:PreProcessVersion]' , dt = '[:DateNoLine]' , step = 'PreProcess')
            SELECT 
                aaa.commondata_001
                , null as commondata_002
                , null as commondata_003
                , null as commondata_004
                , null as commondata_005
                , null as commondata_006
                , NULL AS commondata_007
                , NULL AS commondata_008
                , NULL AS commondata_009
                , NULL AS commondata_010
                , NULL AS commondata_011
                , NULL AS commondata_012
                , NULL AS commondata_013
                , NULL AS commondata_014
                , NULL AS commondata_015
                , [:tagStandard]
                , NULL AS UniqueFloat_200
                , NULL AS UniqueJson_001 
             FROM main aaa
             LEFT JOIN main_max bbb on 1=1;
        """
        ind_ = 0
        for key_ in tagDataMap.keys():
            # print(key_)
            ind_ += 1
            columnOrder = str(ind_).zfill(3)
            orderSQL1 = orderSQL1\
                .replace("[:tagColumnDistinct]", f"MAX(aa.{key_})as {key_} \n\t\t\t\t\t, [:tagColumnDistinct]")\
                .replace("[:tagMaxColumnDistinct]", f"MAX(aa.{key_})as {key_}_max \n\t\t\t\t\t, [:tagMaxColumnDistinct]")\
                .replace("[:tagNewColumn]", f"CASE WHEN commondata_004 = '{key_}' THEN rn END AS {key_}\n\t\t\t\t\t\t, [:tagNewColumn]")\
                .replace("[:tagStandard]", f"aaa.{key_} / bbb.{key_}_max as UniqueFloat_{columnOrder} \n\t\t\t\t, [:tagStandard]")
        for i_ in range(ind_+1, 200):
            columnOrder = str(i_).zfill(3)
            orderSQL1 = orderSQL1 \
                .replace("[:tagStandard]", f"NULL AS UniqueFloat_{columnOrder} \n\t\t\t\t, [:tagStandard]")

        orderSQL1 = orderSQL1\
            .replace("\n\t\t\t\t\t, [:tagColumnDistinct]", f"")\
            .replace("\n\t\t\t\t\t, [:tagMaxColumnDistinct]", f"")\
            .replace("\n\t\t\t\t\t\t, [:tagNewColumn]", f"")\
            .replace("\n\t\t\t\t, [:tagStandard]", f"")
        # print(orderSQL1)
        # return "MakePreProcessFreeFuction",  None, {}
        return "MakePreProcessOrderSQLInsert", [orderSQL1], {}

    # tag2tag - Auto - by accountid
    @classmethod
    def MakePreProcess_AutoTag_P0_11001_1(self, makeInfo):
        return "MakePreProcessFreeFuction",  None, {}

    # tag2tag - Auto - by accountid
    @classmethod
    def MakePreProcess_AutoTag_P0_4001_1(self, makeInfo):
        # return "MakePreProcessFreeFuction",  None, {}

        makeTime = makeInfo['makeTime'].replace('-', '')
        dataTime1 = makeInfo["result"]["rawdata"]['dataTime1']
        dataTime2 = makeInfo["result"]["rawdata"]['dataTime2']
        tagDataMapKeys = makeInfo["result"]["rawdata"]['tagDataMapKeys']
        tagDataMapLen = len(tagDataMapKeys)
        orderSqlList = []

        orderSQL1 = f"""
             INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:PreProcessVersion]_0' , dt = '[:DateNoLine]' , step = 'PreProcess')
                SELECT 
                     aa.commondata_001 as commondata_001    -- 帳號
                    , aa.commondata_002 as commondata_002    -- Tag
                    , null as commondata_003
                    , null as commondata_004
                    , null as commondata_005
                    , null as commondata_006
                    , NULL AS commondata_007
                    , NULL AS commondata_008
                    , NULL AS commondata_009
                    , NULL AS commondata_010
                    , NULL AS commondata_011
                    , NULL AS commondata_012
                    , NULL AS commondata_013
                    , NULL AS commondata_014
                    , NULL AS commondata_015
                    , CUME_DIST() OVER(PARTITION by aa.commondata_002 ORDER by aa.UniqueFloat_001)*0.9999 + 0.0001 as UniqueFloat_001
                    , [:UniqueFloatNull]
                    , NULL AS UniqueJson_001 
                FROM gtwpd.model_usedata aa
                where 1 = 1 
                    AND aa.product = 'maple'
                    AND aa.project = 'AutoTag'
                    AND aa.step = 'RawData'
                    AND aa.version = 'R0_4001_1'
                    AND aa.dt = {makeTime};
            """
        for tagInd_ in range(1, 200):
            columnOrder = str(tagInd_+1).zfill(3)
            orderSQL1 = orderSQL1.replace("[:UniqueFloatNull]", f"NULL AS UniqueFloat_{columnOrder} \n\t\t\t\t\t, [:UniqueFloatNull]")
        orderSQL1 = orderSQL1.replace("\n\t\t\t\t\t, [:UniqueFloatNull]", f"")
        orderSqlList.append(orderSQL1)
        # print(orderSQL1)

        # 200 個Tag一組
        orderSQL2 = f"""
                INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:PreProcessVersion]_[:tagPartitionInt]' , dt = '[:DateNoLine]' , step = 'PreProcess')
                SELECT 
                    bb.commondata_001
                    , null as commondata_002
                    , null as commondata_003
                    , null as commondata_004
                    , null as commondata_005
                    , null as commondata_006
                    , NULL AS commondata_007
                    , NULL AS commondata_008
                    , NULL AS commondata_009
                    , NULL AS commondata_010
                    , '{dataTime1}' AS commondata_011
                    , '{dataTime2}' AS commondata_012
                    , [:tagPartitionInt] AS commondata_013
                    , NULL AS commondata_014
                    , NULL AS commondata_015
                    , [:tagColumnDistinct]
                    , [:UniqueFloatNull]
                    , NULL AS UniqueJson_001 
                FROM (
                    SELECT 
                        aa.commondata_001
                        , [:tagNewColumn]
                    FROM gtwpd.model_usedata aa
                    where 1 = 1 
                        AND aa.product = 'maple'
                        AND aa.project = 'AutoTag'
                        AND aa.step = 'PreProcess'
                        AND aa.version = 'P0_4001_1_0'
                        AND aa.dt = {makeTime}
                ) bb
                GROUP BY commondata_001;
            """
        for tag_partition_ in range(math.ceil(tagDataMapLen/200)):
            orderSubSQL2 = orderSQL2.replace('''[:tagPartitionInt]''', str(tag_partition_ + 1))

            # 製作 subTagTable
            for tag_ind_, tag_order_ in enumerate(range(tag_partition_ * 200, (tag_partition_+1) * 200)):
                raw_str1 = "[:tagNewColumn]"
                raw_str2 = "[:tagColumnDistinct]"
                raw_str3 = "[:UniqueFloatNull]"
                if tag_order_ < tagDataMapLen:
                    replace_str1 = f"CASE WHEN aa.commondata_002 = {tag_order_} THEN aa.UniqueFloat_001 END AS UniqueFloat_{str(tag_ind_ + 1).zfill(3)}\n\t\t\t\t\t\t, [:tagNewColumn]"
                    replace_str2 = f"MAX(bb.UniqueFloat_{str(tag_ind_ + 1).zfill(3)})as UniqueFloat_{str(tag_ind_ + 1).zfill(3)} \n\t\t\t\t\t, [:tagColumnDistinct]"
                    orderSubSQL2 = orderSubSQL2.replace(raw_str1, replace_str1)
                    orderSubSQL2 = orderSubSQL2.replace(raw_str2, replace_str2)
                else:
                    replace_str3 = f"NULL AS UniqueFloat_{str(tag_ind_+1).zfill(3)} \n\t\t\t\t\t, [:UniqueFloatNull]"
                    orderSubSQL2 = orderSubSQL2.replace(raw_str3, replace_str3)
            orderSubSQL2 = orderSubSQL2\
                .replace("\n\t\t\t\t\t\t, [:tagNewColumn]", f"")\
                .replace("\n\t\t\t\t\t, [:tagColumnDistinct]", f"")\
                .replace("\n\t\t\t\t\t, [:UniqueFloatNull]", f"")\
                .replace("\n                    , [:UniqueFloatNull]", f"")
            orderSqlList.append(orderSubSQL2)
            # print(orderSubSQL2)

        # return "MakePreProcessFreeFuction",  None, {'tag_partition_num': math.ceil(tagDataMapLen/200)}
        return "MakePreProcessOrderSQLInsert", orderSqlList, {'tag_partition_num': math.ceil(tagDataMapLen/200)}

    @classmethod
    def MakePreProcess_AutoTag_P0_9001_1(self, makeInfo):
        # return "MakePreProcessFreeFuction",  None, {}

        tagSQL1 = f'''
            SELECT DISTINCT 
                commondata_001 as tag_name
            FROM gtwpd.model_usedata  
            WHERE 1=1
                AND product = 'maple'
                AND project='AutoTag'
                AND version='R0_9001_1'
        '''
        hiveCtrl = RawPreModel.getHiveCtrl()
        tagDataList = hiveCtrl.searchSQL(tagSQL1).tag_name.unique()
        # print(tagDataList)

        makedate = datetime.datetime.strptime(makeInfo['makeTime'], '%Y-%m-%d')
        orderSQL1 = f"""
            WITH tb AS (
                SELECT
                    CommonData_1 as commondata_001, 
                    CommonData_10 as commondata_010, 
                    UniqueInt_4, 
                    date_format(UniqueTime_1, 'yyyyMMdd') as buy_date
                FROM gtwpd.modelextract_modelextract AA
                WHERE 1 = 1
                    AND AA.game = 'maple'
                    AND AA.tablenumber = '16009'
                    AND AA.dt >= '20201230'
                    AND AA.dt < {makedate.strftime('%Y%m%d')}
                    AND AA.UniqueStr_11 != 'RollBack'
                    AND AA.commondata_10 in ('5222123', '5680946')
            ),
            tb2 AS( 
                SELECT 
                commondata_001   -- Tag
                , commondata_011 -- time
                , commondata_012 -- itemID
                FROM gtwpd.model_usedata  
                WHERE 1=1
                    AND product = 'maple'
                    AND project='AutoTag'
                    AND version='R0_9001_1'
             ),
             tb4 AS (
                SELECT 
                     aa.commondata_001 as commondata_001    -- 帳號
                    , aa.commondata_002 as commondata_002    -- Tag
                    , RANK() OVER(PARTITION by aa.commondata_002 ORDER by aa.UniqueFloat_001) as rn
                FROM (
                    SELECT 
                         a.commondata_001 as commondata_001    -- 帳號
                        , b.commondata_001 as commondata_002    -- Tag
                        , sum(a.UniqueInt_4) AS UniqueFloat_001
                     FROM  tb a
                     LEFT JOIN tb2 b on a.commondata_010 = b.commondata_012 and a.buy_date = b.commondata_011
                     WHERE b.commondata_001 IS NOT NULL
                     GROUP BY a.commondata_001, b.commondata_001 
                 ) aa
             ),
            main AS (
                 SELECT 
                    aa.commondata_001
                    , [:tagColumnDistinct]
                 FROM (
                     SELECT 
                        commondata_001
                        , [:tagNewColumn]
                     FROM tb4
                 ) aa
                 GROUP BY commondata_001
            ),
            main_max AS (
                 SELECT 
                    [:tagMaxColumnDistinct]
                 FROM main aa
            )
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:PreProcessVersion]' , dt = '[:DateNoLine]' , step = 'PreProcess')
            SELECT 
                aaa.commondata_001
                , null as commondata_002
                , null as commondata_003
                , null as commondata_004
                , null as commondata_005
                , null as commondata_006
                , NULL AS commondata_007
                , NULL AS commondata_008
                , NULL AS commondata_009
                , NULL AS commondata_010
                , NULL AS commondata_011
                , NULL AS commondata_012
                , NULL AS commondata_013
                , NULL AS commondata_014
                , NULL AS commondata_015
                , [:tagStandard]
                , NULL AS UniqueJson_001 
             FROM main aaa
             LEFT JOIN main_max bbb on 1=1;
        """
        ind_ = 0
        for tagInd_, key_ in enumerate(tagDataList):
            # print(key_)
            columnOrder = str(tagInd_+1).zfill(3)
            orderSQL1 = orderSQL1\
                .replace("[:tagNewColumn]", f"CASE WHEN commondata_002 = '{key_}' THEN rn END AS fashionBox_{key_}\n\t\t\t\t\t\t, [:tagNewColumn]")\
                .replace("[:tagColumnDistinct]", f"MAX(aa.fashionBox_{key_})as fashionBox_{key_} \n\t\t\t\t\t, [:tagColumnDistinct]")\
                .replace("[:tagMaxColumnDistinct]", f"MAX(aa.fashionBox_{key_})as fashionBox_{key_}_max \n\t\t\t\t\t, [:tagMaxColumnDistinct]")\
                .replace("[:tagStandard]", f"aaa.fashionBox_{key_} / bbb.fashionBox_{key_}_max as UniqueFloat_{columnOrder} \n\t\t\t\t, [:tagStandard]")
            ind_ = tagInd_

        for tagInd_ in range(ind_ + 1, 200):
            columnOrder = str(tagInd_+1).zfill(3)
            orderSQL1 = orderSQL1 \
                .replace("[:tagStandard]", f"NULL AS UniqueFloat_{columnOrder} \n\t\t\t\t, [:tagStandard]")

        orderSQL1 = orderSQL1\
            .replace("\n\t\t\t\t\t, [:tagColumnDistinct]", f"")\
            .replace("\n\t\t\t\t\t, [:tagMaxColumnDistinct]", f"")\
            .replace("\n\t\t\t\t\t\t, [:tagNewColumn]", f"")\
            .replace("\n\t\t\t\t, [:tagStandard]", f"")
        # print(orderSQL1)
        return "MakePreProcessOrderSQLInsert", [orderSQL1], {}
