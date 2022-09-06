import os
from package.common.database.hiveCtrl import HiveCtrl
from dotenv import load_dotenv
import pandas as pd
from package.common.common.RawPreModel import RawPreModel
import datetime
import numpy as np

class RawData_AutoTag() :
    '''
    文本要by partition 儲存:
        return "MakeRawDataOrderSQLInsert", [orderSQL1] , None  重複問題？
        return "MakeRawDataFileInsertOverwrite", df, {} -> 以測試，可by partition(dt)儲存，V
    R: 要規範只能作文本嗎？還是說可以拿來拉資料？

    R[輸出版號]_[分類版號]_[測試版號]
    [分類版號]：
        4000：任務相關
        9999：外部/手動資料相關
    '''
    @classmethod
    def MakeRawData_AutoTag_R0_1_1(self, makeInfo):
        orderSQL1 = """
                WITH BASIC_DATA as (
                    SELECT 
                        AAA.commondata_1 as serveraccount
                        , AAA.commondata_5 as pointaccount
                        , AAA.commondata_6 as bfappopenID
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_001
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_002
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_003
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_004
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_005
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_006
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_007
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_008
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_009
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_010
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_011
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_012
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_013
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_014
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_015
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_016
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_017
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_018
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_019
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_020
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_021
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_022
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_023
                        , SUM(CASE WHEN AAA.weekday = 0 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_024
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_025
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_026
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_027
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_028
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_029
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_030
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_031
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_032
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_033
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_034
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_035
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_036
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_037
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_038
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_039
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_040
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_041
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_042
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_043
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_044
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_045
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_046
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_047
                        , SUM(CASE WHEN AAA.weekday = 1 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_048
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_049
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_050
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_051
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_052
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_053
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_054
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_055
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_056
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_057
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_058
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_059
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_060
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_061
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_062
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_063
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_064
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_065
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_066
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_067
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_068
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_069
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_070
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_071
                        , SUM(CASE WHEN AAA.weekday = 2 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_072
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_073
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_074
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_075
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_076
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_077
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_078
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_079
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_080
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_081
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_082
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_083
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_084
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_085
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_086
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_087
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_088
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_089
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_090
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_091
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_092
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_093
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_094
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_095
                        , SUM(CASE WHEN AAA.weekday = 3 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_096
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_097
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_098
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_099
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_100
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_101
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_102
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_103
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_104
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_105
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_106
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_107
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_108
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_109
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_110
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_111
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_112
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_113
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_114
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_115
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_116
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_117
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_118
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_119
                        , SUM(CASE WHEN AAA.weekday = 4 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_120
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_121
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_122
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_123
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_124
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_125
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_126
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_127
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_128
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_129
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_130
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_131
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_132
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_133
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_134
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_135
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_136
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_137
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_138
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_139
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_140
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_141
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_142
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_143
                        , SUM(CASE WHEN AAA.weekday = 5 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_144
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_145
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_146
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_147
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_148
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_149
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_150
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_151
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_152
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_153
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_154
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_155
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1132' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_156
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_1 ELSE 0 END ) AS uniquefloat_157
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_2 ELSE 0 END ) AS uniquefloat_158
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_3 ELSE 0 END ) AS uniquefloat_159
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_4 ELSE 0 END ) AS uniquefloat_160
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_5 ELSE 0 END ) AS uniquefloat_161
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_6 ELSE 0 END ) AS uniquefloat_162
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_7 ELSE 0 END ) AS uniquefloat_163
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_8 ELSE 0 END ) AS uniquefloat_164
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_9 ELSE 0 END ) AS uniquefloat_165
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_10 ELSE 0 END ) AS uniquefloat_166
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_11 ELSE 0 END ) AS uniquefloat_167
                        , SUM(CASE WHEN AAA.weekday = 6 AND AAA.tablenumber = '1133' THEN AAA.uniqueint_12 ELSE 0 END ) AS uniquefloat_168
                    FROM (
                        SELECT 
                            AA.game AS game
                            , AA.dt AS  dt
                            , AA.tablenumber AS tablenumber
                            , AA.commondata_1 AS commondata_1 
                            , AA.commondata_5 AS commondata_5 
                            , AA.commondata_6 AS commondata_6
                            , (datediff( concat_ws('-',substr(AA.dt,1,4),substr(AA.dt,5,2),substr(AA.dt,7,2)) ,'2000-01-01')-1) %7 AS weekday
                            , SUM(AA.UniqueInt_1) AS UniqueInt_1 
                            , SUM(AA.UniqueInt_2) AS UniqueInt_2
                            , SUM(AA.UniqueInt_3) AS UniqueInt_3
                            , SUM(AA.UniqueInt_4) AS UniqueInt_4
                            , SUM(AA.UniqueInt_5) AS UniqueInt_5
                            , SUM(AA.UniqueInt_6) AS UniqueInt_6
                            , SUM(AA.UniqueInt_7) AS UniqueInt_7
                            , SUM(AA.UniqueInt_8) AS UniqueInt_8
                            , SUM(AA.UniqueInt_9) AS UniqueInt_9
                            , SUM(AA.UniqueInt_10) AS UniqueInt_10
                            , SUM(AA.UniqueInt_11) AS UniqueInt_11
                            , SUM(AA.UniqueInt_12) AS UniqueInt_12
                        FROM gtwpd.modelextract_modelextract AA
                        WHERE 1 = 1 
                            AND AA.game = 'maple'
                            AND AA.tablenumber IN ('1132','1133')
                            AND AA.dt >= DATE_FORMAT(DATE_ADD('2022-01-01',-6),'yyyyMMdd')
                            AND AA.dt <= '20220101'
                        GROUP BY 
                            AA.game 
                            , AA.dt
                            , AA.tablenumber 
                            , AA.commondata_1 
                            , AA.commondata_5 
                            , AA.commondata_6
                            , (datediff( concat_ws('-',substr(AA.dt,1,4),substr(AA.dt,5,2),substr(AA.dt,7,2)) ,'2000-01-01')-1) %7
                    ) AAA
                    GROUP BY 
                        AAA.commondata_1
                        , AAA.commondata_5 
                        , AAA.commondata_6
                )
                INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:RawDataVersion]' , dt = '[:DateNoLine]' , step = 'RawData')
                SELECT 
                    AAAA.serveraccount as commondata_1
                    , null as commondata_2
                    , null as commondata_3
                    , null as commondata_4
                    , AAAA.pointaccount as commondata_5
                    , AAAA.bfappopenID as commondata_6
                    , NULL AS commondata_7
                    , NULL AS commondata_8
                    , NULL AS commondata_9
                    , NULL AS commondata_10
                    , NULL AS commondata_11
                    , NULL AS commondata_12
                    , NULL AS commondata_13
                    , NULL AS commondata_14
                    , NULL AS commondata_15
                    , AAAA.uniquefloat_001 AS uniquefloat_001
                    , AAAA.uniquefloat_002 AS uniquefloat_002
                    , AAAA.uniquefloat_003 AS uniquefloat_003
                    , AAAA.uniquefloat_004 AS uniquefloat_004
                    , AAAA.uniquefloat_005 AS uniquefloat_005
                    , AAAA.uniquefloat_006 AS uniquefloat_006
                    , AAAA.uniquefloat_007 AS uniquefloat_007
                    , AAAA.uniquefloat_008 AS uniquefloat_008
                    , AAAA.uniquefloat_009 AS uniquefloat_009
                    , AAAA.uniquefloat_010 AS uniquefloat_010
                    , AAAA.uniquefloat_011 AS uniquefloat_011
                    , AAAA.uniquefloat_012 AS uniquefloat_012
                    , AAAA.uniquefloat_013 AS uniquefloat_013
                    , AAAA.uniquefloat_014 AS uniquefloat_014
                    , AAAA.uniquefloat_015 AS uniquefloat_015
                    , AAAA.uniquefloat_016 AS uniquefloat_016
                    , AAAA.uniquefloat_017 AS uniquefloat_017
                    , AAAA.uniquefloat_018 AS uniquefloat_018
                    , AAAA.uniquefloat_019 AS uniquefloat_019
                    , AAAA.uniquefloat_020 AS uniquefloat_020
                    , AAAA.uniquefloat_021 AS uniquefloat_021
                    , AAAA.uniquefloat_022 AS uniquefloat_022
                    , AAAA.uniquefloat_023 AS uniquefloat_023
                    , AAAA.uniquefloat_024 AS uniquefloat_024
                    , AAAA.uniquefloat_025 AS uniquefloat_025
                    , AAAA.uniquefloat_026 AS uniquefloat_026
                    , AAAA.uniquefloat_027 AS uniquefloat_027
                    , AAAA.uniquefloat_028 AS uniquefloat_028
                    , AAAA.uniquefloat_029 AS uniquefloat_029
                    , AAAA.uniquefloat_030 AS uniquefloat_030
                    , AAAA.uniquefloat_031 AS uniquefloat_031
                    , AAAA.uniquefloat_032 AS uniquefloat_032
                    , AAAA.uniquefloat_033 AS uniquefloat_033
                    , AAAA.uniquefloat_034 AS uniquefloat_034
                    , AAAA.uniquefloat_035 AS uniquefloat_035
                    , AAAA.uniquefloat_036 AS uniquefloat_036
                    , AAAA.uniquefloat_037 AS uniquefloat_037
                    , AAAA.uniquefloat_038 AS uniquefloat_038
                    , AAAA.uniquefloat_039 AS uniquefloat_039
                    , AAAA.uniquefloat_040 AS uniquefloat_040
                    , AAAA.uniquefloat_041 AS uniquefloat_041
                    , AAAA.uniquefloat_042 AS uniquefloat_042
                    , AAAA.uniquefloat_043 AS uniquefloat_043
                    , AAAA.uniquefloat_044 AS uniquefloat_044
                    , AAAA.uniquefloat_045 AS uniquefloat_045
                    , AAAA.uniquefloat_046 AS uniquefloat_046
                    , AAAA.uniquefloat_047 AS uniquefloat_047
                    , AAAA.uniquefloat_048 AS uniquefloat_048
                    , AAAA.uniquefloat_049 AS uniquefloat_049
                    , AAAA.uniquefloat_050 AS uniquefloat_050
                    , AAAA.uniquefloat_051 AS uniquefloat_051
                    , AAAA.uniquefloat_052 AS uniquefloat_052
                    , AAAA.uniquefloat_053 AS uniquefloat_053
                    , AAAA.uniquefloat_054 AS uniquefloat_054
                    , AAAA.uniquefloat_055 AS uniquefloat_055
                    , AAAA.uniquefloat_056 AS uniquefloat_056
                    , AAAA.uniquefloat_057 AS uniquefloat_057
                    , AAAA.uniquefloat_058 AS uniquefloat_058
                    , AAAA.uniquefloat_059 AS uniquefloat_059
                    , AAAA.uniquefloat_060 AS uniquefloat_060
                    , AAAA.uniquefloat_061 AS uniquefloat_061
                    , AAAA.uniquefloat_062 AS uniquefloat_062
                    , AAAA.uniquefloat_063 AS uniquefloat_063
                    , AAAA.uniquefloat_064 AS uniquefloat_064
                    , AAAA.uniquefloat_065 AS uniquefloat_065
                    , AAAA.uniquefloat_066 AS uniquefloat_066
                    , AAAA.uniquefloat_067 AS uniquefloat_067
                    , AAAA.uniquefloat_068 AS uniquefloat_068
                    , AAAA.uniquefloat_069 AS uniquefloat_069
                    , AAAA.uniquefloat_070 AS uniquefloat_070
                    , AAAA.uniquefloat_071 AS uniquefloat_071
                    , AAAA.uniquefloat_072 AS uniquefloat_072
                    , AAAA.uniquefloat_073 AS uniquefloat_073
                    , AAAA.uniquefloat_074 AS uniquefloat_074
                    , AAAA.uniquefloat_075 AS uniquefloat_075
                    , AAAA.uniquefloat_076 AS uniquefloat_076
                    , AAAA.uniquefloat_077 AS uniquefloat_077
                    , AAAA.uniquefloat_078 AS uniquefloat_078
                    , AAAA.uniquefloat_079 AS uniquefloat_079
                    , AAAA.uniquefloat_080 AS uniquefloat_080
                    , AAAA.uniquefloat_081 AS uniquefloat_081
                    , AAAA.uniquefloat_082 AS uniquefloat_082
                    , AAAA.uniquefloat_083 AS uniquefloat_083
                    , AAAA.uniquefloat_084 AS uniquefloat_084
                    , AAAA.uniquefloat_085 AS uniquefloat_085
                    , AAAA.uniquefloat_086 AS uniquefloat_086
                    , AAAA.uniquefloat_087 AS uniquefloat_087
                    , AAAA.uniquefloat_088 AS uniquefloat_088
                    , AAAA.uniquefloat_089 AS uniquefloat_089
                    , AAAA.uniquefloat_090 AS uniquefloat_090
                    , AAAA.uniquefloat_091 AS uniquefloat_091
                    , AAAA.uniquefloat_092 AS uniquefloat_092
                    , AAAA.uniquefloat_093 AS uniquefloat_093
                    , AAAA.uniquefloat_094 AS uniquefloat_094
                    , AAAA.uniquefloat_095 AS uniquefloat_095
                    , AAAA.uniquefloat_096 AS uniquefloat_096
                    , AAAA.uniquefloat_097 AS uniquefloat_097
                    , AAAA.uniquefloat_098 AS uniquefloat_098
                    , AAAA.uniquefloat_099 AS uniquefloat_099
                    , AAAA.uniquefloat_100 AS uniquefloat_100
                    , AAAA.uniquefloat_101 AS uniquefloat_101
                    , AAAA.uniquefloat_102 AS uniquefloat_102
                    , AAAA.uniquefloat_103 AS uniquefloat_103
                    , AAAA.uniquefloat_104 AS uniquefloat_104
                    , AAAA.uniquefloat_105 AS uniquefloat_105
                    , AAAA.uniquefloat_106 AS uniquefloat_106
                    , AAAA.uniquefloat_107 AS uniquefloat_107
                    , AAAA.uniquefloat_108 AS uniquefloat_108
                    , AAAA.uniquefloat_109 AS uniquefloat_109
                    , AAAA.uniquefloat_110 AS uniquefloat_110
                    , AAAA.uniquefloat_111 AS uniquefloat_111
                    , AAAA.uniquefloat_112 AS uniquefloat_112
                    , AAAA.uniquefloat_113 AS uniquefloat_113
                    , AAAA.uniquefloat_114 AS uniquefloat_114
                    , AAAA.uniquefloat_115 AS uniquefloat_115
                    , AAAA.uniquefloat_116 AS uniquefloat_116
                    , AAAA.uniquefloat_117 AS uniquefloat_117
                    , AAAA.uniquefloat_118 AS uniquefloat_118
                    , AAAA.uniquefloat_119 AS uniquefloat_119
                    , AAAA.uniquefloat_120 AS uniquefloat_120
                    , AAAA.uniquefloat_121 AS uniquefloat_121
                    , AAAA.uniquefloat_122 AS uniquefloat_122
                    , AAAA.uniquefloat_123 AS uniquefloat_123
                    , AAAA.uniquefloat_124 AS uniquefloat_124
                    , AAAA.uniquefloat_125 AS uniquefloat_125
                    , AAAA.uniquefloat_126 AS uniquefloat_126
                    , AAAA.uniquefloat_127 AS uniquefloat_127
                    , AAAA.uniquefloat_128 AS uniquefloat_128
                    , AAAA.uniquefloat_129 AS uniquefloat_129
                    , AAAA.uniquefloat_130 AS uniquefloat_130
                    , AAAA.uniquefloat_131 AS uniquefloat_131
                    , AAAA.uniquefloat_132 AS uniquefloat_132
                    , AAAA.uniquefloat_133 AS uniquefloat_133
                    , AAAA.uniquefloat_134 AS uniquefloat_134
                    , AAAA.uniquefloat_135 AS uniquefloat_135
                    , AAAA.uniquefloat_136 AS uniquefloat_136
                    , AAAA.uniquefloat_137 AS uniquefloat_137
                    , AAAA.uniquefloat_138 AS uniquefloat_138
                    , AAAA.uniquefloat_139 AS uniquefloat_139
                    , AAAA.uniquefloat_140 AS uniquefloat_140
                    , AAAA.uniquefloat_141 AS uniquefloat_141
                    , AAAA.uniquefloat_142 AS uniquefloat_142
                    , AAAA.uniquefloat_143 AS uniquefloat_143
                    , AAAA.uniquefloat_144 AS uniquefloat_144
                    , AAAA.uniquefloat_145 AS uniquefloat_145
                    , AAAA.uniquefloat_146 AS uniquefloat_146
                    , AAAA.uniquefloat_147 AS uniquefloat_147
                    , AAAA.uniquefloat_148 AS uniquefloat_148
                    , AAAA.uniquefloat_149 AS uniquefloat_149
                    , AAAA.uniquefloat_150 AS uniquefloat_150
                    , AAAA.uniquefloat_151 AS uniquefloat_151
                    , AAAA.uniquefloat_152 AS uniquefloat_152
                    , AAAA.uniquefloat_153 AS uniquefloat_153
                    , AAAA.uniquefloat_154 AS uniquefloat_154
                    , AAAA.uniquefloat_155 AS uniquefloat_155
                    , AAAA.uniquefloat_156 AS uniquefloat_156
                    , AAAA.uniquefloat_157 AS uniquefloat_157
                    , AAAA.uniquefloat_158 AS uniquefloat_158
                    , AAAA.uniquefloat_159 AS uniquefloat_159
                    , AAAA.uniquefloat_160 AS uniquefloat_160
                    , AAAA.uniquefloat_161 AS uniquefloat_161
                    , AAAA.uniquefloat_162 AS uniquefloat_162
                    , AAAA.uniquefloat_163 AS uniquefloat_163
                    , AAAA.uniquefloat_164 AS uniquefloat_164
                    , AAAA.uniquefloat_165 AS uniquefloat_165
                    , AAAA.uniquefloat_166 AS uniquefloat_166
                    , AAAA.uniquefloat_167 AS uniquefloat_167
                    , AAAA.uniquefloat_168 AS uniquefloat_168
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
                FROM BASIC_DATA AAAA;
            """
        return "MakeRawDataOrderSQLInsert", [orderSQL1] , None

    # Tag2Tag
    @classmethod
    def MakeRawData_AutoTag_R0_2_2(self, makeInfo):
        # return "MakeRawDataFreeFuction", None, {}
        RPM = RawPreModel()
        tagDataMap = RPM.tagMap4000

        tagorder = 0
        tagDictionarys = []
        for key in tagDataMap.keys():
            tagData = tagDataMap[key]
            for ind_ in range(len(tagData['tagshape'])):
                tagorder += 1
                tagDictionary = []
                tagDictionary.append(tagData['enname'])
                tagDictionary.append(tagData['cnname'])
                tagDictionary.append(str(tagorder))
                tagDictionary.append(tagData['memo'])
                tagDictionary.append(tagData['TargetTable'])
                tagDictionary.append(tagData['jsonmessage'])
                tagDictionary.append(len(tagData['tagshape']))
                tagDictionary.append(tagData['tagshape'][ind_])
                tagDictionarys.append(tagDictionary)

        df = pd.DataFrame(tagDictionarys)

        df.columns = [
            "commondata_001"  # 標籤名稱(英)
            , "commondata_002"  # 標籤名稱(中)
            , "commondata_003"  # 標籤編號
            , "commondata_004"  # 標籤解釋
            , "commondata_005"  # 標籤適用Table
            , "commondata_006"  # 標籤註解
            , "commondata_015"  # 編碼長度
            , "uniquefloat_001" # 標籤編碼1
        ]

        return "MakeRawDataFileInsertOverwrite", df, {}

    # Tag2Tag - Auto
    @classmethod
    def MakeRawData_AutoTag_R0_11001_1(self, makeInfo):
        # return "MakeRawDataFreeFuction", None , {}

        makeTime = makeInfo['makeTime'].replace('-', '')
        dataTime1 = (datetime.datetime.strptime(makeInfo['makeTime'], "%Y-%m-%d") + datetime.timedelta(days=-365)).strftime("%Y%m%d")
        dataTime2 = (datetime.datetime.strptime(makeInfo['makeTime'], "%Y-%m-%d") + datetime.timedelta(days=-1)).strftime("%Y%m%d")
        orderSQL1 = f'''
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:RawDataVersion]' , dt = '[:DateNoLine]' , step = 'RawData')
            SELECT DISTINCT 
                commondata_1 AS commondata_001 -- 帳號
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
                , 1 AS commondata_013
                , NULL AS commondata_014
                , NULL AS commondata_015
                , [:allColumns]
            FROM  
                gtwpd.modelextract_modelextract 
            WHERE 1=1  
                AND game='maple' 
                AND dt >= {dataTime1}
                AND dt <= {dataTime2}
                AND tablenumber IN (1002);
        '''
        for tagInd_ in range(200):
            columnOrder = str(tagInd_ + 1).zfill(3)
            orderSQL1 = orderSQL1.replace("[:allColumns]", f"NULL AS uniqueFloat_{columnOrder} \n\t\t\t\t, [:allColumns]")
        for tagInd_ in range(1):
            columnOrder = str(tagInd_ + 1).zfill(3)
            orderSQL1 = orderSQL1.replace("[:allColumns]", f"NULL AS uniqueJson_{columnOrder} \n\t\t\t\t, [:allColumns]")
        orderSQL1 = orderSQL1.replace('\n\t\t\t\t, [:allColumns]', f"")
        # return "MakeRawDataFreeFuction", None , {'dataTime1': dataTime1, 'dataTime2': dataTime2, 'partition_num': 1}
        return "MakeRawDataOrderSQLInsert", [orderSQL1], {'dataTime1': dataTime1, 'dataTime2': dataTime2, 'partition_num': 1}

    # Tag2Tag - Auto
    @classmethod
    def MakeRawData_AutoTag_R0_4001_0(self, makeInfo):
        # return "MakeRawDataFreeFuction", None, {}
        RPM = RawPreModel()
        tagDataMap = RPM.AutoTagMap4000

        tagorder = 0
        tagDictionarys = []
        for key in tagDataMap.keys():
            tagData = tagDataMap[key]
            tagorder += 1
            for ind_ in range(len(tagData['tagshape'])):
                tagDictionary = []
                tagDictionary.append(tagData['enname'])
                tagDictionary.append(tagData['cnname'])
                tagDictionary.append(tagData['enname']) # 標籤編號暫且用英文名
                tagDictionary.append(tagData['memo'])
                tagDictionary.append(tagData['jsonmessage'])
                tagDictionary.append(tagData['tagshape'][ind_])
                tagDictionarys.append(tagDictionary)

        df = pd.DataFrame(tagDictionarys)

        df.columns = [
            "commondata_011"  # 標籤名稱(英)
            , "commondata_012"  # 標籤名稱(中)
            , "commondata_013"  # 標籤編號
            , "commondata_014"  # 標籤說明
            , "commondata_015"  # 標籤JSON
            , "commondata_008"  # 標籤編碼1
        ]
        print(df)

        return "MakeRawDataFileInsertOverwrite", df, {}

    # Tag2Tag - Auto
    @classmethod
    def MakeRawData_AutoTag_R0_4001_1(self, makeInfo):
        # return "MakeRawDataFreeFuction", None , {}

        RPM = RawPreModel()
        tagDataMap = RPM.AutoTagMap4000
        makeTime = makeInfo['makeTime'].replace('-', '')
        dataTime1 = (datetime.datetime.strptime(makeInfo['makeTime'], "%Y-%m-%d") + datetime.timedelta(days=-30)).strftime("%Y%m%d")
        dataTime2 = (datetime.datetime.strptime(makeInfo['makeTime'], "%Y-%m-%d") + datetime.timedelta(days=-1)).strftime("%Y%m%d")
        orderSQL1 = f'''
            WITH tb AS (
                SELECT DISTINCT 
                    commondata_1 AS commondata_001 -- 帳號
                    ,commondata_3 AS commondata_003 -- 角色
                FROM  
                    gtwpd.modelextract_modelextract 
                 WHERE 1=1  
                 AND game='maple' 
                   AND dt >= {dataTime1}
                   AND dt <= {dataTime2}
                    AND tablenumber IN (2001)
            ),
            tb2 AS( 
                SELECT 
                    commondata_3 AS commondata_003 -- 角色
                    ,uniquestr_2 AS uniquestr_002 -- Tag_code
                FROM  
                    gtwpd.modelextract_modelextract 
                 WHERE 1=1  
                 AND game='maple' 
                   AND dt >= {dataTime1}
                   AND dt <= {dataTime2}
                    AND tablenumber IN (4009)
             ) ,
            tb3 AS (
                SELECT 
                    commondata_011        -- Tag name
                    , commondata_008      -- Tag_code
                FROM gtwpd.model_usedata BB 
                where 1 = 1 
                    AND BB.product = 'maple'
                    AND BB.project = 'AutoTag'
                    AND BB.step = 'RawData'
                    AND BB.version = 'R0_4001_0'
                    AND BB.dt = {makeTime}
             )
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:RawDataVersion]' , dt = '[:DateNoLine]' , step = 'RawData')
            SELECT 
                 a.commondata_001 as commondata_001    -- 帳號
                , c.commondata_011 as commondata_002    -- Tag
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
                , count(c.commondata_011) AS UniqueFloat_001 
                , [:UniqueFloatNull]
                , NULL AS UniqueJson_001 
             FROM  tb a
             LEFT JOIN tb2 b on a.commondata_003 = b.commondata_003
             LEFT JOIN tb3 c on b.uniquestr_002 = c.commondata_008
             WHERE c.commondata_011 IS NOT NULL
             GROUP BY a.commondata_001, c.commondata_011;
        '''
        for tagInd_ in range(1, 200):
            columnOrder = str(tagInd_+1).zfill(3)
            orderSQL1 = orderSQL1.replace("[:UniqueFloatNull]", f"NULL AS UniqueFloat_{columnOrder} \n\t\t\t\t, [:UniqueFloatNull]")
        orderSQL1 = orderSQL1.replace("\n\t\t\t\t, [:UniqueFloatNull]", f"")

        # return "MakeRawDataFreeFuction", None , {'dataTime1': dataTime1, 'dataTime2': dataTime2, 'tagDataMapKeys': list(tagDataMap.keys())}
        return "MakeRawDataOrderSQLInsert", [orderSQL1], {'dataTime1': dataTime1, 'dataTime2': dataTime2, 'tagDataMapKeys': list(tagDataMap.keys())}

    # FashionBox 歷史檔期Tag - 購買PR值(不含沒買的)
    @classmethod
    def MakeRawData_AutoTag_R0_9001_1(self, makeInfo):
        # return "MakeUseModelFreeFuction", None, {}
        def getEventList(hiveCtrl):
            sql_str = '''
                    SELECT DISTINCT 
                    date_format(commondata_006, 'yyyyMMdd') as st_date
                    , date_format(commondata_007, 'yyyyMMdd') as ed_date
                    FROM gtwpd.model_usedata  
                    WHERE 1=1
                        AND product = 'maple'
                        AND project='ExternalDataManage'
                        AND version='R1_0_1'
                        AND commondata_006 != ''
                    '''
            df = hiveCtrl.searchSQL(sql_str)
            dt_np = df[['st_date', 'ed_date']].drop_duplicates().to_numpy()
            return dt_np

        hiveCtrl = RawPreModel.getHiveCtrl()
        dt_np = getEventList(hiveCtrl)
        print(dt_np)

        tagorder = 0
        tagDictionarys = []
        for ind_, _ in enumerate(dt_np):
            st_ = _[0]
            ed_ = _[1]
            day_num = datetime.datetime.strptime(ed_, "%Y%m%d") - datetime.datetime.strptime(st_, "%Y%m%d")
            for day_ in range(day_num.days+1):
                tagDictionary = []
                item_id = '5222123' if datetime.datetime.strptime(st_, "%Y%m%d") < datetime.datetime.strptime('20210908', "%Y%m%d") else '5680946'
                tagDictionary.append(f'{item_id}_{st_}')
                tagDictionary.append(f'{item_id}_{st_}')
                tagDictionary.append(ind_)
                tagDictionary.append('從 20201230 開始至今，紀錄每個人買 "時尚隨機箱檔期" 的百分水位')
                tagDictionary.append('{}')
                tagDictionary.append((datetime.datetime.strptime(st_, "%Y%m%d") + datetime.timedelta(days=day_)).strftime("%Y%m%d"))
                tagDictionary.append(item_id)
                tagDictionarys.append(tagDictionary)

        df = pd.DataFrame(tagDictionarys)
        df.columns = [
            "commondata_001"  # 標籤名稱(英)
            , "commondata_002"  # 標籤名稱(中)
            , "commondata_003"  # 標籤編號
            , "commondata_004"  # 標籤說明
            , "commondata_005"  # 標籤JSON
            , "commondata_011"  # 標籤編碼1
            , "commondata_012"  # 標籤編碼2
        ]

        print(df)

        return "MakeRawDataFileInsertOverwrite", df, {}
