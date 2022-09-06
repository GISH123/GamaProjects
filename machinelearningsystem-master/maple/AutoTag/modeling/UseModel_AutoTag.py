import os; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import copy
import datetime
import pandas , numpy , random , math
from dotenv import load_dotenv
from scipy.spatial import distance
from package.common.database.hiveCtrl import HiveCtrl

class UseModel_AutoTag() :

    @classmethod
    def MakePreProcess_AutoTag_P0_1_5(self, makeInfo):
        orderSQL1 = """
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:PreProcessVersion]' , dt = '[:DateNoLine]' , step = 'PreProcess')
            SELECT 
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
                , null ascommondata_011 
                , CASE 	
                    WHEN AA.uniquefloat_051 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '1'
                    WHEN AA.uniquefloat_052 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '2'
                    WHEN AA.uniquefloat_053 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '3'
                    WHEN AA.uniquefloat_054 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '4'
                    WHEN AA.uniquefloat_055 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '5'
                    WHEN AA.uniquefloat_056 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '6'
                    WHEN AA.uniquefloat_057 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '7'
                    WHEN AA.uniquefloat_058 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '8'
                    WHEN AA.uniquefloat_059 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '9'
                    WHEN AA.uniquefloat_060 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '10'
                    WHEN AA.uniquefloat_061 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '11'
                    WHEN AA.uniquefloat_062 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '12'
                    WHEN AA.uniquefloat_063 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '13'
                    WHEN AA.uniquefloat_064 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '14'
                    WHEN AA.uniquefloat_065 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '15'
                    WHEN AA.uniquefloat_066 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '16'
                    WHEN AA.uniquefloat_067 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '17'
                    WHEN AA.uniquefloat_068 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '18'
                    WHEN AA.uniquefloat_069 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '19'
                    WHEN AA.uniquefloat_070 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '20'
                    WHEN AA.uniquefloat_071 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '21'
                    WHEN AA.uniquefloat_072 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '22'
                    WHEN AA.uniquefloat_073 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '23'
                    WHEN AA.uniquefloat_074 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '24'
                    WHEN AA.uniquefloat_075 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '25'
                    WHEN AA.uniquefloat_076 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '26'
                    WHEN AA.uniquefloat_077 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '27'
                    WHEN AA.uniquefloat_078 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '28'
                    WHEN AA.uniquefloat_079 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '29'
                    WHEN AA.uniquefloat_080 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '30'
                    WHEN AA.uniquefloat_081 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '31'
                    WHEN AA.uniquefloat_082 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '32'
                    WHEN AA.uniquefloat_083 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '33'
                    WHEN AA.uniquefloat_084 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '34'
                    WHEN AA.uniquefloat_085 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '35'
                    WHEN AA.uniquefloat_086 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '36'
                    WHEN AA.uniquefloat_087 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '37'
                    WHEN AA.uniquefloat_088 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '38'
                    WHEN AA.uniquefloat_089 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '39'
                    WHEN AA.uniquefloat_090 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '40'
                    WHEN AA.uniquefloat_091 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '41'
                    WHEN AA.uniquefloat_092 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '42'
                    WHEN AA.uniquefloat_093 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '43'
                    WHEN AA.uniquefloat_094 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '44'
                    WHEN AA.uniquefloat_095 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '45'
                    WHEN AA.uniquefloat_096 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '46'
                    WHEN AA.uniquefloat_097 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '47'
                    WHEN AA.uniquefloat_098 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '48'
                    WHEN AA.uniquefloat_099 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '49'
                    WHEN AA.uniquefloat_100 = greatest( AA.uniquefloat_051 , AA.uniquefloat_052 , AA.uniquefloat_053 , AA.uniquefloat_054 , AA.uniquefloat_055 , AA.uniquefloat_056 , AA.uniquefloat_057 , AA.uniquefloat_058 , AA.uniquefloat_059 , AA.uniquefloat_060 , AA.uniquefloat_061 , AA.uniquefloat_062 , AA.uniquefloat_063 , AA.uniquefloat_064 , AA.uniquefloat_065 , AA.uniquefloat_066 , AA.uniquefloat_067 , AA.uniquefloat_068 , AA.uniquefloat_069 , AA.uniquefloat_070 , AA.uniquefloat_071 , AA.uniquefloat_072 , AA.uniquefloat_073 , AA.uniquefloat_074 , AA.uniquefloat_075 , AA.uniquefloat_076 , AA.uniquefloat_077 , AA.uniquefloat_078 , AA.uniquefloat_079 , AA.uniquefloat_080 , AA.uniquefloat_081 , AA.uniquefloat_082 , AA.uniquefloat_083 , AA.uniquefloat_084 , AA.uniquefloat_085 , AA.uniquefloat_086 , AA.uniquefloat_087 , AA.uniquefloat_088 , AA.uniquefloat_089 , AA.uniquefloat_090 , AA.uniquefloat_091 , AA.uniquefloat_092 , AA.uniquefloat_093 , AA.uniquefloat_094 , AA.uniquefloat_095 , AA.uniquefloat_096 , AA.uniquefloat_097 , AA.uniquefloat_098 , AA.uniquefloat_099 , AA.uniquefloat_100) THEN '50'
                END as commondata_012
                , null as commondata_013 
                , null as commondata_014 
                , null as commondata_015 
                , null as uniquefloat_001
                , null as uniquefloat_002
                , null as uniquefloat_003
                , null as uniquefloat_004
                , null as uniquefloat_005
                , null as uniquefloat_006
                , null as uniquefloat_007
                , null as uniquefloat_008
                , null as uniquefloat_009
                , null as uniquefloat_010
                , null as uniquefloat_011
                , null as uniquefloat_012
                , null as uniquefloat_013
                , null as uniquefloat_014
                , null as uniquefloat_015
                , null as uniquefloat_016
                , null as uniquefloat_017
                , null as uniquefloat_018
                , null as uniquefloat_019
                , null as uniquefloat_020
                , null as uniquefloat_021
                , null as uniquefloat_022
                , null as uniquefloat_023
                , null as uniquefloat_024
                , null as uniquefloat_025
                , null as uniquefloat_026
                , null as uniquefloat_027
                , null as uniquefloat_028
                , null as uniquefloat_029
                , null as uniquefloat_030
                , null as uniquefloat_031
                , null as uniquefloat_032
                , null as uniquefloat_033
                , null as uniquefloat_034
                , null as uniquefloat_035
                , null as uniquefloat_036
                , null as uniquefloat_037
                , null as uniquefloat_038
                , null as uniquefloat_039
                , null as uniquefloat_040
                , null as uniquefloat_041
                , null as uniquefloat_042
                , null as uniquefloat_043
                , null as uniquefloat_044
                , null as uniquefloat_045
                , null as uniquefloat_046
                , null as uniquefloat_047
                , null as uniquefloat_048
                , null as uniquefloat_049
                , null as uniquefloat_050
                , null as uniquefloat_051
                , null as uniquefloat_052
                , null as uniquefloat_053
                , null as uniquefloat_054
                , null as uniquefloat_055
                , null as uniquefloat_056
                , null as uniquefloat_057
                , null as uniquefloat_058
                , null as uniquefloat_059
                , null as uniquefloat_060
                , null as uniquefloat_061
                , null as uniquefloat_062
                , null as uniquefloat_063
                , null as uniquefloat_064
                , null as uniquefloat_065
                , null as uniquefloat_066
                , null as uniquefloat_067
                , null as uniquefloat_068
                , null as uniquefloat_069
                , null as uniquefloat_070
                , null as uniquefloat_071
                , null as uniquefloat_072
                , null as uniquefloat_073
                , null as uniquefloat_074
                , null as uniquefloat_075
                , null as uniquefloat_076
                , null as uniquefloat_077
                , null as uniquefloat_078
                , null as uniquefloat_079
                , null as uniquefloat_080
                , null as uniquefloat_081
                , null as uniquefloat_082
                , null as uniquefloat_083
                , null as uniquefloat_084
                , null as uniquefloat_085
                , null as uniquefloat_086
                , null as uniquefloat_087
                , null as uniquefloat_088
                , null as uniquefloat_089
                , null as uniquefloat_090
                , null as uniquefloat_091
                , null as uniquefloat_092
                , null as uniquefloat_093
                , null as uniquefloat_094
                , null as uniquefloat_095
                , null as uniquefloat_096
                , null as uniquefloat_097
                , null as uniquefloat_098
                , null as uniquefloat_099
                , null as uniquefloat_100
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
            FROM gtwpd.model_usedata AA
            WHERE 1 = 1 
                AND AA.product = '[:ProductName]'
                AND AA.project = '[:Project]'
                AND AA.step = 'PreProcess'
                AND AA.version = '[:TagOddsPreProcessVersion]'
                AND AA.dt = '[:DateNoLine]'
            ;
            """

        parameter = makeInfo["parameter"]
        preProcessParameter = parameter["PreProcess"]
        sqlReplaceArr = preProcessParameter["sqlReplaceArr"]

        for sqlReplace in sqlReplaceArr:
            orderSQL1 = orderSQL1.replace(sqlReplace[0], sqlReplace[1])

        return "MakePreProcessOrderSQLInsert", [orderSQL1]


    @classmethod
    def MakeUseModel_AutoTag_M0_1_5(self, makeInfo):
        orderSQL1 = """
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:UseModelVersion]' , dt = '[:DateNoLine]' , step = 'UseModel')
            SELECT 
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
                , BB.commondata_001 
                , BB.commondata_002  
                , null as commondata_013 
                , null as commondata_014 
                , null as commondata_015 
                , NULL AS UniqueFloat_001
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
            INNER JOIN gtwpd.model_usedata BB ON 1 = 1 
                AND BB.commondata_002 = AA.commondata_012  
                AND BB.product = '[:ProductName]' 
                AND BB.project = '[:Project]' 
                AND BB.version = '[:TagTextPreProcessVersion]'
                AND BB.dt = '[:DateNoLine]'
                AND BB.step = 'PreProcess'
            WHERE 1 = 1 
                AND AA.product = '[:ProductName]' 
                AND AA.project = '[:Project]' 
                AND AA.version = '[:MarkTagPreProcessVersion]'
                AND AA.dt = '[:DateNoLine]'
                AND AA.step = 'PreProcess';
        """
        parameter = makeInfo["parameter"]
        preProcessParameter = parameter["PreProcess"]
        sqlReplaceArr = preProcessParameter["sqlReplaceArr"]

        for sqlReplace in sqlReplaceArr:
            orderSQL1 = orderSQL1.replace(sqlReplace[0], sqlReplace[1])

        return "MakeUseModelOrderSQLInsert", [orderSQL1]

    @classmethod
    def MakeUseModel_AutoTag_M0_11001_99(self, makeInfo):
        # return "MakeUseModelFreeFuction", None, {}

        makeTime = makeInfo['makeTime'].replace('-', '')
        tag_partition_num = makeInfo["result"]["rawdata"]['partition_num']

        # Model Result
        orderSQL = []
        for tag_partition_ in range(tag_partition_num):
            orderSubSQL = f"""
                INSERT [:isOverWrite] TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:ModelVersion]' , dt = '[:DateNoLine]' , step = 'ModelResult')
                SELECT 
                    [:allColumns]
                FROM gtwpd.model_usedata
                where 1 = 1 
                    AND product = 'maple'
                    AND project = 'AutoTag'
                    AND step = 'RawData'
                    AND version = 'R0_11001_1'
                    AND dt = {makeTime};
                """
            if tag_partition_ == 0: orderSubSQL = orderSubSQL.replace(' [:isOverWrite]', f" OVERWRITE")
            else: orderSubSQL = orderSubSQL.replace(' [:isOverWrite]', f" INTO")

            orderSubSQL = orderSubSQL.replace('''[:tagPartitionInt]''', str(tag_partition_ + 1))
            for tagInd_ in range(15):
                columnOrder = str(tagInd_ + 1).zfill(3)
                orderSubSQL = orderSubSQL.replace("[:allColumns]", f"commondata_{columnOrder} \n\t\t\t\t, [:allColumns]")
            for tagInd_ in range(200):
                columnOrder = str(tagInd_ + 1).zfill(3)
                orderSubSQL = orderSubSQL.replace("[:allColumns]", f"uniqueFloat_{columnOrder} \n\t\t\t\t, [:allColumns]")
            for tagInd_ in range(1):
                columnOrder = str(tagInd_ + 1).zfill(3)
                orderSubSQL = orderSubSQL.replace("[:allColumns]", f"uniqueJson_{columnOrder} \n\t\t\t\t, [:allColumns]")
            orderSubSQL = orderSubSQL.replace('\n\t\t\t\t, [:allColumns]', f"")
            orderSQL.append(orderSubSQL)
            # print(orderSubSQL)

        return "MakeUseModelOrderSQLInsert", orderSQL, {}

    @classmethod
    def MakeUseModel_AutoTag_M0_4001_99(self, makeInfo):
        # return "MakeUseModelFreeFuction", None, {}

        makeTime = makeInfo['makeTime'].replace('-', '')
        dataTime1 = makeInfo["result"]["rawdata"]['dataTime1']
        dataTime2 = makeInfo["result"]["rawdata"]['dataTime2']
        tagDataMapKeys = makeInfo["result"]["rawdata"]['tagDataMapKeys']
        tag_partition_num = makeInfo["result"]["preprocess"]['tag_partition_num']
        tagDataMapLen = len(tagDataMapKeys)
        orderSqlList = []

        # Model Result
        orderSQL = []
        for tag_partition_ in range(math.ceil(tagDataMapLen/200)):
            orderSubSQL = f"""
                INSERT [:isOverWrite] TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:ModelVersion]_1' , dt = '[:DateNoLine]' , step = 'ModelResult')
                SELECT 
                    [:allColumns]
                FROM gtwpd.model_usedata
                where 1 = 1 
                    AND product = 'maple'
                    AND project = 'AutoTag'
                    AND step = 'PreProcess'
                    AND version = 'P0_4001_1_[:tagPartitionInt]'
                    AND dt = {makeTime};
                """
            if tag_partition_ == 0: orderSubSQL = orderSubSQL.replace(' [:isOverWrite]', f" OVERWRITE")
            else: orderSubSQL = orderSubSQL.replace(' [:isOverWrite]', f" INTO")

            orderSubSQL = orderSubSQL.replace('''[:tagPartitionInt]''', str(tag_partition_ + 1))
            for tagInd_ in range(15):
                columnOrder = str(tagInd_ + 1).zfill(3)
                orderSubSQL = orderSubSQL.replace("[:allColumns]", f"commondata_{columnOrder} \n\t\t\t\t, [:allColumns]")
            for tagInd_ in range(200):
                columnOrder = str(tagInd_ + 1).zfill(3)
                orderSubSQL = orderSubSQL.replace("[:allColumns]", f"uniqueFloat_{columnOrder} \n\t\t\t\t, [:allColumns]")
            for tagInd_ in range(1):
                columnOrder = str(tagInd_ + 1).zfill(3)
                orderSubSQL = orderSubSQL.replace("[:allColumns]", f"uniqueJson_{columnOrder} \n\t\t\t\t, [:allColumns]")
            orderSubSQL = orderSubSQL.replace('\n\t\t\t\t, [:allColumns]', f"")
            orderSQL.append(orderSubSQL)
            # print(orderSubSQL)


        orderSQL2 = f"""
            INSERT OVERWRITE TABLE gtwpd.model_usedata PARTITION ( product = '[:ProductName]' , project = '[:Project]' ,version = '[:ModelVersion]_2' , dt = '[:DateNoLine]' , step = 'ModelResult')
            SELECT
                commondata_001
                , commondata_002
                , commondata_003
                , commondata_004
                , commondata_005
                , commondata_006
                , commondata_007
                , commondata_008
                , commondata_009
                , commondata_010
                , commondata_011
                , commondata_012
                , commondata_013
                , commondata_014
                , commondata_015
                , [:UniqueFloat]
                , UniqueJson_001 
            FROM gtwpd.model_usedata  
            WHERE 1=1
                AND product = 'maple'
                AND project = 'AutoTag'
                AND step = 'ModelResult'
                AND version='V0_4001_1_1'
                AND dt = {makeInfo['makeTime'].replace('-', '')};
        """
        for tagInd_ in range(200):
            columnOrder = str(tagInd_+1).zfill(3)
            orderSQL2 = orderSQL2.replace("[:UniqueFloat]", f"CASE WHEN UniqueFloat_{columnOrder} > 0 THEN 1 ELSE 0 END AS UniqueFloat_{columnOrder} \n\t\t\t\t, [:UniqueFloat]")
        orderSQL2 = orderSQL2.replace("\n\t\t\t\t, [:UniqueFloat]", f"")
        orderSQL.append(orderSQL2)
        # print(orderSQL2)
        return "MakeUseModelOrderSQLInsert", orderSQL , {}

    @classmethod
    def MakeUseModel_AutoTag_M0_9001_1(self, makeInfo):
        return "MakeUseModelFreeFuction", None, {}
