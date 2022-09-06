import pandas

class RawTextFunction():

    def __init__(self):
        pass

    def MakeQuantileTextExcludeZeroDF(self):
        # 後續請使用 CommonFunction GetLimitTextExcludeZeroSQL 做後續
        # df = ""
        tagDataMap = {
            'L10': {
                'enname': 'L10'
                , 'cnname': 'L10'
                , 'tagorder': '1'
                , 'memo': 'Q10-Q09'
                , 'jsonmessage': '{}'
                , 'tagshape': [1, 0.9]
            }, 'L09': {
                'enname': 'L09'
                , 'cnname': 'L09'
                , 'tagorder': '2'
                , 'memo': 'Q09-Q08'
                , 'jsonmessage': '{}'
                , 'tagshape': [0.9, 0.8]
            }, 'L08': {
                'enname': 'L08'
                , 'cnname': 'L08'
                , 'tagorder': '3'
                , 'memo': 'Q08-Q07'
                , 'jsonmessage': '{}'
                , 'tagshape': [0.8, 0.7]
            }, 'L07': {
                'enname': 'L07'
                , 'cnname': 'L07'
                , 'tagorder': '4'
                , 'memo': 'Q07-Q06'
                , 'jsonmessage': '{}'
                , 'tagshape': [0.7, 0.6]
            }, 'L06': {
                'enname': 'L06'
                , 'cnname': 'L06'
                , 'tagorder': '5'
                , 'memo': 'Q06-Q05'
                , 'jsonmessage': '{}'
                , 'tagshape': [0.6, 0.5]
            }, 'L05': {
                'enname': 'L05'
                , 'cnname': 'L05'
                , 'tagorder': '6'
                , 'memo': 'Q05-Q04'
                , 'jsonmessage': '{}'
                , 'tagshape': [0.5, 0.4]
            }, 'L04': {
                'enname': 'L04'
                , 'cnname': 'L04'
                , 'tagorder': '7'
                , 'memo': 'Q04-Q03'
                , 'jsonmessage': '{}'
                , 'tagshape': [0.4, 0.3]
            }, 'L03': {
                'enname': 'L03'
                , 'cnname': 'L03'
                , 'tagorder': '8'
                , 'memo': 'Q03-Q02'
                , 'jsonmessage': '{}'
                , 'tagshape': [0.3, 0.2]
            }, 'L02': {
                'enname': 'L02'
                , 'cnname': 'L02'
                , 'tagorder': '9'
                , 'memo': 'Q02-Q01'
                , 'jsonmessage': '{}'
                , 'tagshape': [0.2, 0.1]
            }, 'L01': {
                'enname': 'L01'
                , 'cnname': 'L01'
                , 'tagorder': '10'
                , 'memo': 'Q01-Q00'
                , 'jsonmessage': '{}'
                , 'tagshape': [0.1, 0]
            }, 'L00': {
                'enname': 'L00'
                , 'cnname': 'L00'
                , 'tagorder': '11'
                , 'memo': 'Zero'
                , 'jsonmessage': '{}'
                , 'tagshape': [0, -0.1]
            }
        }

        tagDictionarys = []
        for key in tagDataMap.keys():
            tagData = tagDataMap[key]
            tagDictionary = []
            tagDictionary.append(tagData['enname'])
            tagDictionary.append(tagData['cnname'])
            tagDictionary.append(tagData['tagorder'])
            tagDictionary.append(tagData['memo'])
            tagDictionary.append(tagData['jsonmessage'])
            tagDictionary.append(tagData['tagshape'][0])
            tagDictionary.append(tagData['tagshape'][1])
            tagDictionarys.append(tagDictionary)

        df = pandas.DataFrame(tagDictionarys)

        df.columns = [
            "commondata_011"  # 標籤名稱(英)
            , "commondata_012"  # 標籤名稱(中)
            , "commondata_013"  # 標籤順序
            , "commondata_014"  # 標籤說明
            , "commondata_015"  # 其他說明JSON
            , "uniquefloat_001"  # 上限
            , "uniquefloat_002"  # 下限
        ]
        return df


