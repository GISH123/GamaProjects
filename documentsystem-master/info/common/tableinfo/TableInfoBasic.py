
class TableInfoBasic() :

    def __init__(self):
        pass

    @classmethod
    def getInitInfoMap(self) :
        return {
            "value": self.getMainXML()
            , "[::DataSourceListXML]": {
                "value": self.getDataSourceListXML()
                , "[::ConnectionXML]": {
                    "value": self.getConnectionXML()
                    , "[::ServerName]": {"value": "[:ServerName]"}
                    , "[::ServerPort]": {"value": "[:ServerPort]"}
                    , "[::DBName]": {"value": "[:DBName]"}
                    , "[::UserName]": {"value": "[:UserName]"}
                    , "[::SelectSQLCode]": {"value": "[:SelectSQLCode]"}
                    , "[::HashCode10Lower]" : {"value": "[:HashCode10Lower]"}
                    , "[::HashCode10Upper]": {"value": "[:HashCode10Upper]"}
                }
                , "[::ColumnListXML]": self.getBasicColumnInfo()
                , "[::ConnectionObjectXML]": {
                    "value": self.getConnectionObjectXML()
                    , "[::DataSourceTableName]": {"value": "[:DataSourceTableName]"}
                    , "[::SelectSQLCode]": {"value": "[:SelectSQLCode]"}
                }
                , "[::DataSourceName]": {"value": "[:DataSourceName]"}
                , "[::DataSourceTableName]": {"value": "[:DataSourceTableName]"}
                , "[::HashCode10Lower]" : {"value": "[:HashCode10Lower]"}
                , "[::HashCode10Upper]": {"value": "[:HashCode10Upper]"}
            }
        }

    @classmethod
    def makeXMLByInfoMap(self,xmlInfoMap):
        xmlStr = ""
        if "value" in xmlInfoMap.keys():
            if isinstance(xmlInfoMap.get("value"),list):
                xmlStr = ""
                for singleInfoMap in xmlInfoMap.get("value") :
                    xmlStr = xmlStr + "\n" + self.makeXMLByInfoMap(singleInfoMap)
            else :
                xmlStr = xmlInfoMap.get("value")
                for key in xmlInfoMap.keys():
                    if "[::" in key:
                        xmlStr = xmlStr.replace(key, self.makeXMLByInfoMap(xmlInfoMap[key]))
        return xmlStr

    # ====================================================================================================
    @classmethod
    def getDataSourceBasicInfo(self):
        return {
            "value": self.getDataSourceListXML()
            , "[::ConnectionXML]": {
                "value": self.getConnectionXML()
                , "[::ServerName]": {"value": "[:ServerName]"}
                , "[::ServerPort]": {"value": "[:ServerPort]"}
                , "[::DBName]": {"value": "[:DBName]"}
                , "[::UserName]": {"value": "[:UserName]"}
                , "[::SelectSQLCode]": {"value": "[:SelectSQLCode]"}
                , "[::HashCode10Lower]" : {"value": "[:HashCode10Lower]"}
                , "[::HashCode10Upper]": {"value": "[:HashCode10Upper]"}
            }
            , "[::ColumnListXML]": self.getBasicColumnInfo()
            , "[::ConnectionObjectListXML]": {
                "value": self.getConnectionObjectXML()
                , "[::DataSourceTableName]": {"value": "[:DataSourceTableName]"}
                , "[::SelectSQLCode]": {"value": "[:SelectSQLCode]"}
            }
            , "[::DataSourceName]": {"value": "[:DataSourceName]"}
            , "[::DataSourceTableName]": {"value": "[:DataSourceTableName]"}
            , "[::HashCode10Lower]" : {"value": "[:HashCode10Lower]"}
            , "[::HashCode10Upper]": {"value": "[:HashCode10Upper]"}
        }

    @classmethod
    def getBasicColumnInfo2(self):
        return  {
            "value": self.getColumnListXML()
            , "[::dt]" : {"value":"<column aggregation='Count' caption='dt' hidden='true' datatype='date' datatype-customized='true' name='[dt]' role='dimension' type='ordinal' />"}
            , "[::game]" : {"value":"<column caption='game' hidden='true' datatype='string' name='[game]' role='dimension' type='nominal' />"}
            , "[::world]" : {"value":"<column caption='world' hidden='true' datatype='string' name='[world]' role='dimension' type='nominal' />"}
            , "[::commondata_1]" : {"value":"<column caption='commondata_1' datatype='string' hidden='true' name='[commondata_1]' role='dimension' type='nominal' />"}
            , "[::commondata_2]" : {"value":"<column caption='commondata_2' datatype='string' hidden='true' name='[commondata_2]' role='dimension' type='nominal' />"}
            , "[::commondata_3]" : {"value":"<column caption='commondata_3' datatype='string' hidden='true' name='[commondata_3]' role='dimension' type='nominal' />"}
            , "[::commondata_4]" : {"value":"<column caption='commondata_4' datatype='string' hidden='true' name='[commondata_4]' role='dimension' type='nominal' />"}
            , "[::commondata_5]" : {"value":"<column caption='commondata_5' datatype='string' hidden='true' name='[commondata_5]' role='dimension' type='nominal' />"}
            , "[::commondata_6]" : {"value":"<column caption='commondata_6' datatype='string' hidden='true' name='[commondata_6]' role='dimension' type='nominal' />"}
            , "[::commondata_7]" : {"value":"<column caption='commondata_7' datatype='string' hidden='true' name='[commondata_7]' role='dimension' type='nominal' />"}
            , "[::commondata_8]" : {"value":"<column caption='commondata_8' datatype='string' hidden='true' name='[commondata_8]' role='dimension' type='nominal' />"}
            , "[::commondata_9]" : {"value":"<column caption='commondata_9' datatype='string' hidden='true' name='[commondata_9]' role='dimension' type='nominal' />"}
            , "[::commondata_10]" : {"value":"<column caption='commondata_10' datatype='string' hidden='true' name='[commondata_10]' role='dimension' type='nominal' />"}
            , "[::commondata_11]" : {"value":"<column caption='commondata_11' datatype='integer' hidden='true' name='[commondata_11]' role='measure' type='quantitative' />"}
            , "[::commondata_12]" : {"value":"<column caption='commondata_12' datatype='integer' hidden='true' name='[commondata_12]' role='measure' type='quantitative' />"}
            , "[::commondata_13]" : {"value":"<column caption='commondata_13' datatype='integer' hidden='true' name='[commondata_13]' role='measure' type='quantitative' />"}
            , "[::commondata_14]" : {"value":"<column caption='commondata_14' datatype='integer' hidden='true' name='[commondata_14]' role='measure' type='quantitative' />"}
            , "[::commondata_15]" : {"value":"<column caption='commondata_15' datatype='integer' hidden='true' name='[commondata_15]' role='measure' type='quantitative' />"}
            , "[::uniqueint_1]" : {"value":"<column caption='uniqueint_1' datatype='integer' hidden='true' name='[uniqueint_1]' role='measure' type='quantitative' />"}
            , "[::uniqueint_2]" : {"value":"<column caption='uniqueint_2' datatype='integer' hidden='true' name='[uniqueint_2]' role='measure' type='quantitative' />"}
            , "[::uniqueint_3]" : {"value":"<column caption='uniqueint_3' datatype='integer' hidden='true' name='[uniqueint_3]' role='measure' type='quantitative' />"}
            , "[::uniqueint_4]" : {"value":"<column caption='uniqueint_4' datatype='integer' hidden='true' name='[uniqueint_4]' role='measure' type='quantitative' />"}
            , "[::uniqueint_5]" : {"value":"<column caption='uniqueint_5' datatype='integer' hidden='true' name='[uniqueint_5]' role='measure' type='quantitative' />"}
            , "[::uniqueint_6]" : {"value":"<column caption='uniqueint_6' datatype='integer' hidden='true' name='[uniqueint_6]' role='measure' type='quantitative' />"}
            , "[::uniqueint_7]" : {"value":"<column caption='uniqueint_7' datatype='integer' hidden='true' name='[uniqueint_7]' role='measure' type='quantitative' />"}
            , "[::uniqueint_8]" : {"value":"<column caption='uniqueint_8' datatype='integer' hidden='true' name='[uniqueint_8]' role='measure' type='quantitative' />"}
            , "[::uniqueint_9]" : {"value":"<column caption='uniqueint_9' datatype='integer' hidden='true' name='[uniqueint_9]' role='measure' type='quantitative' />"}
            , "[::uniqueint_10]" : {"value":"<column caption='uniqueint_10' datatype='integer' hidden='true' name='[uniqueint_10]' role='measure' type='quantitative' />"}
            , "[::uniqueint_11]" : {"value":"<column caption='uniqueint_11' datatype='integer' hidden='true' name='[uniqueint_11]' role='measure' type='quantitative' />"}
            , "[::uniqueint_12]" : {"value":"<column caption='uniqueint_12' datatype='integer' hidden='true' name='[uniqueint_12]' role='measure' type='quantitative' />"}
            , "[::uniqueint_13]" : {"value":"<column caption='uniqueint_13' datatype='integer' hidden='true' name='[uniqueint_13]' role='measure' type='quantitative' />"}
            , "[::uniqueint_14]" : {"value":"<column caption='uniqueint_14' datatype='integer' hidden='true' name='[uniqueint_14]' role='measure' type='quantitative' />"}
            , "[::uniqueint_15]" : {"value":"<column caption='uniqueint_15' datatype='integer' hidden='true' name='[uniqueint_15]' role='measure' type='quantitative' />"}
            , "[::uniquestr_1]" : {"value":"<column caption='uniquestr_1' datatype='string' hidden='true' name='[uniquestr_1]' role='dimension' type='nominal' />"}
            , "[::uniquestr_2]" : {"value":"<column caption='uniquestr_2' datatype='string' hidden='true' name='[uniquestr_2]' role='dimension' type='nominal' />"}
            , "[::uniquestr_3]" : {"value":"<column caption='uniquestr_3' datatype='string' hidden='true' name='[uniquestr_3]' role='dimension' type='nominal' />"}
            , "[::uniquestr_4]" : {"value":"<column caption='uniquestr_4' datatype='string' hidden='true' name='[uniquestr_4]' role='dimension' type='nominal' />"}
            , "[::uniquestr_5]" : {"value":"<column caption='uniquestr_5' datatype='string' hidden='true' name='[uniquestr_5]' role='dimension' type='nominal' />"}
            , "[::uniquestr_6]" : {"value":"<column caption='uniquestr_6' datatype='string' hidden='true' name='[uniquestr_6]' role='dimension' type='nominal' />"}
            , "[::uniquestr_7]" : {"value":"<column caption='uniquestr_7' datatype='string' hidden='true' name='[uniquestr_7]' role='dimension' type='nominal' />"}
            , "[::uniquestr_8]" : {"value":"<column caption='uniquestr_8' datatype='string' hidden='true' name='[uniquestr_8]' role='dimension' type='nominal' />"}
            , "[::uniquestr_9]" : {"value":"<column caption='uniquestr_9' datatype='string' hidden='true' name='[uniquestr_9]' role='dimension' type='nominal' />"}
            , "[::uniquestr_10]" : {"value":"<column caption='uniquestr_10' datatype='string' hidden='true' name='[uniquestr_10]' role='dimension' type='nominal' />"}
            , "[::uniquestr_11]" : {"value":"<column caption='uniquestr_11' datatype='string' hidden='true' name='[uniquestr_11]' role='dimension' type='nominal' />"}
            , "[::uniquestr_12]" : {"value":"<column caption='uniquestr_12' datatype='string' hidden='true' name='[uniquestr_12]' role='dimension' type='nominal' />"}
            , "[::uniquestr_13]" : {"value":"<column caption='uniquestr_13' datatype='string' hidden='true' name='[uniquestr_13]' role='dimension' type='nominal' />"}
            , "[::uniquestr_14]" : {"value":"<column caption='uniquestr_14' datatype='string' hidden='true' name='[uniquestr_14]' role='dimension' type='nominal' />"}
            , "[::uniquestr_15]" : {"value":"<column caption='uniquestr_15' datatype='string' hidden='true' name='[uniquestr_15]' role='dimension' type='nominal' />"}
            , "[::uniquestr_16]" : {"value":"<column caption='uniquestr_16' datatype='string' hidden='true' name='[uniquestr_16]' role='dimension' type='nominal' />"}
            , "[::uniquestr_17]" : {"value":"<column caption='uniquestr_17' datatype='string' hidden='true' name='[uniquestr_17]' role='dimension' type='nominal' />"}
            , "[::uniquestr_18]" : {"value":"<column caption='uniquestr_18' datatype='string' hidden='true' name='[uniquestr_18]' role='dimension' type='nominal' />"}
            , "[::uniquestr_19]" : {"value":"<column caption='uniquestr_19' datatype='string' hidden='true' name='[uniquestr_19]' role='dimension' type='nominal' />"}
            , "[::uniquestr_20]" : {"value":"<column caption='uniquestr_20' datatype='string' hidden='true' name='[uniquestr_20]' role='dimension' type='nominal' />"}
            , "[::uniquedbl_1]" : {"value":"<column caption='uniquedbl_1' datatype='real' hidden='true' name='[uniquedbl_1]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_2]" : {"value":"<column caption='uniquedbl_2' datatype='real' hidden='true' name='[uniquedbl_2]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_3]" : {"value":"<column caption='uniquedbl_3' datatype='real' hidden='true' name='[uniquedbl_3]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_4]" : {"value":"<column caption='uniquedbl_4' datatype='real' hidden='true' name='[uniquedbl_4]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_5]" : {"value":"<column caption='uniquedbl_5' datatype='real' hidden='true' name='[uniquedbl_5]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_6]" : {"value":"<column caption='uniquedbl_6' datatype='real' hidden='true' name='[uniquedbl_6]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_7]" : {"value":"<column caption='uniquedbl_7' datatype='real' hidden='true' name='[uniquedbl_7]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_8]" : {"value":"<column caption='uniquedbl_8' datatype='real' hidden='true' name='[uniquedbl_8]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_9]" : {"value":"<column caption='uniquedbl_9' datatype='real' hidden='true' name='[uniquedbl_9]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_10]" : {"value":"<column caption='uniquedbl_10' datatype='real' hidden='true' name='[uniquedbl_10]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_11]" : {"value":"<column caption='uniquedbl_11' datatype='real' hidden='true' name='[uniquedbl_11]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_12]" : {"value":"<column caption='uniquedbl_12' datatype='real' hidden='true' name='[uniquedbl_12]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_13]" : {"value":"<column caption='uniquedbl_13' datatype='real' hidden='true' name='[uniquedbl_13]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_14]" : {"value":"<column caption='uniquedbl_14' datatype='real' hidden='true' name='[uniquedbl_14]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_15]" : {"value":"<column caption='uniquedbl_15' datatype='real' hidden='true' name='[uniquedbl_15]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_16]" : {"value":"<column caption='uniquedbl_16' datatype='real' hidden='true' name='[uniquedbl_16]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_17]" : {"value":"<column caption='uniquedbl_17' datatype='real' hidden='true' name='[uniquedbl_17]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_18]" : {"value":"<column caption='uniquedbl_18' datatype='real' hidden='true' name='[uniquedbl_18]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_19]" : {"value":"<column caption='uniquedbl_19' datatype='real' hidden='true' name='[uniquedbl_19]' role='measure' type='quantitative' />"}
            , "[::uniquedbl_20]" : {"value":"<column caption='uniquedbl_20' datatype='real' hidden='true' name='[uniquedbl_20]' role='measure' type='quantitative' />"}
            , "[::uniquetime_1]" : {"value":"<column caption='uniquetime_1' datatype='datetime' hidden='true' name='[uniquetime_1]' role='dimension' type='ordinal' />"}
            , "[::uniquetime_2]" : {"value":"<column caption='uniquetime_2' datatype='datetime' hidden='true' name='[uniquetime_2]' role='dimension' type='ordinal' />"}
            , "[::uniquetime_3]" : {"value":"<column caption='uniquetime_3' datatype='datetime' hidden='true' name='[uniquetime_3]' role='dimension' type='ordinal' />"}
            , "[::otherstr_1]" : {"value":"<column caption='otherstr_1' datatype='string' hidden='true' name='[otherstr_1]' role='dimension' type='nominal' />"}
            , "[::otherstr_2]" : {"value":"<column caption='otherstr_2' datatype='string' hidden='true' name='[otherstr_2]' role='dimension' type='nominal' />"}
            , "[::otherstr_3]" : {"value":"<column caption='otherstr_3' datatype='string' hidden='true' name='[otherstr_3]' role='dimension' type='nominal' />"}
            , "[::otherstr_4]" : {"value":"<column caption='otherstr_4' datatype='string' hidden='true' name='[otherstr_4]' role='dimension' type='nominal' />"}
            , "[::otherstr_5]" : {"value":"<column caption='otherstr_5' datatype='string' hidden='true' name='[otherstr_5]' role='dimension' type='nominal' />"}
            , "[::otherstr_6]" : {"value":"<column caption='otherstr_6' datatype='string' hidden='true' name='[otherstr_6]' role='dimension' type='nominal' />"}
            , "[::otherstr_7]" : {"value":"<column caption='otherstr_7' datatype='string' hidden='true' name='[otherstr_7]' role='dimension' type='nominal' />"}
            , "[::otherstr_8]" : {"value":"<column caption='otherstr_8' datatype='string' hidden='true' name='[otherstr_8]' role='dimension' type='nominal' />"}
            , "[::otherstr_9]" : {"value":"<column caption='otherstr_9' datatype='string' hidden='true' name='[otherstr_9]' role='dimension' type='nominal' />"}
            , "[::otherstr_10]" : {"value":"<column caption='otherstr_10' datatype='string' hidden='true' name='[otherstr_10]' role='dimension' type='nominal' />"}
        }

    @classmethod
    def getBasicColumnInfo(self):
        columnInfoMap = {}
        columnInfoMap["dt"] = {"description": "dt", "hidden": "false", "datatype": "string_to_date"}
        columnInfoMap["game"] = {"description": "game", "hidden": "false", "datatype": "string"}
        columnInfoMap["world"] = {"description": "world", "hidden": "false", "datatype": "string"}
        columnInfoMap["commondata_1"] = {"description": "commondata_1", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_2"] = {"description": "commondata_2", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_3"] = {"description": "commondata_3", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_4"] = {"description": "commondata_4", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_5"] = {"description": "commondata_5", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_6"] = {"description": "commondata_6", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_7"] = {"description": "commondata_7", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_8"] = {"description": "commondata_8", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_9"] = {"description": "commondata_9", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_10"] = {"description": "commondata_10", "datatype": "string", "hidden": "true"}
        columnInfoMap["commondata_11"] = {"description": "commondata_11", "datatype": "integer", "hidden": "true"}
        columnInfoMap["commondata_12"] = {"description": "commondata_12", "datatype": "integer", "hidden": "true"}
        columnInfoMap["commondata_13"] = {"description": "commondata_13", "datatype": "integer", "hidden": "true"}
        columnInfoMap["commondata_14"] = {"description": "commondata_14", "datatype": "integer", "hidden": "true"}
        columnInfoMap["commondata_15"] = {"description": "commondata_15", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_1"] = {"description": "uniqueint_1", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_2"] = {"description": "uniqueint_2", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_3"] = {"description": "uniqueint_3", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_4"] = {"description": "uniqueint_4", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_5"] = {"description": "uniqueint_5", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_6"] = {"description": "uniqueint_6", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_7"] = {"description": "uniqueint_7", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_8"] = {"description": "uniqueint_8", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_9"] = {"description": "uniqueint_9", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_10"] = {"description": "uniqueint_10", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_11"] = {"description": "uniqueint_11", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_12"] = {"description": "uniqueint_12", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_13"] = {"description": "uniqueint_13", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_14"] = {"description": "uniqueint_14", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniqueint_15"] = {"description": "uniqueint_15", "datatype": "integer", "hidden": "true"}
        columnInfoMap["uniquestr_1"] = {"description": "uniquestr_1", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_2"] = {"description": "uniquestr_2", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_3"] = {"description": "uniquestr_3", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_4"] = {"description": "uniquestr_4", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_5"] = {"description": "uniquestr_5", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_6"] = {"description": "uniquestr_6", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_7"] = {"description": "uniquestr_7", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_8"] = {"description": "uniquestr_8", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_9"] = {"description": "uniquestr_9", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_10"] = {"description": "uniquestr_10", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_11"] = {"description": "uniquestr_11", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_12"] = {"description": "uniquestr_12", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_13"] = {"description": "uniquestr_13", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_14"] = {"description": "uniquestr_14", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_15"] = {"description": "uniquestr_15", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_16"] = {"description": "uniquestr_16", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_17"] = {"description": "uniquestr_17", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_18"] = {"description": "uniquestr_18", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_19"] = {"description": "uniquestr_19", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquestr_20"] = {"description": "uniquestr_20", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquedbl_1"] = {"description": "uniquedbl_1", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_2"] = {"description": "uniquedbl_2", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_3"] = {"description": "uniquedbl_3", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_4"] = {"description": "uniquedbl_4", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_5"] = {"description": "uniquedbl_5", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_6"] = {"description": "uniquedbl_6", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_7"] = {"description": "uniquedbl_7", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_8"] = {"description": "uniquedbl_8", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_9"] = {"description": "uniquedbl_9", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_10"] = {"description": "uniquedbl_10", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_11"] = {"description": "uniquedbl_11", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_12"] = {"description": "uniquedbl_12", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_13"] = {"description": "uniquedbl_13", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_14"] = {"description": "uniquedbl_14", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_15"] = {"description": "uniquedbl_15", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_16"] = {"description": "uniquedbl_16", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_17"] = {"description": "uniquedbl_17", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_18"] = {"description": "uniquedbl_18", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_19"] = {"description": "uniquedbl_19", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquedbl_20"] = {"description": "uniquedbl_20", "datatype": "real", "hidden": "true"}
        columnInfoMap["uniquetime_1"] = {"description": "uniquetime_1", "datatype": "datetime", "hidden": "true"}
        columnInfoMap["uniquetime_2"] = {"description": "uniquetime_2", "datatype": "datetime", "hidden": "true"}
        columnInfoMap["uniquetime_3"] = {"description": "uniquetime_3", "datatype": "datetime", "hidden": "true"}
        columnInfoMap["otherstr_1"] = {"description": "otherstr_1", "datatype": "string", "hidden": "true"}
        columnInfoMap["otherstr_2"] = {"description": "otherstr_2", "datatype": "string", "hidden": "true"}
        columnInfoMap["otherstr_3"] = {"description": "otherstr_3", "datatype": "string", "hidden": "true"}
        columnInfoMap["otherstr_4"] = {"description": "otherstr_4", "datatype": "string", "hidden": "true"}
        columnInfoMap["otherstr_5"] = {"description": "otherstr_5", "datatype": "string", "hidden": "true"}
        columnInfoMap["otherstr_6"] = {"description": "otherstr_6", "datatype": "string", "hidden": "true"}
        columnInfoMap["otherstr_7"] = {"description": "otherstr_7", "datatype": "string", "hidden": "true"}
        columnInfoMap["otherstr_8"] = {"description": "otherstr_8", "datatype": "string", "hidden": "true"}
        columnInfoMap["otherstr_9"] = {"description": "otherstr_9", "datatype": "string", "hidden": "true"}
        columnInfoMap["otherstr_10"] = {"description": "otherstr_10", "datatype": "string", "hidden": "true"}
        columnInfoMap["uniquearray_1"] = {"description": "uniquearray_1", "datatype": "string", "hidden": "nodata"}
        columnInfoMap["uniquearray_2"] = {"description": "uniquearray_2", "datatype": "string", "hidden": "nodata"}
        columnInfoMap["uniquejson_1"] = {"description": "uniquejson_1", "datatype": "string", "hidden": "nodata"}
        return columnInfoMap

    @classmethod
    def getBasicColumnInitXml(self):
        return {
            "string": "<column caption='[::Description]' datatype='string' hidden='[::Hidden]' name='[[::DataName]]' role='dimension' type='nominal' />"
            , "integer": "<column caption='[::Description]' datatype='integer' hidden='[::Hidden]' name='[[::DataName]]' role='measure' type='quantitative' />"
            , "real": "<column caption='[::Description]' datatype='real' hidden='[::Hidden]' name='[[::DataName]]' role='measure' type='quantitative' />"
            , "datetime": "<column caption='[::Description]' datatype='datetime' hidden='[::Hidden]' name='[[::DataName]]' role='dimension' type='ordinal' />"
            , "string_to_date": "<column aggregation='Count' caption='[::Description]' datatype='date' hidden='[::Hidden]' datatype-customized='true' name='[[::DataName]]' role='dimension' type='ordinal' />"
            , "string_to_real": "<column caption='[::Description]' datatype='real' hidden='[::Hidden]' datatype-customized='true' name='[[::DataName]]' role='measure' type='quantitative' />"
        }

    #====================================================================================================
    @classmethod
    def getMainXML(self):
        return """<?xml version='1.0' encoding='utf-8' ?>

<!-- build 20204.20.1116.1810                               -->
<workbook original-version='18.1' source-build='2020.4.0 (20204.20.1116.1810)' version='18.1' xml:base='http://localhost:8348' xmlns:user='http://www.tableausoftware.com/xml/user'>
  <document-format-change-manifest>
    <_.fcp.MarkAnimation.true...MarkAnimation />
    <_.fcp.ObjectModelEncapsulateLegacy.true...ObjectModelEncapsulateLegacy />
    <_.fcp.ObjectModelTableType.true...ObjectModelTableType />
    <_.fcp.SchemaViewerObjectModel.true...SchemaViewerObjectModel />
    <SheetIdentifierTracking />
    <WindowsPersistSimpleIdentifiers />
  </document-format-change-manifest>
  <repository-location id='Test' path='/t/A_Teach/workbooks' revision='1.0' site='A_Teach' />
  <preferences />
  <datasources>
    [::DataSourceListXML]
  </datasources>
  <worksheets>
    <worksheet name='工作表 1'>
      <repository-location id='1' path='/t/A_Teach/workbooks/Test' revision='' site='A_Teach' />
      <table>
        <view>
          <datasources />
          <aggregation value='true' />
        </view>
        <style />
        <panes>
          <pane selection-relaxation-option='selection-relaxation-allow'>
            <view>
              <breakdown value='auto' />
            </view>
            <mark class='Automatic' />
          </pane>
        </panes>
        <rows />
        <cols />
      </table>
      <simple-id uuid='{654D9F2E-45E1-4C64-AC4D-A09F37879038}' />
    </worksheet>
  </worksheets>
  <windows>
    <window class='worksheet' maximized='true' name='工作表 1'>
      <cards>
        <edge name='left'>
          <strip size='160'>
            <card type='pages' />
            <card type='filters' />
            <card type='marks' />
          </strip>
        </edge>
        <edge name='top'>
          <strip size='31'>
            <card type='columns' />
          </strip>
          <strip size='31'>
            <card type='rows' />
          </strip>
          <strip size='31'>
            <card type='title' />
          </strip>
        </edge>
      </cards>
      <simple-id uuid='{A3DFBA4C-5EFE-4CE5-96E0-C6078CBEFD8A}' />
    </window>
  </windows>
</workbook>"""

    @classmethod
    def getDataSourceListXML(self):
        return """<datasource caption='[::DataSourceName]' inline='true' name='federated.1hdfd8606dilgx1g62[::HashCode10Lower]' version='18.1'>
      [::ConnectionXML]
      <aliases enabled='yes' />
      <_.fcp.ObjectModelTableType.true...column caption='[::DataSourceTableName]' datatype='table' name='[__tableau_internal_object_id__].[_58C0F131E87440AA91CF13[::HashCode10Upper]]' role='measure' type='quantitative' />
      [::ColumnListXML]
      <layout _.fcp.SchemaViewerObjectModel.false...dim-percentage='0.5' _.fcp.SchemaViewerObjectModel.false...measure-percentage='0.4' dim-ordering='alphabetic' measure-ordering='alphabetic' show-structure='true' />
      <semantic-values>
        <semantic-value key='[Country].[Name]' value='&quot;Taiwan&quot;' />
      </semantic-values>
      <_.fcp.ObjectModelEncapsulateLegacy.true...object-graph>
        <objects>
          [::ConnectionObjectListXML]  
        </objects>
      </_.fcp.ObjectModelEncapsulateLegacy.true...object-graph>
    </datasource>"""

    @classmethod
    def getConnectionXML(self):
        return """<connection class='federated'>
        <named-connections>
          <named-connection caption='[::ServerName]' name='greenplum.0isr80o1ah8mra1bcs[::HashCode10Lower]'>
            <connection authentication='' class='greenplum' dbname='[:DBName]' odbc-native-protocol='' one-time-sql='' port='[::ServerPort]' server='[::ServerName]' sslmode='' username='[::UserName]' workgroup-auth-mode='' />
          </named-connection>
        </named-connections>
        <_.fcp.ObjectModelEncapsulateLegacy.false...relation connection='greenplum.0isr80o1ah8mra1bcs[::HashCode10Lower]' name='X__SQL___' type='text'>[::SelectSQLCode]</_.fcp.ObjectModelEncapsulateLegacy.false...relation>
        <_.fcp.ObjectModelEncapsulateLegacy.true...relation connection='greenplum.0isr80o1ah8mra1bcs[::HashCode10Lower]' name='X__SQL___' type='text'>[::SelectSQLCode]</_.fcp.ObjectModelEncapsulateLegacy.true...relation>
        <metadata-records>
          <metadata-record class='column'>
            <remote-name>dt</remote-name>
            <remote-type>130</remote-type>
            <local-name>[dt]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>dt</remote-alias>
            <ordinal>1</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>world</remote-name>
            <remote-type>130</remote-type>
            <local-name>[world]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>world</remote-alias>
            <ordinal>2</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>game</remote-name>
            <remote-type>130</remote-type>
            <local-name>[game]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>game</remote-alias>
            <ordinal>3</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_1</remote-name>
            <remote-type>130</remote-type>
            <local-name>[commondata_1]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_1</remote-alias>
            <ordinal>4</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_2</remote-name>
            <remote-type>130</remote-type>
            <local-name>[commondata_2]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_2</remote-alias>
            <ordinal>5</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_3</remote-name>
            <remote-type>130</remote-type>
            <local-name>[commondata_3]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_3</remote-alias>
            <ordinal>6</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_4</remote-name>
            <remote-type>130</remote-type>
            <local-name>[commondata_4]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_4</remote-alias>
            <ordinal>7</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_5</remote-name>
            <remote-type>130</remote-type>
            <local-name>[commondata_5]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_5</remote-alias>
            <ordinal>8</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_6</remote-name>
            <remote-type>130</remote-type>
            <local-name>[commondata_6]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_6</remote-alias>
            <ordinal>9</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_7</remote-name>
            <remote-type>130</remote-type>
            <local-name>[commondata_7]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_7</remote-alias>
            <ordinal>10</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_8</remote-name>
            <remote-type>130</remote-type>
            <local-name>[commondata_8]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_8</remote-alias>
            <ordinal>11</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_9</remote-name>
            <remote-type>130</remote-type>
            <local-name>[commondata_9]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_9</remote-alias>
            <ordinal>12</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_10</remote-name>
            <remote-type>130</remote-type>
            <local-name>[commondata_10]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_10</remote-alias>
            <ordinal>13</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_11</remote-name>
            <remote-type>20</remote-type>
            <local-name>[commondata_11]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_11</remote-alias>
            <ordinal>14</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_12</remote-name>
            <remote-type>20</remote-type>
            <local-name>[commondata_12]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_12</remote-alias>
            <ordinal>15</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_13</remote-name>
            <remote-type>20</remote-type>
            <local-name>[commondata_13]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_13</remote-alias>
            <ordinal>16</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_14</remote-name>
            <remote-type>20</remote-type>
            <local-name>[commondata_14]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_14</remote-alias>
            <ordinal>17</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>commondata_15</remote-name>
            <remote-type>20</remote-type>
            <local-name>[commondata_15]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>commondata_15</remote-alias>
            <ordinal>18</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_1</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_1]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_1</remote-alias>
            <ordinal>19</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_2</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_2]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_2</remote-alias>
            <ordinal>20</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_3</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_3]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_3</remote-alias>
            <ordinal>21</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_4</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_4]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_4</remote-alias>
            <ordinal>22</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_5</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_5]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_5</remote-alias>
            <ordinal>23</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_6</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_6]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_6</remote-alias>
            <ordinal>24</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_7</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_7]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_7</remote-alias>
            <ordinal>25</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_8</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_8]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_8</remote-alias>
            <ordinal>26</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_9</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_9]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_9</remote-alias>
            <ordinal>27</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_10</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_10]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_10</remote-alias>
            <ordinal>28</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_11</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_11]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_11</remote-alias>
            <ordinal>29</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_12</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_12]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_12</remote-alias>
            <ordinal>30</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_13</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_13]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_13</remote-alias>
            <ordinal>31</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_14</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_14]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_14</remote-alias>
            <ordinal>32</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniqueint_15</remote-name>
            <remote-type>20</remote-type>
            <local-name>[uniqueint_15]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniqueint_15</remote-alias>
            <ordinal>33</ordinal>
            <local-type>integer</local-type>
            <aggregation>Sum</aggregation>
            <precision>19</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_BIGINT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_SBIGINT&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_1</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_1]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_1</remote-alias>
            <ordinal>34</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_2</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_2]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_2</remote-alias>
            <ordinal>35</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_3</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_3]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_3</remote-alias>
            <ordinal>36</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_4</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_4]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_4</remote-alias>
            <ordinal>37</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_5</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_5]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_5</remote-alias>
            <ordinal>38</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_6</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_6]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_6</remote-alias>
            <ordinal>39</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_7</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_7]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_7</remote-alias>
            <ordinal>40</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_8</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_8]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_8</remote-alias>
            <ordinal>41</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_9</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_9]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_9</remote-alias>
            <ordinal>42</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_10</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_10]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_10</remote-alias>
            <ordinal>43</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_11</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_11]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_11</remote-alias>
            <ordinal>44</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_12</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_12]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_12</remote-alias>
            <ordinal>45</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_13</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_13]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_13</remote-alias>
            <ordinal>46</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_14</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_14]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_14</remote-alias>
            <ordinal>47</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_15</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_15]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_15</remote-alias>
            <ordinal>48</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_16</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_16]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_16</remote-alias>
            <ordinal>49</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_17</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_17]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_17</remote-alias>
            <ordinal>50</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_18</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_18]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_18</remote-alias>
            <ordinal>51</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_19</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_19]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_19</remote-alias>
            <ordinal>52</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquestr_20</remote-name>
            <remote-type>130</remote-type>
            <local-name>[uniquestr_20]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquestr_20</remote-alias>
            <ordinal>53</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_1</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_1]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_1</remote-alias>
            <ordinal>54</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_2</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_2]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_2</remote-alias>
            <ordinal>55</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_3</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_3]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_3</remote-alias>
            <ordinal>56</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_4</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_4]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_4</remote-alias>
            <ordinal>57</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_5</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_5]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_5</remote-alias>
            <ordinal>58</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_6</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_6]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_6</remote-alias>
            <ordinal>59</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_7</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_7]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_7</remote-alias>
            <ordinal>60</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_8</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_8]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_8</remote-alias>
            <ordinal>61</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_9</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_9]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_9</remote-alias>
            <ordinal>62</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_10</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_10]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_10</remote-alias>
            <ordinal>63</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_11</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_11]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_11</remote-alias>
            <ordinal>64</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_12</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_12]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_12</remote-alias>
            <ordinal>65</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_13</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_13]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_13</remote-alias>
            <ordinal>66</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_14</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_14]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_14</remote-alias>
            <ordinal>67</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_15</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_15]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_15</remote-alias>
            <ordinal>68</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_16</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_16]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_16</remote-alias>
            <ordinal>69</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_17</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_17]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_17</remote-alias>
            <ordinal>70</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_18</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_18]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_18</remote-alias>
            <ordinal>71</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_19</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_19]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_19</remote-alias>
            <ordinal>72</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquedbl_20</remote-name>
            <remote-type>5</remote-type>
            <local-name>[uniquedbl_20]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquedbl_20</remote-alias>
            <ordinal>73</ordinal>
            <local-type>real</local-type>
            <aggregation>Sum</aggregation>
            <precision>15</precision>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_FLOAT&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_DOUBLE&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquetime_1</remote-name>
            <remote-type>7</remote-type>
            <local-name>[uniquetime_1]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquetime_1</remote-alias>
            <ordinal>74</ordinal>
            <local-type>datetime</local-type>
            <aggregation>Year</aggregation>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_TYPE_TIMESTAMP&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_TYPE_TIMESTAMP&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquetime_2</remote-name>
            <remote-type>7</remote-type>
            <local-name>[uniquetime_2]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquetime_2</remote-alias>
            <ordinal>75</ordinal>
            <local-type>datetime</local-type>
            <aggregation>Year</aggregation>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_TYPE_TIMESTAMP&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_TYPE_TIMESTAMP&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>uniquetime_3</remote-name>
            <remote-type>7</remote-type>
            <local-name>[uniquetime_3]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>uniquetime_3</remote-alias>
            <ordinal>76</ordinal>
            <local-type>datetime</local-type>
            <aggregation>Year</aggregation>
            <contains-null>true</contains-null>
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_TYPE_TIMESTAMP&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_TYPE_TIMESTAMP&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>otherstr_1</remote-name>
            <remote-type>130</remote-type>
            <local-name>[otherstr_1]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>otherstr_1</remote-alias>
            <ordinal>77</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>otherstr_2</remote-name>
            <remote-type>130</remote-type>
            <local-name>[otherstr_2]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>otherstr_2</remote-alias>
            <ordinal>78</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>otherstr_3</remote-name>
            <remote-type>130</remote-type>
            <local-name>[otherstr_3]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>otherstr_3</remote-alias>
            <ordinal>79</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>otherstr_4</remote-name>
            <remote-type>130</remote-type>
            <local-name>[otherstr_4]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>otherstr_4</remote-alias>
            <ordinal>80</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>otherstr_5</remote-name>
            <remote-type>130</remote-type>
            <local-name>[otherstr_5]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>otherstr_5</remote-alias>
            <ordinal>81</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>otherstr_6</remote-name>
            <remote-type>130</remote-type>
            <local-name>[otherstr_6]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>otherstr_6</remote-alias>
            <ordinal>82</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>otherstr_7</remote-name>
            <remote-type>130</remote-type>
            <local-name>[otherstr_7]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>otherstr_7</remote-alias>
            <ordinal>83</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>otherstr_8</remote-name>
            <remote-type>130</remote-type>
            <local-name>[otherstr_8]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>otherstr_8</remote-alias>
            <ordinal>84</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>otherstr_9</remote-name>
            <remote-type>130</remote-type>
            <local-name>[otherstr_9]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>otherstr_9</remote-alias>
            <ordinal>85</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
          <metadata-record class='column'>
            <remote-name>otherstr_10</remote-name>
            <remote-type>130</remote-type>
            <local-name>[otherstr_10]</local-name>
            <parent-name>[X__SQL___]</parent-name>
            <remote-alias>otherstr_10</remote-alias>
            <ordinal>86</ordinal>
            <local-type>string</local-type>
            <aggregation>Count</aggregation>
            <width>8190</width>
            <contains-null>true</contains-null>
            <collation flag='0' name='LEN_RUS_WO' />
            <attributes>
              <attribute datatype='string' name='DebugRemoteType'>&quot;SQL_WLONGVARCHAR&quot;</attribute>
              <attribute datatype='string' name='DebugWireType'>&quot;SQL_C_WCHAR&quot;</attribute>
            </attributes>
            <_.fcp.ObjectModelEncapsulateLegacy.true...object-id>[_58C0F131E87440AA91CF13[::HashCode10Upper]]</_.fcp.ObjectModelEncapsulateLegacy.true...object-id>
          </metadata-record>
        </metadata-records>
      </connection>"""

    @classmethod
    def getColumnListXML(self):
        return """
      [::dt]
      [::game]
      [::world]
      [::commondata_1]
      [::commondata_2]
      [::commondata_3]
      [::commondata_4]
      [::commondata_5]
      [::commondata_6]
      [::commondata_7]
      [::commondata_8]
      [::commondata_9]
      [::commondata_10]
      [::commondata_11]
      [::commondata_12]
      [::commondata_13]
      [::commondata_14]
      [::commondata_15]
      [::uniqueint_1]
      [::uniqueint_2]
      [::uniqueint_3]
      [::uniqueint_4]
      [::uniqueint_5]
      [::uniqueint_6]
      [::uniqueint_7]
      [::uniqueint_8]
      [::uniqueint_9]
      [::uniqueint_10]
      [::uniqueint_11]
      [::uniqueint_12]
      [::uniqueint_13]
      [::uniqueint_14]
      [::uniqueint_15]
      [::uniquestr_1]
      [::uniquestr_2]
      [::uniquestr_3]
      [::uniquestr_4]
      [::uniquestr_5]
      [::uniquestr_6]
      [::uniquestr_7]
      [::uniquestr_8]
      [::uniquestr_9]
      [::uniquestr_10]
      [::uniquestr_11]
      [::uniquestr_12]
      [::uniquestr_13]
      [::uniquestr_14]
      [::uniquestr_15]
      [::uniquestr_16]
      [::uniquestr_17]
      [::uniquestr_18]
      [::uniquestr_19]
      [::uniquestr_20]
      [::uniquedbl_1]
      [::uniquedbl_2]
      [::uniquedbl_3]
      [::uniquedbl_4]
      [::uniquedbl_5]
      [::uniquedbl_6]
      [::uniquedbl_7]
      [::uniquedbl_8]
      [::uniquedbl_9]
      [::uniquedbl_10]
      [::uniquedbl_11]
      [::uniquedbl_12]
      [::uniquedbl_13]
      [::uniquedbl_14]
      [::uniquedbl_15]
      [::uniquedbl_16]
      [::uniquedbl_17]
      [::uniquedbl_18]
      [::uniquedbl_19]
      [::uniquedbl_20]
      [::uniquetime_1]
      [::uniquetime_2]
      [::uniquetime_3]
      [::otherstr_1]
      [::otherstr_2]
      [::otherstr_3]
      [::otherstr_4]
      [::otherstr_5]
      [::otherstr_6]
      [::otherstr_7]
      [::otherstr_8]
      [::otherstr_9]
      [::otherstr_10]"""

    @classmethod
    def getConnectionObjectXML(self):
        return """<object caption='[:DataSourceTableName]' id='_58C0F131E87440AA91CF13[::HashCode10Upper]'>
            <properties context=''>
              <relation connection='greenplum.0isr80o1ah8mra1bcs[::HashCode10Lower]' name='X__SQL___' type='text'>[::SelectSQLCode]</relation>
            </properties>
          </object>"""
