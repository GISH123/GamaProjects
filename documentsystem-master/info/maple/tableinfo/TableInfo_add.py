from info.common.tableinfo.TableInfoBasic import TableInfoBasic as TableInfoBasic
from info.maple.tableinfo.TableInfoMain import TableInfoMain as TableInfoMain
import package.common.document.documentTool as documentTool

class TableInfo_add(TableInfoBasic):
    @classmethod
    def getBURport1806_22002_21012Info(self, makeInfo):

        # 要合併的tableNumber
        oriTbList = ['1806','22002','21012']
        # tableNumber -> 要由哪些欄位進行合併
        commonCols = {'commondata_1':  ['1806','22002','21012'],
                      'commondata_2':  ['1806','22002','21012'],
                      'commondata_3':  ['22002','21012'],
                      'commondata_4':  ['22002','21012'],
                      'commondata_5':  ['1806'],
                      'commondata_6':  ['1806'],
                      'commondata_7':  ['1806'],
                      'commondata_9':  ['22002'],
                      'world'       :  ['22002','21012'],
                      'dt'          :  ['1806','22002','21012'],
                      'game'        :  ['1806','22002','21012']
                      }

        dataCode = '_'.join(oriTbList)
        # 輸入合併後之表格資訊，含資料來源名稱等
        tableInfo = {
            "dataCode": 'CD' + dataCode
            , "tableNumber": dataCode
            , "dataName": "帳號IP登入 + 戰鬥指標 + 真實用戶(角色)"
            , "memo": "帳號IP登入 + 戰鬥指標 + 真實用戶(角色)"
            , "dataType": "ComboData"
        }
        return self.__mergeBURport(makeInfo, oriTbList, commonCols, tableInfo)

    @classmethod
    def getBURport1003_2001_2002Info(self, makeInfo):

        # 要合併的tableNumber
        oriTbList = ['1003','2001','2002']
        # tableNumber -> 要由哪些欄位進行合併
        commonCols = {'commondata_1':  ['1003','2001','2002'],
                      'commondata_2':  ['1003','2001','2002'],
                      'commondata_3':  ['1003','2001','2002'],
                      'commondata_4':  ['1003','2001','2002'],
                      'commondata_5':  ['1003'],
                      'commondata_6':  ['1003'],
                      'commondata_7':  ['1003'],
                      'world'       :  ['1003','2001','2002'],
                      'dt'          :  ['1003','2001','2002'],
                      'game'        :  ['1003','2001','2002']
                      }

        dataCode = '_'.join(oriTbList)
        # 輸入合併後之表格資訊，含資料來源名稱等
        tableInfo = {
            "dataCode": 'CD' + dataCode
            , "tableNumber": dataCode
            , "dataName": "遊戲在線角色基本資料 + 等級經驗聲望變化量"
            , "memo": "遊戲在線角色基本資料 + 等級經驗聲望變化量"
            , "dataType": "ComboData"
        }
        return self.__mergeBURport(makeInfo, oriTbList, commonCols, tableInfo)
    
    @classmethod
    def getBURport6019_2001_2002Info(self, makeInfo):

        # 要合併的tableNumber
        oriTbList = ['6019', '2001', '2002']
        # tableNumber -> 要由哪些欄位進行合併
        commonCols = {'commondata_1': ['6019', '2001', '2002'],
                      'commondata_2': ['6019', '2001', '2002'],
                      'commondata_3': ['6019', '2001', '2002'],
                      'commondata_4': ['6019', '2001', '2002'],
                      'commondata_5': ['6019'],
                      'commondata_6': ['6019'],
                      'commondata_7': ['6019'],
                      'commondata_9': ['6019'],
                      'world': ['6019', '2001', '2002'],
                      'dt': ['6019', '2001', '2002'],
                      'game': ['6019', '2001', '2002']
                      }

        dataCode = '_'.join(oriTbList)
        # 輸入合併後之表格資訊，含資料來源名稱等
        tableInfo = {
            "dataCode": 'CD' + dataCode
            , "tableNumber": dataCode
            , "dataName": "角色基本資料 + 等級經驗聲望變化量 + 道具消費細項"
            , "memo": "角色基本資料 + 等級經驗聲望變化量 + 道具消費細項"
            , "dataType": "ComboData"
        }
        return self.__mergeBURport(makeInfo, oriTbList, commonCols, tableInfo)

    @classmethod
    def __mergeBURport(self,makeInfo, oriTbList, commonCols, tableInfo):
        dataCode = '_'.join(oriTbList)
        resultDict ={}
        tableInfoMain = TableInfoMain()
        for i in oriTbList:
            comm = f"tableInfoMain.getBUReport{i}Info(makeInfo)[1]"
            #print(eval(comm))
            resultDict[i] = eval(comm)
        tableauInfoMap = self.getDataSourceBasicInfo()
        columnListMap = {
            "value": self.getColumnListXML()
        }

        columnInfoMap = self.getBasicColumnInfo()
        columnInfoMap["tableInfo"] = tableInfo
        combineDataList = documentTool.getCombineDict(oriTbList,resultDict,commonCols,makeInfo)
        columnInfoMap["tableInfo"] = tableInfo

        for col in combineDataList[0].keys():
            columnInfoMap[col] = combineDataList[0][col]
        #print( columnInfoMap)
        xmlReplaceArr = [
            ["[:DataSourceName]", "{} {} {}".format(tableInfo["dataCode"], makeInfo["gameCHName"], tableInfo["dataName"])]
            , ["[:DataSourceTableName]", "{}".format(tableInfo["dataName"])]
            , ["[:ServerName]", makeInfo["serverName"]]
            , ["[:ServerPort]", makeInfo["serverPort"]]
            , ["[:DBName]", makeInfo["dbName"]]
            , ["[:UserName]", makeInfo["userName"]]
            , ["[:SelectSQLCode]", combineDataList[1]]
            , ["[:HashCode10Lower]", f"0000bu{dataCode}"]
            , ["[:HashCode10Upper]", f"0000BU{dataCode}"]
        ]
        
        for columnInfoKey in columnInfoMap.keys():
            if columnInfoKey != "tableInfo":
                xmlStr = self.getBasicColumnInitXml()[columnInfoMap[columnInfoKey]["datatype"]]
                xmlStr = xmlStr.replace("[::Description]", columnInfoMap[columnInfoKey]["description"])
                xmlStr = xmlStr.replace("[::Hidden]", columnInfoMap[columnInfoKey]["hidden"])
                xmlStr = xmlStr.replace("[::DataName]", columnInfoKey)
                columnListMap["[::{}]".format(columnInfoKey)] = {"value": xmlStr}
    
        tableauInfoMap["[::ColumnListXML]"] = columnListMap
        tableauXMLStr = self.makeXMLByInfoMap(tableauInfoMap)
        for xmlReplace in xmlReplaceArr:
            tableauXMLStr = tableauXMLStr.replace(xmlReplace[0], xmlReplace[1])
        return {"value": tableauXMLStr}, columnInfoMap



