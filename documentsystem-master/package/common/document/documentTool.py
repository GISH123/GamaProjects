
__countDict = {'commondata': 15, 'uniqueint': 15, 'uniquestr': 20, 'uniquedbl': 20, 'uniquetime': 3, 'otherstr' :10}

def getColList():
    colList = ['dt', 'world', 'game',] +\
              [f'commondata_{i + 1}' for i in range(__countDict['commondata'])] +\
              [f'uniqueint_{i + 1}' for i in range(__countDict['uniqueint'])] +\
              [f'uniquestr_{i + 1}' for i in range(__countDict['uniquestr'])] +\
              [f'uniquedbl_{i + 1}' for i in range(__countDict['uniquedbl'])] +\
              [f'uniquetime_{i + 1}' for i in range(__countDict['uniquetime'])] +\
              [f'otherstr_{i + 1}' for i in range(__countDict['otherstr'])]
    return colList

def getCombineDict(oriTbList, infoDict,  commonCols, makeInfo ,printDetail = False):
    oriTbList.sort()
    for key in commonCols.keys():
        commonCols[key].sort()
    infoDict = {tb: __removeHiddenValues(infoDict[tb]) for tb in infoDict.keys()}
    if printDetail:
        print(infoDict)
    commonCols = {col.lower() : commonCols[col] for col in commonCols.keys()}
    mergeDict  = __createMergeMap(oriTbList, infoDict,printDetail)
    return  __infoMap(commonCols,mergeDict, infoDict) , __mergeSQLNew(commonCols,mergeDict,oriTbList,makeInfo,printDetail)

def __removeHiddenValues(input):
    return {key: input[key] for key in (input.keys() - ['tableInfo']) if input[key]['hidden'].lower() in ('false', 'f')}

def __createMergeMap(oriTbList,infoDict,printDetail = False):
    resturnDict = {}
    otherList = []
    for dataType in __countDict.keys() - ['commondata', 'otherstr']:
        dataLen = __countDict[dataType]
        # 先抓各表格資料內容
        datalist = []
        for tb in oriTbList:
            for i in range(dataLen):
                colName = f"{dataType}_{i+1}"
                if colName in infoDict[tb].keys():
                    datalist += [[tb,colName]]
        if len(datalist) > dataLen:
            otherList += datalist[dataLen:]
            datalist = datalist[:dataLen]

        for i in range(len(datalist)):
            colName = f"{dataType}_{i + 1}"
            resturnDict[colName] = datalist[i]

    dataType = 'otherstr'
    for i in range(len(otherList)):
        colName = f"{dataType}_{i + 1}"
        resturnDict[colName] = otherList[i]
    if printDetail:
        print(resturnDict)
    return resturnDict


def __infoMap(commonCols,mergeDict,infoDict):
    mergedInfoMap = {}
    print(commonCols)
    for key in commonCols.keys():
        tb = commonCols[key][0]
        mergedInfoMap[key] = infoDict[tb][key]
    for key in mergeDict.keys():
        tb, col = mergeDict[key][0] , mergeDict[key][1]
        mergedInfoMap[key] = infoDict[tb][col]
        mergedInfoMap[key]['memo'] = f"{tb} : {col}"
    print('mergedInfoMap: ',mergedInfoMap)
    return mergedInfoMap


def __mergeSQL(commonCols, mergeDict,oriTbList,makeInfo):

    # 欄位處理

    allcolList = getColList()
    sqlColList = []
    joinColList = []
    for col in allcolList :
        if col in commonCols.keys():
            if commonCols[col] =='join':
                sqlColList += [col]
                joinColList += [col]
            else:
                sqlColList += ["MAX(CASE WHEN tn = {0} THEN {1} END) AS {1}".format(commonCols[col],col)]
        elif col in mergeDict.keys():
            sqlColList += [f"MAX(CASE WHEN tn = {mergeDict[col][0]} THEN {mergeDict[col][1]} END) AS {col}"]
        else:
            sqlColList += [f"NULL AS {col}"]
    #print(sqlColList)

    sqlTableList = [ "SELECT *, {0} AS tn FROM {1}.bu{0}".format(tb, makeInfo["schemaName"])   for tb in oriTbList]
    mergeSQL =  "SELECT " + "\n, ".join(sqlColList) + " FROM \n  ( " + "\n UNION ALL \n".join(sqlTableList) +")AA GROUP BY " + ','.join(joinColList)

    return mergeSQL

def __mergeSQLNew(commonCols, mergeDict,oriTbList,makeInfo ,printSQL = False):

    # 欄位處理
    scm = makeInfo['schemaName']
    allcolList = getColList()
    sqlColList = []
    for col in allcolList :
        if col in commonCols.keys():
            sqlColList += [f" bu{commonCols[col][0]}.{col} AS {col}"]
        elif col in mergeDict.keys():
            sqlColList += [f" bu{'.'.join(mergeDict[col])} AS {col}"]
        else:
            sqlColList += [f"NULL AS {col}"]
    mergeSQL =  "SELECT " + "\n, ".join(sqlColList) + f" FROM \n {scm}.bu{oriTbList[0]} \n"

    for tb in oriTbList[1:]:
        onList = []
        for key in commonCols.keys():
            if len(commonCols[key]) >= 2:
                if commonCols[key][1] == tb:
                    onList.append(f'bu{commonCols[key][0]}.{key} = bu{commonCols[key][1]}.{key} \n')
                    commonCols[key].pop(1)
        mergeSQL += f" INNER JOIN {scm}.bu{tb} ON " + 'AND '.join(onList)
    if printSQL:
        print(mergeSQL)
    return mergeSQL