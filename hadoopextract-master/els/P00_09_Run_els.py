import os , datetime ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import els.P02_01_DataSize as dataSizeCtrl
import els.P02_02_DataCheck as dataCheckCtrl
import els.P02_04_RestoreMakeSH as restoreMakeSHCtel
import els.P02_05_RestoreMakeData as restoreMakeDataCtrl
import els.P02_10_MakeTableAndPartition as makeTableAndPartitionCtrl
import els.P11_00_MakeInitExtract as makeInitExtractCtrl
import els.P11_10_MakeALL as makeALLCtrl
import els.P11_11_MakeALLMonthFirst as makeALLMonthFirstCtrl

def main(makeTimeStr = ""):
    mainALLData(makeTimeStr)

def mainALLData(makeTimeStr = ""):
    ####################################################################################################
    nowTime = datetime.datetime.now()
    nowZeroTime = datetime.datetime(nowTime.year, nowTime.month, nowTime.day, 0, 0, 0, 0)
    makeDateStrArr = [] if makeTimeStr == "" else [makeTimeStr]
    startDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    endDateStr = (nowZeroTime - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    gameName = "els"
    hdfsDBName = "DB"
    hiveDBName = "DB"

    partitionRunAll = "True"  # 02_10 階段 如何重作 ALL Partition
    partitionRunRestore = "True"  # 02_10 階段 如何重作 Restore Partition

    typeArray = []  # 11_10 階段 如何重作中繼站資料
    tableNameArray = []  # 11_10 階段 如何重作中繼站資料

    worldNameArr = []  # 11_20 階段 如何建立建模前資料
    makeCodeArr = []  # 11_20 階段 如何建立建模前資料
    fileCodeArr = []  # 11_20 階段 如何建立建模前資料

    partitionWorldNameArr = worldNameArr  # 11_21 階段 如何建立建模前Partition
    partitionMakeCodeArr = makeCodeArr  # 11_21 階段 如何建立建模前Partition
    ####################################################################################################
    runDataSize = False
    runDataCheck = False
    runRestoreMakeSH = False
    runRestoreMakeData = False
    runMakeTableAndPartition = True
    runMakeInitExtract = True
    runMakeALL = True
    ####################################################################################################
    if True == True :
        #makeDateStrArr = ["2020-08-15", "2020-08-15", "2020-08-15", "2020-08-14", "2020-08-15", "2020-08-01"]
        startDateStr = "2020-10-31"
        endDateStr = "2020-12-14"

        gameName = "els"
        hdfsDBName = "DB"
        hiveDBName = "DB"

        partitionRunAll = "True"                   # 02_10 階段 如何重作 ALL Partition
        partitionRunRestore = "True"                # 02_10 階段 如何重作 Restore Partition

        typeArray = []                              # 11_10 階段 如何重作中繼站資料
        tableNameArray = []                         # 11_10 階段 如何重作中繼站資料

        worldNameArr = []                           # 11_20 階段 如何建立建模前資料
        makeCodeArr = []                            # 11_20 階段 如何建立建模前資料
        fileCodeArr = []                            # 11_20 階段 如何建立建模前資料

        partitionWorldNameArr = worldNameArr        # 11_21 階段 如何建立建模前Partition
        partitionMakeCodeArr = makeCodeArr          # 11_21 階段 如何建立建模前Partition
        ################################################################################################
        runDataSize = False
        runDataCheck = False
        runRestoreMakeSH = False
        runRestoreMakeData = False
        runMakeTableAndPartition = True
        runMakeInitExtract = True
        runMakeALL = True
    ####################################################################################################
    """
        02_01 原始區資料容量撈取
    """
    dataSizeMakeDateStrArr = []
    if makeDateStrArr != [] :
        makeDateStrSet = set()
        for makeDateStr in makeDateStrArr :
            makeDateStrSet.add(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d").strftime("%Y-%m-%d"))
            makeDateStrSet.add((datetime.datetime.strptime(makeDateStr, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
        dataSizeMakeDateStrArr = list(makeDateStrSet)
        dataSizeMakeDateStrArr.sort()
    dataSize_parametersData = {
        "makedate" : dataSizeMakeDateStrArr
        , "startdate": [startDateStr]
        , "enddate": [(datetime.datetime.strptime(endDateStr, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d")]
        , "gamename" : [gameName]
        , "dbname" : [hdfsDBName]
    }
    if runDataSize == True :
        dataSizeCtrl.main(dataSize_parametersData)
    ####################################################################################################
    """
        02_02 原始區資料錯誤檢查
    """
    dataCheckMakeDateStrArr = []
    if makeDateStrArr != []:
        makeDateStrSet = set()
        for makeDateStr in makeDateStrArr:
            makeDateStrSet.add(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d").strftime("%Y-%m-%d"))
            makeDateStrSet.add((datetime.datetime.strptime(makeDateStr, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
        dataCheckMakeDateStrArr = list(makeDateStrSet)
        dataCheckMakeDateStrArr.sort()
    dataCheck_parametersData = {
        "makedate": dataCheckMakeDateStrArr
        , "startdate": [startDateStr]
        , "enddate": [(datetime.datetime.strptime(endDateStr, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d")]
        , "gamename" : [gameName]
        , "dbname" : [hdfsDBName]
    }
    if runDataCheck == True:
        dataCheckCtrl.main(dataCheck_parametersData)
    ####################################################################################################
    """
        02_04 原始區鏡像檔案製作
    """
    restoreMakeSHDateStrArr = []
    if makeDateStrArr != []:
        makeDateStrSet = set()
        for makeDateStr in makeDateStrArr:
            makeDateStrSet.add(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d").strftime("%Y-%m-%d"))
            makeDateStrSet.add((datetime.datetime.strptime(makeDateStr, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
        restoreMakeSHDateStrArr = list(makeDateStrSet)
        restoreMakeSHDateStrArr.sort()
    restoreMakeSH_parametersData = {
        "makedate": restoreMakeSHDateStrArr
        , "startdate": [startDateStr]
        , "enddate": [(datetime.datetime.strptime(endDateStr, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d")]
        , "gamename" : [gameName]
        , "dbname" : [hdfsDBName]
    }
    if runRestoreMakeSH == True:
        restoreMakeSHCtel.main(restoreMakeSH_parametersData)
    ####################################################################################################
    """
        02_05 原始區資料鏡像製作
    """
    restoreMakeDateStrArr = []
    if makeDateStrArr != []:
        makeDateStrSet = set()
        for makeDateStr in makeDateStrArr:
            makeDateStrSet.add(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d").strftime("%Y-%m-%d"))
            makeDateStrSet.add((datetime.datetime.strptime(makeDateStr, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
        restoreMakeDateStrArr = list(makeDateStrSet)
        restoreMakeDateStrArr.sort()
    restoreMake_parametersData = {
        "makedate": restoreMakeDateStrArr
        , "startdate": [startDateStr]
        , "enddate": [(datetime.datetime.strptime(endDateStr, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d")]
    }
    if runRestoreMakeData == True:
        restoreMakeDataCtrl.main(restoreMake_parametersData)
    ####################################################################################################
    """
        02_10 Partition製作
    """
    partitionMakeDateStrArr = []
    if makeDateStrArr != []:
        makeDateStrSet = set()
        for makeDateStr in makeDateStrArr:
            makeDateStrSet.add(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d").strftime("%Y-%m-%d"))
            makeDateStrSet.add((datetime.datetime.strptime(makeDateStr, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
        partitionMakeDateStrArr = list(makeDateStrSet)
        partitionMakeDateStrArr.sort()
    partitionMake_parametersData = {
        "makedate": partitionMakeDateStrArr
        , "startdate": [startDateStr]
        , "enddate": [(datetime.datetime.strptime(endDateStr, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d")]
        , "gamename" : [gameName]
        , "dbname" : [hiveDBName]
        , "runall": [partitionRunAll]
        , "runrestore": [partitionRunRestore]
    }
    if runMakeTableAndPartition == True:
        makeTableAndPartitionCtrl.main(partitionMake_parametersData)
    ####################################################################################################
    """
        11_00 每日登入製作
    """
    makeInitMakeDateStrArr = []
    if makeDateStrArr != []:
        makeDateStrSet = set()
        for makeDateStr in makeDateStrArr:
            makeDateStrSet.add((datetime.datetime.strptime(makeDateStr, "%Y-%m-%d") - datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
            makeDateStrSet.add(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d").strftime("%Y-%m-%d"))
        makeInitMakeDateStrArr = list(makeDateStrSet)
        makeInitMakeDateStrArr.sort()
    else :
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d") - datetime.timedelta(days=1)
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeInitMakeDateStrArr.append(makeDatetime.strftime("%Y-%m-%d"))
            makeDatetime = makeDatetime + datetime.timedelta(days=1)
    makeInit_parametersData = {
        "makedate": makeInitMakeDateStrArr
    }
    if runMakeInitExtract == True:
        makeInitExtractCtrl.main(makeInit_parametersData)
    ####################################################################################################
    """
        02_10 中繼站資料製作
    """
    makeALLMakeDateStrArr = []
    if makeDateStrArr != []:
        makeDateStrSet = set()
        for makeDateStr in makeDateStrArr:
            makeDateStrSet.add((datetime.datetime.strptime(makeDateStr, "%Y-%m-%d") - datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
            makeDateStrSet.add(datetime.datetime.strptime(makeDateStr, "%Y-%m-%d").strftime("%Y-%m-%d"))
        makeALLMakeDateStrArr = list(makeDateStrSet)
        makeALLMakeDateStrArr.sort()
    else :
        startDateTime = datetime.datetime.strptime(startDateStr, "%Y-%m-%d") - datetime.timedelta(days=1)
        endDateTime = datetime.datetime.strptime(endDateStr, "%Y-%m-%d")
        makeDatetime = startDateTime
        while makeDatetime <= endDateTime:
            makeALLMakeDateStrArr.append(makeDatetime.strftime("%Y-%m-%d"))
            makeDatetime = makeDatetime + datetime.timedelta(days=1)
    makeALL_parametersData = {
        "makedate": makeALLMakeDateStrArr
    }
    if runMakeALL == True:
        makeALLMonthFirstCtrl.main(makeALL_parametersData)
    ####################################################################################################


if __name__ == "__main__" :
    main()