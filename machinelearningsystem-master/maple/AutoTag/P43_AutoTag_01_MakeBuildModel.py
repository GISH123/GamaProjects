import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None

import common.P43_01_MakeModelingCommon as makeModeling

if __name__ == "__main__":
    modelParameter = {
        "ProjectMemo": ""
        , "PreProcess": {
            "sqlReplaceArr": [
                ["[:TagDataPreProcessVersion]", "P0_1_1"]
                , ["[:TagTextPreProcessVersion]", "P0_1_2"]
                , ["[:InnerDataPreProcessVersion]", "P0_1_3"]
                , ["[:TagOddsPreProcessVersion]", "P0_1_4"]
                , ["[:MarkTagPreProcessVersion]", "P0_1_5"]
            ]
        }
    }

    '''Vic - Code'''
    '''modelParameter["VersionMemo"] = "AutoTag 資料整理相關"
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["vicying"]
    #     , "makedate": ["2022-01-01"]
    #     , "productname": ["maple"]
    #     , "project": ["AutoTag"]
    #     , "modelversion": ["V0_0_1"]
    #     , "rawdataversion": ["R0_1_1"]
    #     , "preprocessversion": ["P0_1_1"]
    #     , "runstep": ["rawdata", "preprocess"]
    #     , "parameter": modelParameter #print(makeInfo["parameter"]) 使用makeInfo["parameter"] 撈取參數
    # })

    modelParameter["VersionMemo"] = "AutoTag 文本資料塞入"
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["vicying"]
    #     , "makedate": ["2022-01-01"]
    #     , "productname": ["maple"]
    #     , "project": ["AutoTag"]
    #     , "modelversion": ["V0_0_2"]
    #     , "preprocessversion": ["P0_1_2"]
    #     , "runstep": ["preprocess"]
    #     , "parameter": modelParameter  # print(makeInfo["parameter"]) 使用makeInfo["parameter"] 撈取參數
    # })


    # modelParameter["VersionMemo"] = "AutoTag 資料與文本內積"
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["vicying"]
    #     , "makedate": ["2022-01-01"]
    #     , "productname": ["maple"]
    #     , "project": ["AutoTag"]
    #     , "modelversion": ["V0_0_3"]
    #     , "preprocessversion": ["P0_1_3"]
    #     , "runstep": ["preprocess"]
    #     , "parameter": modelParameter
    # })

    # modelParameter["VersionMemo"] = "AutoTag 資料與文本排續"
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["vicying"]
    #     , "makedate": ["2022-01-01"]
    #     , "productname": ["maple"]
    #     , "project": ["AutoTag"]
    #     , "modelversion": ["V0_0_4"]
    #     , "preprocessversion": ["P0_1_4"]
    #     , "runstep": ["preprocess"]
    #     , "parameter": modelParameter
    # })

    # modelParameter["VersionMemo"] = "AutoTag 資料與文本排續"
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["vicying"]
    #     , "makedate": ["2022-01-01"]
    #     , "productname": ["maple"]
    #     , "project": ["AutoTag"]
    #     , "modelversion": ["V0_0_5"]
    #     , "preprocessversion": ["P0_1_5"]
    #     , "usemodelversion": ["M0_1_5"]
    #     , "runstep": ["preprocess", "usemodel"]
    #     , "parameter": modelParameter
    # })'''

    '''wupeiyu - Code'''
    #
    # modelParameter["VersionMemo"] = "AutoTag 特定模組測試"
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["vicying"]
    #     , "makedate": ["2022-01-01"]
    #     , "productname": ["maple"]
    #     , "project": ["AutoTag"]
    #     , "modelversion": ["V0_0_97"]
    #     , "rawdataversion": ["R0_2_2"]
    #     , "preprocessversion": ["P0_2_2"]
    #     , "runstep": ["rawdata","preprocess"]
    #     , "parameter": modelParameter
    # })
    #
    # modelParameter["VersionMemo"] = "AutoTag 特定模組測試-Data2Tag-資料前處理"
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["vicying"]
    #     , "makedate": ["2022-01-01"]
    #     , "productname": ["maple"]
    #     , "project": ["AutoTag"]
    #     , "modelversion": ["V0_0_98"]
    #     , "rawdataversion": ["R0_2_3"]
    #     , "preprocessversion": ["P0_2_3"]
    #     # , "runstep": ["rawdata"]
    #     , "runstep": ["rawdata","preprocess"]
    #     , "parameter": modelParameter
    # })

    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["peiyuwu"]
        , "makedate": ["2022-01-01"]
        , "productname": ["maple"]
        , "project": ["AutoTag"]
        , "modelversion": ["V0_11001_1"]
        , "rawdataversion": ["R0_11001_1"]
        , "preprocessversion": ["P0_11001_1"]
        , "usemodelversion": ["M0_11001_99"]
        , "runstep": ["rawdata", "preprocess", "usemodel"]
        # , "parameter": modelParameter
    })

    modelParameter["VersionMemo"] = "AutoTag 特定模組測試-4000"
    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["peiyuwu"]
        , "makedate": ["2022-01-01"]
        , "productname": ["maple"]
        , "project": ["AutoTag"]
        , "modelversion": ["V0_4001_0"]
        , "rawdataversion": ["R0_4001_0"]
        , "runstep": ["rawdata"]
        # , "parameter": modelParameter
    })

    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["peiyuwu"]
        , "makedate": ["2022-01-01"]
        , "productname": ["maple"]
        , "project": ["AutoTag"]
        , "modelversion": ["V0_4001_1"]
        , "rawdataversion": ["R0_4001_1"]
        , "preprocessversion": ["P0_4001_1"]
        , "usemodelversion": ["M0_4001_99"]
        , "runstep": ["rawdata", "preprocess", "usemodel"]
        # , "parameter": modelParameter
    })
    # modelParameter["VersionMemo"] = "AutoTag 特定模組測試-外部資料Tag"
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-01-01"]
    #     , "productname": ["maple"]
    #     , "project": ["AutoTag"]
    #     , "modelversion": ["V0_9001_1"]
    #     , "rawdataversion": ["R0_9001_1"]
    #     , "preprocessversion": ["P0_9001_1"]
    #     , "usemodelversion": ["M0_9001_1"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    #     # , "parameter": modelParameter
    # })
