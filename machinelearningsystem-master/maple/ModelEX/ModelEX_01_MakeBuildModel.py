import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling

# 該程式主要為基本建立模型的檔案

if __name__ == "__main__":
    # Make Data
    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["peiyuwu"]
        , "makedate": ["2022-06-09"]
        , "productname": ["maple"]
        , "project": ["ModelEX"]
        , "rawdataversion": ["R1_0_1"]
        , "preprocessversion": ["P1_0_1"]
        , "usemodelversion": ["M1_0_1"]
        , "modelversion": ["V1_0_0"]
        , "runstep": ["rawdata", "preprocess", "usemodel"]
    })

    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["vicying"]
    #     , "makedate": ["2022-01-01"]
    #     , "productname": ["ExampleProduct"]
    #     , "project": ["CheckProject"]
    #     , "rawdataversion": ["R1_0_3"]
    #     , "preprocessversion": ["P1_0_3"]
    #     , "usemodelversion": ["M1_0_3"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })
    #
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["vicying"]
    #     , "makedate": ["2022-01-01"]
    #     , "productname": ["ExampleProduct"]
    #     , "project": ["CheckProject"]
    #     , "rawdataversion": ["R1_0_4"]
    #     , "preprocessversion": ["P1_0_4"]
    #     , "usemodelversion": ["M1_0_4"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })



