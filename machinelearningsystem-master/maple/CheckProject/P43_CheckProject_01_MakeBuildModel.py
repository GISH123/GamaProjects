import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling

# 該程式主要為基本建立模型的檔案

if __name__ == "__main__":
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2019-12-01"]
    #     , "productname": ["maple"]
    #     , "project": ["CheckProject"]
    #     , "preprocessversion": ["P1_0_4"]
    #     , "runstep": ["preprocess"]
    #     , "modelversion": ["V1_0_0"]
    # })
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2019-12-01"]
    #     , "productname": ["maple"]
    #     , "project": ["CheckProject"]
    #     , "preprocessversion": ["P1_0_5"]
    #     , "runstep": ["preprocess"]
    #     , "modelversion": ["V2_0_0"]
    # })

    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["peiyuwu"]
        , "makedate": ["2022-06-18"]
        , "productname": ["maple"]
        , "project": ["CheckProject"]
        , "rawdataversion": ["R1_0_7"]
        , "preprocessversion": ["P1_0_7"]
        , "usemodelversion": ["M1_0_7"]
        , "runstep": ["rawdata", "preprocess", "usemodel"]
        , "modelversion": ["V1_0_7"]
    })

    #
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



