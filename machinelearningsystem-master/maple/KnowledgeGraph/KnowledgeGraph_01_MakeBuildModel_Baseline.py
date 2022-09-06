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
        , "project": ["KnowledgeGraph"]
        , "rawdataversion": ["R0_0_1"]
        , "preprocessversion": ["P0_0_1"]
        , "usemodelversion": ["M0_0_1"]
        , "modelversion": ["V0_0_0"]
        , "runstep": ["rawdata", "preprocess", "usemodel"]
    })
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-06-09"]
    #     , "productname": ["maple"]
    #     , "project": ["KnowledgeGraph"]
    #     , "rawdataversion": ["R3_0_1"]
    #     , "preprocessversion": ["P3_0_1"]
    #     , "usemodelversion": ["M3_0_1"]
    #     , "modelversion": ["V3_0_0"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })

    # Modeling
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-06-09"]
    #     , "productname": ["maple"]
    #     , "project": ["KnowledgeGraph"]
    #     , "rawdataversion": ["R2_0_1"]
    #     , "preprocessversion": ["P2_0_1"]
    #     , "usemodelversion": ["M2_0_1"]
    #     , "modelversion": ["V2_0_1"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })
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



