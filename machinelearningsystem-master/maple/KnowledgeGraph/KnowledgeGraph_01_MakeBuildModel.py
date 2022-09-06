import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling

# 該程式主要為基本建立模型的檔案

if __name__ == "__main__":
    # Make Data
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-06-09"]
    #     , "productname": ["maple"]
    #     , "project": ["KnowledgeGraph"]
    #     , "rawdataversion": ["R0_1_1"]
    #     , "preprocessversion": ["P0_1_1"]
    #     , "usemodelversion": ["M0_1_1"]
    #     , "modelversion": ["V0_1_1"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-06-09"]
    #     , "productname": ["maple"]
    #     , "project": ["KnowledgeGraph"]
    #     , "rawdataversion": ["R0_1_1"]
    #     , "preprocessversion": ["P0_1_2"]
    #     , "usemodelversion": ["M0_1_1"]
    #     , "modelversion": ["V0_1_2"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-06-09"]
    #     , "productname": ["maple"]
    #     , "project": ["KnowledgeGraph"]
    #     , "rawdataversion": ["R1_1_1"]
    #     , "preprocessversion": ["P1_1_1"]
    #     , "usemodelversion": ["M1_1_1"]
    #     , "modelversion": ["V1_1_1"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-06-09"]
    #     , "productname": ["maple"]
    #     , "project": ["KnowledgeGraph"]
    #     , "rawdataversion": ["R2_1_2"]
    #     , "preprocessversion": ["P2_1_2"]
    #     , "usemodelversion": ["M2_1_1"]
    #     , "modelversion": ["V2_1_2"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-06-09"]
    #     , "productname": ["maple"]
    #     , "project": ["KnowledgeGraph"]
    #     , "rawdataversion": ["R3_1_3"]
    #     , "preprocessversion": ["P3_1_3"]
    #     , "usemodelversion": ["M3_1_3"]
    #     , "modelversion": ["V3_1_3"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-06-09"]
    #     , "productname": ["maple"]
    #     , "project": ["KnowledgeGraph"]
    #     , "rawdataversion": ["R3_1_4"]
    #     , "preprocessversion": ["P3_1_4"]
    #     , "usemodelversion": ["M3_1_4"]
    #     , "modelversion": ["V3_1_4"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-06-09"]
    #     , "productname": ["maple"]
    #     , "project": ["KnowledgeGraph"]
    #     , "rawdataversion": ["R3_1_4"]
    #     , "preprocessversion": ["P3_1_5"]
    #     , "usemodelversion": ["M3_1_5"]
    #     , "modelversion": ["V3_1_5"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-06-09"]
    #     , "productname": ["maple"]
    #     , "project": ["KnowledgeGraph"]
    #     , "rawdataversion": ["R3_1_4"]
    #     , "preprocessversion": ["P3_1_6"]
    #     , "usemodelversion": ["M3_1_6"]
    #     , "modelversion": ["V3_1_6"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-06-09"]
    #     , "productname": ["maple"]
    #     , "project": ["KnowledgeGraph"]
    #     , "rawdataversion": ["R3_1_4"]
    #     , "preprocessversion": ["P3_1_6"]
    #     , "usemodelversion": ["M3_1_7"]
    #     , "modelversion": ["V3_1_7"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-06-09"]
    #     , "productname": ["maple"]
    #     , "project": ["KnowledgeGraph"]
    #     , "rawdataversion": ["R4_1_1"]
    #     , "preprocessversion": ["P4_1_1"]
    #     , "usemodelversion": ["M4_1_1"]
    #     , "modelversion": ["V4_1_1"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })
    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["peiyuwu"]
        , "makedate": ["2022-06-09"]
        , "productname": ["maple"]
        , "project": ["KnowledgeGraph"]
        , "rawdataversion": ["R3_1_5"]
        , "preprocessversion": ["P3_1_8"]
        , "usemodelversion": ["M3_1_8"]
        , "modelversion": ["V3_1_8"]
        , "runstep": ["rawdata", "preprocess", "usemodel"]
    })
    makeModeling.main({
        "runtype": ["buildmodel"]
        , "builduser": ["peiyuwu"]
        , "makedate": ["2022-06-09"]
        , "productname": ["maple"]
        , "project": ["KnowledgeGraph"]
        , "rawdataversion": ["R3_1_5"]
        , "preprocessversion": ["P3_1_8"]
        , "usemodelversion": ["M3_1_9"]
        , "modelversion": ["V3_1_9"]
        , "runstep": ["rawdata", "preprocess", "usemodel"]
    })
    # makeModeling.main({
    #     "runtype": ["buildmodel"]
    #     , "builduser": ["peiyuwu"]
    #     , "makedate": ["2022-06-09"]
    #     , "productname": ["maple"]
    #     , "project": ["KnowledgeGraph"]
    #     , "rawdataversion": ["R99_99_99"]
    #     , "preprocessversion": ["P99_99_99"]
    #     , "usemodelversion": ["M99_99_99"]
    #     , "modelversion": ["V99_99_99"]
    #     , "runstep": ["rawdata", "preprocess", "usemodel"]
    # })
    #
    #
