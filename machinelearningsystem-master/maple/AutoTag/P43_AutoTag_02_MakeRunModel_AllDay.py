import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling
import datetime

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
            ],
        }
    }

    makeDatetime = datetime.datetime.strptime('2021-01-01', "%Y-%m-%d")
    while makeDatetime <= datetime.datetime.strptime('2022-08-24', "%Y-%m-%d"):
        while True:
            try:
                makeModeling.main({
                    "runtype": ["runmodel"]  # 只能輸入 buildmodel 或 runmodel , 只會取第一個值
                    , "builduser": ["peiyuwu"]  # 只能輸入作者名 , 只會取第一個值
                    , "makedate": [makeDatetime.strftime("%Y-%m-%d")]  # 只能輸入日期格式為YYYY-MM-DD , 取多個值
                    , "productname": ["maple"]  # 只能輸入產品名 , 只會取第一個值
                    , "project": ["AutoTag"]  # 只能輸入計畫名 , 只會取第一個值
                    , "modelversion": ["V0_11001_1"]  # 只能輸入格式為V0_0_1 , 在 runtype 為 buildmodel 取單一值 , 在 runtype 為 runmodel 取多個值
                    , "parameter": modelParameter  # print(makeInfo["parameter"])
                })

                makeModeling.main({
                    "runtype": ["runmodel"]  # 只能輸入 buildmodel 或 runmodel , 只會取第一個值
                    , "builduser": ["peiyuwu"]  # 只能輸入作者名 , 只會取第一個值
                    , "makedate": [makeDatetime.strftime("%Y-%m-%d")]  # 只能輸入日期格式為YYYY-MM-DD , 取多個值
                    , "productname": ["maple"]  # 只能輸入產品名 , 只會取第一個值
                    , "project": ["AutoTag"]  # 只能輸入計畫名 , 只會取第一個值
                    , "modelversion": ["V0_4001_0"]  # 只能輸入格式為V0_0_1 , 在 runtype 為 buildmodel 取單一值 , 在 runtype 為 runmodel 取多個值
                    , "parameter": modelParameter  # print(makeInfo["parameter"])
                })

                makeModeling.main({
                    "runtype": ["runmodel"]  # 只能輸入 buildmodel 或 runmodel , 只會取第一個值
                    , "builduser": ["peiyuwu"]  # 只能輸入作者名 , 只會取第一個值
                    , "makedate": [makeDatetime.strftime("%Y-%m-%d")]  # 只能輸入日期格式為YYYY-MM-DD , 取多個值
                    , "productname": ["maple"]  # 只能輸入產品名 , 只會取第一個值
                    , "project": ["AutoTag"]  # 只能輸入計畫名 , 只會取第一個值
                    , "modelversion": ["V0_4001_1"]  # 只能輸入格式為V0_0_1 , 在 runtype 為 buildmodel 取單一值 , 在 runtype 為 runmodel 取多個值
                    , "parameter": modelParameter  # print(makeInfo["parameter"])
                })
                #
                # makeModeling.main({
                #     "runtype": ["runmodel"]  # 只能輸入 buildmodel 或 runmodel , 只會取第一個值
                #     , "builduser": ["vicying"]  # 只能輸入作者名 , 只會取第一個值
                #     , "makedate": [makeDatetime.strftime("%Y-%m-%d")]  # 只能輸入日期格式為YYYY-MM-DD , 取多個值
                #     , "productname": ["maple"]  # 只能輸入產品名 , 只會取第一個值
                #     , "project": ["AutoTag"]  # 只能輸入計畫名 , 只會取第一個值
                #     , "modelversion": ["V0_9001_1"]  # 只能輸入格式為V0_0_1 , 在 runtype 為 buildmodel 取單一值 , 在 runtype 為 runmodel 取多個值
                #     , "parameter": modelParameter  # print(makeInfo["parameter"])
                # })
                break
            except Exception as e:
                continue

        makeDatetime += datetime.timedelta(days=1)