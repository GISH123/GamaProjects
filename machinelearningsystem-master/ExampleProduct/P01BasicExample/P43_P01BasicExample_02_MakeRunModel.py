import os ,sys ; os.chdir(os.path.dirname(__file__)) if os.name == "posix" else None
import common.P43_01_MakeModelingCommon as makeModeling

# 該程式主要為基本執行模型的檔案

if __name__ == "__main__":

    makeModeling.main({
        "runtype": ["runmodel"]                                         # 只能輸入 buildmodel 或 runmodel , 只會取第一個值
        , "builduser": ["vicying"]                                      # 只能輸入作者名 , 只會取第一個值
        , "makedate": ["2022-01-01"]                                    # 只能輸入日期格式為YYYY-MM-DD , 取多個值
        , "productname": ["ExampleProduct"]                             # 只能輸入產品名 , 只會取第一個值
        , "project": ["P01BasicExample"]                                # 只能輸入計畫名 , 只會取第一個值
        , "modelversion": ["V0_0_2"]                  # 只能輸入格式為V0_0_1 , 在 runtype 為 buildmodel 取單一值 , 在 runtype 為 runmodel 取多個值
        , "runstep": ["usemodel"]
        , "parameter": {
            'rawdata': {},
            'preprocess': {
                'makedate': [
                    # '2020-12-30',
                    '2021-01-14',

                ]
            },
            'usemodel': {}
        }
    })
