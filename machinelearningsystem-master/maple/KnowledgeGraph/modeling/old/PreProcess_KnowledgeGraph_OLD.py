from collections import defaultdict
import csv
import numpy as np
from tqdm import tqdm
import os
import pandas
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl
import pandas as pd

class PreProcess_KnowledgeGraph_OLD() :
    @classmethod
    def read_equipment_revised_ALL(self, Path):
        read_equ = pd.read_csv(Path)
        read_equ = read_equ.drop(['Unnamed: 0', 'KR Name', 'Unnamed: 5'], axis=1)
        read_equ = read_equ.fillna(0)

        read_equ['event'] = pd.to_datetime(read_equ['Date']).dt.strftime('%Y/%m/%d')
        read_equ['Date'] = pd.to_datetime(read_equ['Date']).dt.strftime('%Y-%m-%d')
        read_equ = read_equ.reset_index(drop=True)

        return read_equ

    @classmethod
    def fashion_revised_all(self, path):
        read_equ = pd.read_csv(path)

        return read_equ

    @classmethod
    def kg4FashionItemStyle(self, Path):
        read_equ = self.read_equipment_revised_ALL(Path='D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/2021_2022equipment_revised_ALL.csv')
        print(read_equ.columns)

        ###############################################################################################################
        cols = [
            'ItemID', '大師標籤', 'IP合作', '漂浮特效', '暗色系', '螢光系', '柔和色系', '可愛風格', '制服風格',
            '貴族風格', '動物風格', '怪物風格', '角色風格', '自然風格', '重裝風格', '運動風格', '搞怪風格',
            '不擋身/腿', '不擋住名字', '渲染特效', 'Date', 'event'
        ]
        meltcols = [
            '大師標籤', 'IP合作',
            '漂浮特效',
            '暗色系', '螢光系', '柔和色系',
            '可愛風格', '制服風格', '貴族風格', '動物風格', '怪物風格', '角色風格', '自然風格', '重裝風格', '運動風格', '搞怪風格',
            '不擋身/腿', '不擋住名字',
            '渲染特效'
        ]

        #### KG:ItemId-Relation-Concept
        itemWithConcept = read_equ[cols].reset_index(drop=True)
        itemWithConcept = pd.melt(itemWithConcept, id_vars=['ItemID'], value_vars=meltcols)
        itemWithConcept.columns = ['Entity1', 'Entity2', 'Relation']
        itemWithConcept['Relation'] = itemWithConcept['Relation'].astype(int).astype(str)
        itemWithConcept = itemWithConcept.loc[itemWithConcept['Relation'] == '1', :].reset_index(drop=True)
        itemWithConcept['Relation'] = '是'
        print(itemWithConcept.head())

        ### KG:FashionBox-Relation-ItemID
        fashionWithItem = read_equ[['Date', 'ItemID']].reset_index(drop=True)
        boxIndex1 = fashionWithItem['Date'] < '2021-09-08'
        boxIndex2 = fashionWithItem['Date'] >= '2021-09-08'
        fashionWithItem.columns = ['Entity1', 'Entity2']
        fashionWithItem.loc[boxIndex1, 'Entity1'] = '5222123_' + fashionWithItem.loc[boxIndex1, 'Entity1']
        fashionWithItem.loc[boxIndex2, 'Entity1'] = '5680946_' + fashionWithItem.loc[boxIndex2, 'Entity1']
        fashionWithItem['Relation'] = '產生'
        fashionWithItem = fashionWithItem[['Entity1', 'Relation', 'Entity2']]
        print(fashionWithItem.head())

        ## 輸出
        last_event = read_equ.tail(1).Date.to_list()[0]
        fashionKG = pd.concat([itemWithConcept, fashionWithItem]).reset_index(drop=True)
        # print(fashionKG.head())
        fashionKG.to_csv(
            f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{Path}/{''.join(last_event.split('-'))}_kgFashionBox.csv")

    @classmethod
    def kg4UserData(self, FromPath, ToPath):
        read_equ = self.read_equipment_revised_ALL(Path='D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/2021_2022equipment_revised_ALL.csv')
        dt_list = read_equ.Date.unique()

        # 歷史購買資料 + 持有資料
        for _ in tqdm(range(36, len(dt_list)-1)):
            st_dt = datetime.datetime.strptime(dt_list[_], "%Y-%m-%d")
            print(st_dt)

            # # 持有
            # kgUserIdItemId = pd.read_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{FromPath}/{st_dt.strftime('%Y%m%d')}_kgUserIdItemId.csv").drop('Unnamed: 0', axis=1)
            # kgUserIdItemId_np = kgUserIdItemId.to_numpy()
            #
            # data = []
            # for _ in kgUserIdItemId_np:
            #     id = _[0].replace("'", "")
            #     item_list = _[1].replace("'", "").split(',')
            #     for str_ in item_list:
            #         data.append([id, str_])
            #
            # kgUserIdItemId = pd.DataFrame(data)
            # kgUserIdItemId['Relation'] = '持有'
            # try:
            #     kgUserIdItemId.columns = ['Entity1','Entity2','Relation']
            # except:
            #     ...
            # kgUserIdItemId.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{ToPath}/{st_dt.strftime('%Y%m%d')}_kgUserIdItemId.csv")
            #
            # # 購買
            # kguseridpurchase = pd.read_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{FromPath}/{st_dt.strftime('%Y%m%d')}_kguseridpurchase.csv").drop('Unnamed: 0', axis=1)
            # kguseridpurchase['Relation'] = '購買'
            # try:
            #     kguseridpurchase.columns = ['Entity1','Entity2','num','Relation']
            # except:
            #     ...
            # kguseridpurchase.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{ToPath}/{st_dt.strftime('%Y%m%d')}_kguseridpurchase.csv")

            # 登入
            kgUserIdLogin = pd.read_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{FromPath}/{st_dt.strftime('%Y%m%d')}_kgUserIdLogin.csv").drop('Unnamed: 0', axis=1)
            kgUserIdLogin.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{ToPath}/{st_dt.strftime('%Y%m%d')}_kgUserIdLogin.csv")

        # 歷史檔期購買資料
        # dt_datetime_list = []
        # for dt in dt_list:
        #     ed_dt = datetime.datetime.strptime(dt, "%Y-%m-%d")
        #     dt_datetime_list.append(ed_dt)
        #
        # kgUserIdPurchaseFashionBox = pd.read_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{FromPath}/{dt_datetime_list[-1].strftime('%Y%m%d')}_kgUserIdPurchaseFashionBox.csv").drop('Unnamed: 0', axis=1)
        # data = []
        # kgUserIdPurchaseFashionBox_np = kgUserIdPurchaseFashionBox.to_numpy()
        # for _ in tqdm(kgUserIdPurchaseFashionBox_np):
        #     data.append(_)
        #
        #     dt = datetime.datetime.strptime(str(_[0]), "%Y%m%d")
        #     for event_ in dt_datetime_list:
        #         if event_ <= dt:
        #             data[-1][0] = f"{_[2]}_{event_.strftime('%Y-%m-%d')}"
        #         else:
        #             break
        #
        # data = np.array(data)
        # df = pd.DataFrame(data, columns=['event','serviceaccountid','itemid'])
        # df = df.groupby(['event','serviceaccountid'])['itemid'].count().reset_index()
        # df.columns = ['Entity2','Entity1','num']
        # df['Relation'] = '購買'
        # print(df)
        #
        # df.to_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{ToPath}/{dt_datetime_list[-1].strftime('%Y%m%d')}_kgUserIdPurchaseFashionBox.csv")


    # Item 代表向量-平均向量
    @classmethod
    def itemRepresent(self, similaryTopKArrs, word2VecModel):
        avgVecs = [[] for _ in similaryTopKArrs]
        for ind_, items_ in enumerate(similaryTopKArrs):
            avgVec = np.zeros_like(word2VecModel.wv[f'{items_[0]}'])
            for item_ in items_:
                avgVec += word2VecModel.wv[f'{item_}']
            avgVecs[ind_] = list(avgVec / len(items_))
        return avgVecs

    # 獲得最相似檔期平均向量
    @classmethod
    def getSimilarityAvgVec(self, kgModel, TargetItem, datetime_np, item_np, filiter_dt1, filiter_dt2, topk=2):
        # print(TargetItem, datetime_np, item_np, filiter_dt1, filiter_dt2)
        # print(item_np[(datetime_np >= filiter_dt1) & (datetime_np < filiter_dt2)])

        # 獲取模型字典
        ent_to_idx = kgModel.ent_to_idx
        get_ent_idxs_func_ = np.vectorize(ent_to_idx.get)
        ent_list = kgModel.trained_model_params[0]

        # 當今/歷史檔期向量
        entity_embedding_Item = [ent_list[get_ent_idxs_func_(TargetItem)]]
        item_vecs = []
        item_np = item_np[(datetime_np >= filiter_dt1) & (datetime_np < filiter_dt2)]
        for item_ in item_np:
            item_vecs.append(ent_list[get_ent_idxs_func_(item_)])

        # 檔期向量相似度(取topk=2)
        cosine_similarity_list = cosine_similarity(entity_embedding_Item, item_vecs)
        indCosineTuple = heapq.nlargest(topk, enumerate(cosine_similarity_list[0]), key=lambda x: x[1])

        # 當今檔期向量近似過去檔期向量
        similary_item = []
        entity_embedding_Item_avg = np.zeros_like(ent_list[get_ent_idxs_func_(TargetItem)])
        for ind_, cosine_ in indCosineTuple:
            similary_item.append(item_np[ind_])
            entity_embedding_Item_avg += ent_list[get_ent_idxs_func_(item_np[ind_])]
        entity_embedding_Item_avg /= topk

        # print(f'The Best Similarity Item of {TargetItem}:{similary_item}')
        return entity_embedding_Item_avg, similary_item


    @classmethod
    def MakePreProcess_KnowledgeGraph_P0_0_1(self, makeInfo):
        # return "MakePreProcessFreeFuction", None
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )
        sql_str = """  
                INSERT OVERWRITE TABLE gtwpd.modelextract_modelextract PARTITION(game='maple', dt='20220629',world='COMMON',tablenumber='16092')
                SELECT 
                    null as CommonData_1
                    , null as CommonData_2
                    , null as CommonData_3
                    , null as CommonData_4
                    , null as CommonData_5
                    , null as CommonData_6 
                    , 'v242' as CommonData_7
                    , commondata_008 as CommonData_8 
                    , commondata_009 as CommonData_9
                    , null as CommonData_10
                    , null as CommonData_11
                    , null as CommonData_12
                    , null as CommonData_13
                    , null as CommonData_14
                    , null as CommonData_15
                    , null as UniqueInt_1
                    , null as UniqueInt_2
                    , null as UniqueInt_3
                    , null as UniqueInt_4
                    , null as UniqueInt_5
                    , null as UniqueInt_6
                    , null as UniqueInt_7
                    , null as UniqueInt_8
                    , null as UniqueInt_9 
                    , null as UniqueInt_10
                    , null as UniqueInt_11
                    , null as UniqueInt_12
                    , null as UniqueInt_13
                    , null as UniqueInt_14
                    , null as UniqueInt_15
                    , CAST(uniquefloat_001 as string) as UniqueStr_1
                    , CAST(uniquefloat_002 as string) as UniqueStr_2
                    , CAST(uniquefloat_003 as string) as UniqueStr_3
                    , CAST(uniquefloat_004 as string) as UniqueStr_4
                    , CAST(uniquefloat_005 as string) as UniqueStr_5
                    , CAST(uniquefloat_006 as string) as UniqueStr_6
                    , CAST(uniquefloat_007 as string) as UniqueStr_7
                    , CAST(uniquefloat_008 as string) as UniqueStr_8
                    , CAST(uniquefloat_009 as string) as UniqueStr_9
                    , CAST(uniquefloat_010 as string) as UniqueStr_10
                    , CAST(uniquefloat_011 as string) as UniqueStr_11 
                    , CAST(uniquefloat_012 as string) as uniquestr_12
                    , CAST(uniquefloat_013 as string) as uniquestr_13
                    , CAST(uniquefloat_014 as string) as uniquestr_14
                    , CAST(uniquefloat_015 as string) as uniquestr_15
                    , CAST(uniquefloat_016 as string) as uniquestr_16
                    , CAST(uniquefloat_017 as string) as uniquestr_17
                    , CAST(uniquefloat_018 as string) as uniquestr_18
                    , CAST(uniquefloat_019 as string) as uniquestr_19
                    , null as uniquestr_20
                    , uniquefloat_020 as uniquedbl_1
                    , null as uniquedbl_2
                    , null as uniquedbl_3
                    , null as uniquedbl_4
                    , null as uniquedbl_5
                    , null as uniquedbl_6
                    , null as uniquedbl_7
                    , null as uniquedbl_8
                    , null as uniquedbl_9
                    , null as uniquedbl_10
                    , null as uniquedbl_11
                    , null as uniquedbl_12
                    , null as uniquedbl_13
                    , null as uniquedbl_14
                    , null as uniquedbl_15
                    , null as uniquedbl_16
                    , null as uniquedbl_17
                    , null as uniquedbl_18
                    , null as uniquedbl_19
                    , null as uniquedbl_20
                    , CAST(commondata_010 as timestamp) as uniquetime_1
                    , CAST(commondata_011 as timestamp) as uniquetime_2
                    , null as uniquetime_3
                    , commondata_012 as otherstr_1
                    , commondata_013 as otherstr_2
                    , null as otherstr_3
                    , null as otherstr_4
                    , null as otherstr_5
                    , null as otherstr_6
                    , null as otherstr_7
                    , null as otherstr_8
                    , null as otherstr_9
                    , null as otherstr_10
                    , array(null) as uniquearray_1
                    , array(null) as uniquearray_2
                    , null as uniquejson_1
                FROM gtwpd.model_usedata 
                WHERE 1=1
                    AND product = 'maple'
                    AND project='KnowledgeGraph'
                    AND step = 'RawData'
                    AND version='R0_0_1'
                """
        print(sql_str)
        hiveCtrl.executeSQL(sql_str)
        return "MakePreProcessFreeFuction", None

    @classmethod
    def MakePreProcess_KnowledgeGraph_P2_0_1(self, makeInfo):
        print('Do MakePreProcess_KnowledgeGraph_P2_0_1 ...')
        return "MakePreProcessFreeFuction", None

        self.kg4FashionItemStyle('P2_0_1')

        self.kg4UserData(FromPath='R2_0_1', ToPath='P2_0_1')

        return "MakePreProcessFreeFuction", None

    @classmethod
    def MakePreProcess_KnowledgeGraph_P3_0_1(self, makeInfo):
        return "MakeUseModelFreeFuction", None
        print('do MakePreProcess_KnowledgeGraph_P3_0_1...')
        print('read read_equipment_revised_ALL...')
        read_equ = self.read_equipment_revised_ALL(
            Path='D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/2021_2022equipment_revised_ALL.csv')
        dt_list = [_ for _ in read_equ.Date.unique()]
        maindf = pd.read_csv(
            F"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M2_0_1/{dt_list[-1]}_kgModelData.csv"
            # , nrows=500000
        ).drop('Unnamed: 0', axis=1)
        print(maindf.loc[maindf['serviceaccountId'] == 'a08006969'])

        # 黏貼y值
        print('get Y value...')
        data = []
        maindf[['FashionBox', 'Date']] = maindf['FashionBoxEvent'].str.split('_', expand=True)
        maindf_dict = maindf[['Date', 'serviceaccountId', 'num']].set_index(['Date', 'serviceaccountId']).to_dict(
            'index')
        dt_list = [_ for _ in maindf.Date.unique()]
        for key_, val_ in tqdm(maindf_dict.items()):
            data_dt = key_[0]
            userId = key_[1]

            dt_ind_ = dt_list.index(data_dt)
            future_ind_ = dt_ind_ + 1
            if dt_ind_ != len(dt_list) - 1:
                future_dt = dt_list[future_ind_]
            else:
                future_dt = '9999-99-99'

            if (future_dt, userId) not in maindf_dict:
                maindf_dict[(data_dt, userId)]['Y'] = 0
            else:
                maindf_dict[(data_dt, userId)]['Y'] = maindf_dict[(future_dt, userId)]['num']

        df = pd.DataFrame.from_dict(maindf_dict, orient='index', columns=['num', 'Y']).reset_index()
        df.columns = ['Date', 'serviceaccountId', 'num', 'Y']
        boxIndex1 = df['Date'] < '2021-09-08'
        boxIndex2 = df['Date'] >= '2021-09-08'
        df.loc[boxIndex1, 'Date'] = '5222123_' + df.loc[boxIndex1, 'Date']
        df.loc[boxIndex2, 'Date'] = '5680946_' + df.loc[boxIndex2, 'Date']
        maindf = pd.merge(
            maindf,
            df[['Date', 'serviceaccountId', 'Y']].rename(columns={'Date': 'FashionBoxEvent'}),
            on=['FashionBoxEvent', 'serviceaccountId'],
            how='left'
        ).drop(['FashionBox', 'Date'], axis=1)
        del df, maindf_dict
        print(maindf.loc[maindf['serviceaccountId'] == 'a08006969'])

        print('sum score value by event...')
        for col_ in maindf.columns:
            if col_ not in ['FashionBoxEvent', 'serviceaccountId']:
                maindf[col_] = maindf[col_].astype('float32')

        print(maindf.dtypes)
        event_score = maindf.drop(['serviceaccountId'], axis=1).groupby('FashionBoxEvent').sum()
        print(event_score)

        # 疊代資料
        print('maindf itering...')
        maindf_index_np = maindf[['FashionBoxEvent', 'serviceaccountId']].to_numpy()
        maindf_data_np = maindf.set_index(['FashionBoxEvent', 'serviceaccountId']).to_numpy()
        user_data_score_list = defaultdict(lambda: np.zeros(maindf_data_np.shape[1]))
        data = []
        for ind_ in tqdm(range(maindf_data_np.shape[0])):
            serviceaccountid = maindf_index_np[ind_, 1]
            maindf_data_np[ind_, :-1] = maindf_data_np[ind_, :-1] + user_data_score_list[serviceaccountid][:-1]
            user_data_score_list[serviceaccountid] = maindf_data_np[ind_]
        del user_data_score_list

        FashionBoxEventList = event_score.index.tolist()
        FashionBoxEventNumpy = event_score.to_numpy()
        for ind_ in tqdm(range(1, len(FashionBoxEventList))):
            FashionBoxEventNumpy[ind_, :-1] += FashionBoxEventNumpy[ind_ - 1, :-1]
        for ind_ in tqdm(range(maindf_data_np.shape[0])):
            event = maindf_index_np[ind_, 0]
            dt_ind_ = FashionBoxEventList.index(event)
            # if maindf_index_np[ind_, 1] == 'a08006969' :print(maindf_data_np[ind_])
            # print(maindf_data_np[ind_])
            # print(np.sum(maindf_data_np[ind_]))
            maindf_data_np[ind_] = maindf_data_np[ind_] / FashionBoxEventNumpy[dt_ind_]
            # if maindf_index_np[ind_, 1] == 'a08006969' :print(maindf_data_np[ind_])

        fileName = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_0_1/{dt_list[-1]}_kgModelData.csv"
        with open(fileName, 'w', encoding='utf-8', newline='') as csvfile:
            writer = csv.writer(csvfile)
            outputdata = np.concatenate((maindf_index_np, maindf_data_np), axis=1)

            # write a row to the csv file
            writer.writerow(maindf.columns)
            for _ in outputdata:
                writer.writerow(_)

        return "MakeUseModelFreeFuction", None

    # 檔期資料圖譜化
    @classmethod
    def MakePreProcess_KnowledgeGraph_P0_1_1(self, makeInfo):
        # return "MakePreProcessFreeFuction", None

        hiveCtrl = self.getHiveCtrl()
        df = self.getFashionBoxBUInfo(hiveCtrl)
        meltcols = [
            '大師標籤', 'IP合作'
            , '漂浮特效', '暗色系', '螢光系', '柔和色系', '可愛風格'
            , '制服風格', '貴族風格', '動物風格', '怪物風格', '角色風格'
            , '自然風格', '重裝風格', '運動風格', '搞怪風格', '不擋身/腿'
            , '不擋住名字', '渲染特效'
        ]

        df['客觀概述'] = df['客觀概述'].apply(lambda x: x.split('、'))
        eventWithoutward = df[['ItemID','客觀概述']].explode(['客觀概述'])
        eventWithoutward = eventWithoutward.loc[eventWithoutward['客觀概述'] != ''].reset_index(drop=True)
        eventWithConcept = pd.melt(df, id_vars=['Date1'], value_vars=meltcols)
        ItemWithConcept = pd.melt(df, id_vars=['ItemID'], value_vars=meltcols)
        eventWithItem = df.loc[:, ["Date1", "ItemID", "Prob."]].reset_index(drop=True)

        eventWithoutward.columns = ['entity1', 'entity2']
        eventWithConcept.columns = ['entity1', 'entity2', 'num']
        ItemWithConcept.columns = ['entity1', 'entity2', 'num']
        eventWithItem.columns = ['entity1', 'entity2', 'num']

        eventWithoutward['relation'] = '外觀'
        eventWithoutward['num'] = 1.0
        eventWithConcept['relation'] = '是'
        ItemWithConcept['relation'] = '是'
        eventWithItem['relation'] = '產生'

        eventWithConcept = eventWithConcept.loc[eventWithConcept.num != 0].reset_index(drop=True)
        ItemWithConcept = ItemWithConcept.loc[ItemWithConcept.num != 0].reset_index(drop=True)

        boxIndex1 = eventWithConcept['entity1'] < '2021-09-08'
        boxIndex2 = eventWithConcept['entity1'] >= '2021-09-08'
        eventWithConcept['entity1'] = pd.to_datetime(eventWithConcept['entity1']).dt.strftime('%Y-%m-%d')
        eventWithConcept.loc[boxIndex1, 'entity1'] = '5222123_' + eventWithConcept.loc[boxIndex1, 'entity1']
        eventWithConcept.loc[boxIndex2, 'entity1'] = '5680946_' + eventWithConcept.loc[boxIndex2, 'entity1']

        boxIndex1 = eventWithItem['entity1'] < '2021-09-08'
        boxIndex2 = eventWithItem['entity1'] >= '2021-09-08'
        eventWithItem['entity1'] = pd.to_datetime(eventWithItem['entity1']).dt.strftime('%Y-%m-%d')
        eventWithItem.loc[boxIndex1, 'entity1'] = '5222123_' + eventWithItem.loc[boxIndex1, 'entity1']
        eventWithItem.loc[boxIndex2, 'entity1'] = '5680946_' + eventWithItem.loc[boxIndex2, 'entity1']

        kgGraph = pd.concat([eventWithConcept, ItemWithConcept, eventWithItem, eventWithoutward])

        upload_df = self.getUploadDataFrame()
        upload_df['commondata_001'] = kgGraph['entity1']
        upload_df['commondata_002'] = kgGraph['relation']
        upload_df['commondata_003'] = kgGraph['entity2']
        upload_df['uniquefloat_001'] = kgGraph['num']

        return "MakePreProcessFileInsertOverwrite", upload_df

    # 產生新的圖譜知識
    @classmethod
    def MakePreProcess_KnowledgeGraph_P2_1_1(self, makeInfo):
        inputFilePath = 'D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_1_1'
        outputFilePath = 'D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P2_1_1'
        hiveCtrl = self.getHiveCtrl()

        for makedate_ in makeInfo['parameter']['preprocess']['makedate']:
            eventStartDate, eventEndDate = self.getLastOverEventRange(makedate_, hiveCtrl)
            print(eventStartDate)

            df = pd.read_csv(inputFilePath + f"/KGRDFN_eventBuydf_{eventStartDate.strftime('%Y%m%d')}.csv").drop('Unnamed: 0', axis=1)
            df.columns = ['entity1','relation','entity2','num']

            df1 = pd.read_csv(inputFilePath + f"/KGRDFN_maindf_{eventStartDate.strftime('%Y%m%d')}.csv").drop('Unnamed: 0', axis=1)
            df1.columns = ['entity1','relation','entity2','num']

            df2 = pd.read_csv(inputFilePath + f"/KGRDFN_eventTagdf_{eventStartDate.strftime('%Y%m%d')}.csv").drop('Unnamed: 0', axis=1)
            df2.columns = ['entity1','relation','entity2','num']

            userid_list = df1.loc[df1.relation == '持有', 'entity1'].unique()
            # relation_list = ['持有', '是']
            relation_list = ['購買', '是']

            # leaf_node
            data_np = pd.concat([df, df1, df2]).to_numpy()
            graph = defaultdict(lambda :defaultdict(list))
            for _ in data_np:
                entity1 = str(_[0])
                relation = str(_[1])
                entity2 = str(_[2])
                num = float(_[3])
                graph[entity1][relation].append((entity2, num))

            for user_ in userid_list:
                num_list = self.getRootLeafEdge(graph = graph, node = user_, edge_num = 1, edge_list = relation_list, num_list = [])
                tmp_ = defaultdict(lambda :0)
                for tag_, num in num_list:
                    tmp_[tag_] += num
                for tag_, num in tmp_.items():
                    graph[user_]['喜歡'].append((tag_, num))

            with open(outputFilePath + f"/KGRDFN_traindf_{eventStartDate.strftime('%Y%m%d')}.csv", 'w',
                    encoding='utf-8', newline='') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=['entity1', 'relation', 'entity2', 'num'])
                writer.writeheader()

                for entity1_ in graph.keys():
                    for relation_ in graph[entity1_].keys():
                        # if relation == '購買': continue
                        for entity2_, num_ in graph[entity1_][relation_]:
                            writer.writerow({
                                'entity1': entity1_,
                                'relation': relation_,
                                'entity2': entity2_,
                                'num': num_
                            })
        return "MakePreProcessFreeFuction", None

    @classmethod
    def MakePreProcess_KnowledgeGraph_P3_1_2(self, makeInfo):
        return "MakePreProcessFreeFuction", None
        from ampligraph.utils import save_model, restore_model

        model_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M2_1_1/"
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R3_1_2/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_2/"

        df = pd.DataFrame()
        hiveCtrl = self.getHiveCtrl()
        print(item_list)
        # return "MakePreProcessFreeFuction", None

        for makedate_ in makeInfo['parameter']['preprocess']['modeldate']:
            eventDate1, eventDate2, eventDate3 = self.getLast2OverEventRange(makedate_, hiveCtrl)
            print(eventDate1, eventDate2, eventDate3)

            kgModel = restore_model(model_name_path = model_path + f"{eventDate1.strftime('%Y%m%d')}_ComplExModel_KG_FocusE.pkl")
            ent_to_idx = kgModel.ent_to_idx
            get_ent_idxs_func_ = np.vectorize(ent_to_idx.get)
            ent_list = kgModel.trained_model_params[0]  # Entity Embedding
            rel_to_idx = kgModel.rel_to_idx
            get_rel_idxs_func_ = np.vectorize(rel_to_idx.get)
            rel_list = kgModel.trained_model_params[1]  # Relation Embedding

            dataDF = pd.read_csv(input_path + f"traindf_{eventDate3.strftime('%Y%m%d')}.csv").fillna(0).drop('Unnamed: 0', axis=1)
            dataDF1 = dataDF[['service_account_id', 'num_train']]
            dataDF2 = dataDF[['service_account_id', 'num_valid']]
            dataDF1.columns = ['service_account_id', 'num']
            dataDF2.columns = ['service_account_id', 'num']
            dataDF1['data_set'] = eventDate2.strftime('%Y-%m-%d')
            dataDF2['data_set'] = eventDate3.strftime('%Y-%m-%d')
            df = pd.concat([dataDF1, dataDF2])
            data_np = df.to_numpy()
            data = []
            for _ in tqdm(data_np):
                id_ = _[0]
                num_ = _[1]
                data_set = _[2]

                if datetime.datetime.strptime(data_set, "%Y-%m-%d") < datetime.datetime.strptime('2021-09-08', "%Y-%m-%d"):
                    item_ = f'5222123_{data_set}'
                else:
                    item_ = f'5680946_{data_set}'

                if id_ in ent_to_idx.keys():
                    entity_embedding_UserID = ent_list[get_ent_idxs_func_(id_)]
                else:
                    entity_embedding_UserID = [0 for _ in range(len(ent_list[get_ent_idxs_func_(item_)]))]
                entity_embedding_Item = ent_list[get_ent_idxs_func_(item_)]

                data.append([id_, num_, data_set])
                data[-1].extend(entity_embedding_UserID)
                data[-1].extend(entity_embedding_Item)
                for ind_ in range(len(entity_embedding_Item)):
                    data[-1].append(entity_embedding_UserID[ind_] * entity_embedding_Item[ind_])

            df = pd.DataFrame(data)
            vector_cols = [f'col_{_}' for _ in range(df.shape[1] - 3)]
            cols = ['id', 'Y', 'data_set'] + vector_cols
            df.columns = cols
            df.to_csv(output_path + f"xgbTraindf_{eventDate3.strftime('%Y%m%d')}.csv")

        return "MakePreProcessFreeFuction", None


    # 預測上個檔期有出現的人
    @classmethod
    def MakePreProcess_KnowledgeGraph_P3_1_1(self, makeInfo):
        # return "MakePreProcessFreeFuction", None
        from ampligraph.utils import save_model, restore_model

        model_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M2_1_1/"
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R3_1_1/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_1/"
        hiveCtrl = RawPreModel.getHiveCtrl()

        # 所有資料
        print('read traindf...')
        df = pd.DataFrame()
        for trainDate_ in tqdm(makeInfo['parameter']['preprocess']['trainDate']):
            dataDF = pd.read_csv(input_path + f"traindf_{trainDate_.replace('-', '')}.csv")\
                .fillna(0).drop('Unnamed: 0', axis=1)
            df = pd.concat([df, dataDF])

        # 建XGB資料
        datetime_np, item_np = self.getEventItemList(hiveCtrl)
        validDate_list = makeInfo['parameter']['preprocess']['validDate']
        for ind_ in range(1, len(validDate_list)):
            trainDate = datetime.datetime.strptime(validDate_list[ind_ - 1], '%Y-%m-%d')
            validDate = datetime.datetime.strptime(validDate_list[ind_], '%Y-%m-%d')
            print(trainDate, validDate)

            # 獲取訓練檔期模型
            kgModel = restore_model(
                model_name_path=model_path + f"{trainDate.strftime('%Y%m%d')}_ComplExModel_KG_FocusE.pkl")
            ent_to_idx = kgModel.ent_to_idx
            get_ent_idxs_func_ = np.vectorize(ent_to_idx.get)
            ent_list = kgModel.trained_model_params[0]  # Entity Embedding
            rel_to_idx = kgModel.rel_to_idx
            get_rel_idxs_func_ = np.vectorize(rel_to_idx.get)
            rel_list = kgModel.trained_model_params[1]  # Relation Embedding

            # 獲取當今檔期向量(近似過去檔期)
            if validDate < datetime.datetime.strptime('2021-09-08', "%Y-%m-%d"):
                Y_item = f'5222123_{validDate.strftime("%Y-%m-%d")}'
            else:
                Y_item = f'5680946_{validDate.strftime("%Y-%m-%d")}'
            entity_embedding_Item_avg, similary_item = \
                self.getSimilarityAvgVec(kgModel, Y_item, datetime_np, item_np, validDate)

            # 訓練/驗證資料
            data_np = df.loc[df['data_set'] <= validDate.strftime("%Y-%m-%d")].reset_index(drop=True).to_numpy()
            data = []
            for _ in tqdm(data_np):
                id_ = _[0]
                num_ = _[1]
                data_set = _[2]
                Y_item = _[3]

                if id_ in ent_to_idx.keys():
                    entity_embedding_UserID = ent_list[get_ent_idxs_func_(id_)]
                else:
                    entity_embedding_UserID = [0 for _ in range(len(ent_list[get_ent_idxs_func_(Y_item)]))]

                if datetime.datetime.strptime(data_set, "%Y-%m-%d") < validDate:
                    entity_embedding_Item = ent_list[get_ent_idxs_func_(Y_item)]
                else:
                    entity_embedding_Item = entity_embedding_Item_avg
                data.append([id_, num_, data_set])

                # 一般項/交叉項
                data[-1].extend(entity_embedding_UserID)
                data[-1].extend(entity_embedding_Item)
                for ind_ in range(len(entity_embedding_Item)):
                    data[-1].append(entity_embedding_UserID[ind_] * entity_embedding_Item[ind_])

            # XGB資料
            dataDF = pd.DataFrame(data)
            vector_cols = [f'col_{_}' for _ in range(dataDF.shape[1] - 3)]
            cols = ['id', 'Y', 'data_set'] + vector_cols
            dataDF.columns = cols
            dataDF.to_csv(output_path + f"xgbTraindf_{trainDate.strftime('%Y%m%d')}.csv")

        return "MakePreProcessFreeFuction", None

    # 預測過去該檔期出現的人
    @classmethod
    def MakePreProcess_KnowledgeGraph_P3_1_3(self, makeInfo):
        # return "MakePreProcessFreeFuction", None
        from ampligraph.utils import save_model, restore_model

        model_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M2_1_1/"
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R3_1_3/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_3/"
        hiveCtrl = RawPreModel.getHiveCtrl()

        # 所有資料
        df = pd.DataFrame()
        trainDate_list = makeInfo['parameter']['preprocess']['trainDate']
        startDate = datetime.datetime.strptime(trainDate_list[0], '%Y-%m-%d')
        for trainDate_ in tqdm(trainDate_list):
            dataDF = pd.read_csv(input_path + f"traindf_{trainDate_.replace('-', '')}.csv") \
                .fillna(0).drop('Unnamed: 0', axis=1)
            df = pd.concat([df, dataDF])

        # 建XGB資料
        datetime_np, item_np = self.getEventItemList(hiveCtrl)
        validDate_list = makeInfo['parameter']['preprocess']['validDate']
        for ind_ in range(2, len(validDate_list)):
            innerDate1 = datetime.datetime.strptime(validDate_list[ind_ - 2], '%Y-%m-%d')
            innerDate2 = datetime.datetime.strptime(validDate_list[ind_ - 1], '%Y-%m-%d')
            trainDate = datetime.datetime.strptime(validDate_list[ind_ - 1], '%Y-%m-%d')
            validDate = datetime.datetime.strptime(validDate_list[ind_], '%Y-%m-%d')
            print(trainDate, validDate)

            # 獲取訓練檔期模型
            kgModel = restore_model(
                model_name_path=model_path + f"{trainDate.strftime('%Y%m%d')}_ComplExModel_KG_FocusE.pkl")
            ent_to_idx = kgModel.ent_to_idx
            get_ent_idxs_func_ = np.vectorize(ent_to_idx.get)
            ent_list = kgModel.trained_model_params[0]  # Entity Embedding
            rel_to_idx = kgModel.rel_to_idx
            get_rel_idxs_func_ = np.vectorize(rel_to_idx.get)
            rel_list = kgModel.trained_model_params[1]  # Relation Embedding

            # 獲取當今檔期向量(近似過去檔期)
            if validDate < datetime.datetime.strptime('2021-09-08', "%Y-%m-%d"):
                Y_item = f'5222123_{validDate.strftime("%Y-%m-%d")}'
            else:
                Y_item = f'5680946_{validDate.strftime("%Y-%m-%d")}'
            entity_embedding_Item_avg, similary_item = \
                self.getSimilarityAvgVec(kgModel, Y_item, datetime_np, item_np, startDate, validDate)

            # 訓練資料
            train_df = df.loc[df['data_set'] < validDate.strftime("%Y-%m-%d")].reset_index(drop=True)
            users_list = df.loc[
                (df['data_set'] >= innerDate1.strftime("%Y-%m-%d")) &
                (df['data_set'] <= innerDate2.strftime("%Y-%m-%d")),
                'service_account_id'
            ].unique()
            print(train_df.shape)
            print(pd.concat([train_df.head(),train_df.tail()]))

            # 驗證資料
            valid_df =  df.loc[
                (df['data_set'] >= innerDate1.strftime("%Y-%m-%d")) &
                (df['data_set'] <= innerDate2.strftime("%Y-%m-%d")),
                ['service_account_id', 'Y_item']
            ].reset_index(drop=True)
            # valid_df = pd.DataFrame()
            # for item_ in similary_item:
            #     valid_df = pd.concat([valid_df, df.loc[df['Y_item'] == item_, ['service_account_id', 'Y_item']].reset_index(drop=True)])
            # valid_df = valid_df.loc[valid_df['service_account_id'].isin(users_list)].reset_index(drop=True)
            valid_df['data_set'] = 'valid'
            valid_df = pd.merge(
                valid_df[['service_account_id', 'Y_item', 'data_set']]
                , df.loc[df['data_set'] == validDate.strftime("%Y-%m-%d"), ['service_account_id', 'Y']].reset_index(drop=True)
                , on='service_account_id'
                , how='left'
            ).fillna(0)
            print(valid_df.shape)
            print(pd.concat([valid_df.head(),valid_df.tail()]))

            with open(output_path + f"/xgbTraindf_{trainDate.strftime('%Y%m%d')}.csv", 'w',
                      encoding='utf-8', newline='') as csvfile:

                vector_cols = ['id', 'Y', 'data_set'] + [f'col_{_}' for _ in range(len(entity_embedding_Item_avg)*3 + 1)]
                writer = csv.writer(csvfile)
                writer.writerow(vector_cols)

                data_np = pd.concat([train_df[['service_account_id', 'Y', 'data_set', 'Y_item']],
                                     valid_df[['service_account_id', 'Y', 'data_set', 'Y_item']]]).to_numpy()
                for _ in tqdm(data_np):
                    data = []
                    id_ = _[0]
                    num_ = _[1]
                    data_set = _[2]
                    Y_item = _[3]

                    relation_embedding = rel_list[get_rel_idxs_func_('購買')]
                    if id_ in ent_to_idx.keys():
                        entity_embedding_UserID = ent_list[get_ent_idxs_func_(id_)]
                    else:
                        continue
                        # entity_embedding_UserID = [0 for _ in range(len(ent_list[get_ent_idxs_func_(Y_item)]))]

                    if data_set == 'valid':
                        entity_embedding_Item = entity_embedding_Item_avg
                    else:
                        entity_embedding_Item = ent_list[get_ent_idxs_func_(Y_item)]

                    # 一般項/交叉項
                    cos_similiary = 0
                    v1_len = v2_len = 0
                    data.append([id_, num_, data_set])
                    data[-1].extend(entity_embedding_UserID)
                    data[-1].extend(entity_embedding_Item)
                    data[-1].extend(self.complexScoreArr(entity_embedding_UserID, relation_embedding, entity_embedding_Item, 64))
                    data[-1].append(self.complexScore(entity_embedding_UserID, relation_embedding, entity_embedding_Item, 64))
                    # for ind_ in range(len(entity_embedding_Item)):
                    #     data[-1].append(round(entity_embedding_UserID[ind_] * entity_embedding_Item[ind_], 4))
                    #     cos_similiary += entity_embedding_UserID[ind_] * entity_embedding_Item[ind_]
                    #     v1_len += entity_embedding_UserID[ind_] ** 2
                    #     v2_len += entity_embedding_Item[ind_] ** 2
                    # cos_similiary /= (v1_len ** 0.5)
                    # cos_similiary /= (v2_len ** 0.5)
                    # data[-1].append(round(cos_similiary, 4))

                    writer.writerow(data[-1])

        return "MakePreProcessFreeFuction", None

    # 預測過去該檔期出現的人
    @classmethod
    def MakePreProcess_KnowledgeGraph_P3_1_4(self, makeInfo):
        # return "MakePreProcessFreeFuction", None
        from ampligraph.utils import save_model, restore_model

        model_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M2_1_1/"
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R3_1_4/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_4/"
        hiveCtrl = RawPreModel.getHiveCtrl()

        # 所有資料
        trainDate_list = makeInfo['parameter']['preprocess']['trainDate']
        startDate = datetime.datetime.strptime(trainDate_list[0], '%Y-%m-%d')

        # 建XGB資料
        datetime_np, item_np = self.getEventItemList(hiveCtrl)
        validDate_list = makeInfo['parameter']['preprocess']['validDate']
        for ind_ in range(1, len(validDate_list)):
            trainDate = datetime.datetime.strptime(validDate_list[ind_ - 1], '%Y-%m-%d')
            validDate = datetime.datetime.strptime(validDate_list[ind_], '%Y-%m-%d')
            print()
            print(trainDate, validDate)

            # 獲取訓練檔期模型
            kgModel = restore_model(
                model_name_path=model_path + f"{trainDate.strftime('%Y%m%d')}_ComplExModel_KG_FocusE.pkl")
            ent_to_idx = kgModel.ent_to_idx
            get_ent_idxs_func_ = np.vectorize(ent_to_idx.get)
            ent_list = kgModel.trained_model_params[0]  # Entity Embedding
            rel_to_idx = kgModel.rel_to_idx
            get_rel_idxs_func_ = np.vectorize(rel_to_idx.get)
            rel_list = kgModel.trained_model_params[1]  # Relation Embedding

            # 獲取當今檔期向量(近似過去檔期)
            if validDate < datetime.datetime.strptime('2021-09-08', "%Y-%m-%d"):
                Y_item = f'5222123_{validDate.strftime("%Y-%m-%d")}'
            else:
                Y_item = f'5680946_{validDate.strftime("%Y-%m-%d")}'
            entity_embedding_Item_avg, similary_item = \
                self.getSimilarityAvgVec(kgModel, Y_item, datetime_np, item_np, startDate, validDate, topk=1)

            # 訓練資料
            train_df = pd.read_csv(input_path + f"traindf_{validDate.strftime('%Y%m%d')}.csv") \
                .fillna(0).drop('Unnamed: 0', axis=1)

            with open(output_path + f"/xgbTraindf_{validDate.strftime('%Y%m%d')}.csv", 'w', encoding='utf-8', newline='') as csvfile:
                writer = csv.writer(csvfile)

                vector_cols = ['id', 'Y', 'data_set', 'score']
                writer.writerow(vector_cols)

                data_np = train_df.to_numpy()
                for _ in tqdm(data_np):
                    data = []
                    id_ = _[0]
                    num_ = _[1]
                    data_set = _[2]

                    relation_embedding = rel_list[get_rel_idxs_func_('購買')]
                    entity_embedding_Item = entity_embedding_Item_avg
                    if id_ in ent_to_idx.keys():
                        entity_embedding_UserID = ent_list[get_ent_idxs_func_(id_)]
                        writer.writerow([id_, num_, data_set, self.complexScore(entity_embedding_UserID, relation_embedding, entity_embedding_Item, 64)])
                    else:
                        writer.writerow([id_, num_, data_set, 0])


        return "MakePreProcessFreeFuction", None

    # 預測過去該檔期出現的人
    @classmethod
    def MakePreProcess_KnowledgeGraph_P3_1_5(self, makeInfo):
        # return "MakePreProcessFreeFuction", None
        from ampligraph.utils import save_model, restore_model

        model_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M2_1_1/"
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R3_1_4/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_5/"
        hiveCtrl = RawPreModel.getHiveCtrl()

        # 所有資料
        df = pd.DataFrame()
        trainDate_list = makeInfo['parameter']['preprocess']['trainDate']
        startDate = datetime.datetime.strptime(trainDate_list[0], '%Y-%m-%d')
        for trainDate_ in tqdm(trainDate_list):
            dataDF = pd.read_csv(input_path + f"traindf_{trainDate_.replace('-', '')}.csv") \
                .fillna(0).drop('Unnamed: 0', axis=1)
            df = pd.concat([df, dataDF])

        # 建XGB資料
        datetime_np, item_np = self.getEventItemList(hiveCtrl)
        validDate_list = makeInfo['parameter']['preprocess']['validDate']
        for ind_ in range(1, len(validDate_list)):
            trainDate = datetime.datetime.strptime(validDate_list[ind_ - 1], '%Y-%m-%d')
            validDate = datetime.datetime.strptime(validDate_list[ind_], '%Y-%m-%d')
            print()
            print(trainDate, validDate)

            # 獲取訓練檔期模型
            kgModel = restore_model(
                model_name_path=model_path + f"{trainDate.strftime('%Y%m%d')}_ComplExModel_KG_FocusE.pkl")
            ent_to_idx = kgModel.ent_to_idx
            get_ent_idxs_func_ = np.vectorize(ent_to_idx.get)
            ent_list = kgModel.trained_model_params[0]  # Entity Embedding
            rel_to_idx = kgModel.rel_to_idx
            get_rel_idxs_func_ = np.vectorize(rel_to_idx.get)
            rel_list = kgModel.trained_model_params[1]  # Relation Embedding


            # 訓練資料
            train_df = df.loc[df['data_set'] <= validDate.strftime("%Y-%m-%d")].reset_index(drop=True)
            print(train_df.shape)
            print(pd.concat([train_df.head(),train_df.tail()]))

            with open(output_path + f"/xgbTraindf_{validDate.strftime('%Y%m%d')}.csv", 'w',
                      encoding='utf-8', newline='') as csvfile:

                vector_cols = ['id', 'Y', 'data_set'] + [f'col_{_}' for _ in range(128*3 + 1)]
                writer = csv.writer(csvfile)
                writer.writerow(vector_cols)

                data_np = train_df.to_numpy()
                for _ in tqdm(data_np):
                    data = []
                    id_ = _[0]
                    num_ = _[1]
                    data_set = _[2]
                    Y_item = _[3]

                    relation_embedding = rel_list[get_rel_idxs_func_('購買')]
                    if id_ in ent_to_idx.keys():
                        entity_embedding_UserID = ent_list[get_ent_idxs_func_(id_)]
                    else:
                        # continue
                        entity_embedding_UserID = [0 for _ in range(len(ent_list[get_ent_idxs_func_(Y_item)]))]

                    # 獲取當今檔期向量(近似過去檔期)
                    entity_embedding_Item, similary_item = \
                        self.getSimilarityAvgVec(kgModel, Y_item, datetime_np, item_np, startDate, validDate)

                    # 一般項/交叉項
                    cos_similiary = 0
                    v1_len = v2_len = 0
                    data.append([id_, num_, data_set])
                    data[-1].extend(entity_embedding_UserID)
                    data[-1].extend(entity_embedding_Item)
                    data[-1].extend(self.complexScoreArr(entity_embedding_UserID, relation_embedding, entity_embedding_Item, 64))
                    data[-1].append(self.complexScore(entity_embedding_UserID, relation_embedding, entity_embedding_Item, 64))
                    # for ind_ in range(len(entity_embedding_Item)):
                    #     data[-1].append(round(entity_embedding_UserID[ind_] * entity_embedding_Item[ind_], 4))
                    #     cos_similiary += entity_embedding_UserID[ind_] * entity_embedding_Item[ind_]
                    #     v1_len += entity_embedding_UserID[ind_] ** 2
                    #     v2_len += entity_embedding_Item[ind_] ** 2
                    # cos_similiary /= (v1_len ** 0.5)
                    # cos_similiary /= (v2_len ** 0.5)
                    # data[-1].append(round(cos_similiary, 4))

                    writer.writerow(data[-1])
            break

        return "MakePreProcessFreeFuction", None

