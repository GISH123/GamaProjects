import matplotlib.pyplot as plt
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
import heapq
from maple.KnowledgeGraph.modeling.old.PreProcess_KnowledgeGraph_OLD import PreProcess_KnowledgeGraph_OLD
from sklearn.metrics.pairwise import cosine_similarity
from package.common.common.RawPreModel import RawPreModel
from sklearn import preprocessing
import scipy.stats as ss
from scipy.stats import kde

class PreProcess_KnowledgeGraph(PreProcess_KnowledgeGraph_OLD) :
    ####################################################################################################################

    # 獲得檔期list
    @classmethod
    def getEventList(self, hiveCtrl):
        sql_str = '''
                SELECT DISTINCT commondata_006 as Date
                FROM gtwpd.model_usedata  
                WHERE 1=1
                    AND product = 'maple'
                    AND project='KnowledgeGraph'
                    AND version='R0_1_1'
                    AND commondata_006 != ''
                '''
        # print(sql_str)
        df = hiveCtrl.searchSQL(sql_str)
        dt_list = df.date.unique()[:-1]
        # print(dt_list)
        return dt_list

    @classmethod
    def getEventItemList(self, hiveCtrl):
        item_list = []
        dt_list = self.getEventList(hiveCtrl)
        datetime_dt = []
        for dt_ in dt_list:
            datetime_dt.append(datetime.datetime.strptime(dt_, "%Y-%m-%d %H:%M:%S"))
            if datetime.datetime.strptime(dt_, "%Y-%m-%d %H:%M:%S") < datetime.datetime.strptime('2021-09-08',
                                                                                                 "%Y-%m-%d"):
                item_ = f"5222123_{dt_[:10]}"
            else:
                item_ = f"5680946_{dt_[:10]}"
            item_list.append(item_)
        datetime_dt = np.array(datetime_dt)
        item_list = np.array(item_list)
        return datetime_dt, item_list

    # 獲得最近一期完成的檔期資料
    @classmethod
    def getLastOverEventRange(self, today, hiveCtrl):
        sql_str = '''
                SELECT DISTINCT commondata_006 as Date
                FROM gtwpd.model_usedata  
                WHERE 1=1
                    AND product = 'maple'
                    AND project='KnowledgeGraph'
                    AND version='R0_1_1'
                    AND commondata_006 != ''
                '''
        df = hiveCtrl.searchSQL(sql_str)
        dt_list = df.date.unique()
        # print(dt_list)

        today_dt = datetime.datetime.strptime(today, "%Y-%m-%d")
        dt_ind_ = 0
        for ind_ in range(0, len(dt_list) - 1):
            dt = datetime.datetime.strptime(dt_list[ind_], "%Y-%m-%d %H:%M:%S")
            if today_dt < dt: break

            dt_ind_ = ind_

        return (
            datetime.datetime.strptime(dt_list[dt_ind_- 1], "%Y-%m-%d %H:%M:%S"),
            datetime.datetime.strptime(dt_list[dt_ind_], "%Y-%m-%d %H:%M:%S")
        )

    @classmethod
    def getLast2OverEventRange(self, today, hiveCtrl):
        sql_str = '''
                SELECT DISTINCT commondata_006 as Date
                FROM gtwpd.model_usedata  
                WHERE 1=1
                    AND product = 'maple'
                    AND project='KnowledgeGraph'
                    AND version='R0_1_1'
                    AND commondata_006 != ''
                '''
        df = hiveCtrl.searchSQL(sql_str)
        dt_list = df.date.unique()
        # print(dt_list)

        today_dt = datetime.datetime.strptime(today, "%Y-%m-%d")
        dt_ind_ = 0
        for ind_ in range(0, len(dt_list) - 1):
            dt = datetime.datetime.strptime(dt_list[ind_], "%Y-%m-%d %H:%M:%S")
            if today_dt < dt: break

            dt_ind_ = ind_

        return (
            datetime.datetime.strptime(dt_list[dt_ind_ - 2], "%Y-%m-%d %H:%M:%S"),
            datetime.datetime.strptime(dt_list[dt_ind_ - 1], "%Y-%m-%d %H:%M:%S"),
            datetime.datetime.strptime(dt_list[dt_ind_], "%Y-%m-%d %H:%M:%S")
        )

    ####################################################################################################################
    @classmethod
    def getGraph(self, graph_data):
        # raw graph
        data_np = graph_data.to_numpy()
        graph = defaultdict(lambda: defaultdict(list))
        for _ in data_np:
            entity1 = str(_[0])
            relation = str(_[1])
            entity2 = str(_[2])
            num = float(_[3])
            graph[entity1][relation].append((entity2, num))
        return graph

    @classmethod
    def getGraphInit(self, graph, entity, relation_list, type='groupby', new_str = ''):
        if type == 'groupby':
            for rel_ in relation_list:
                tmp_ = defaultdict(lambda: 0)
                for tag_, num in graph[entity][rel_]:
                    tmp_[tag_] += num
                graph[entity][new_str] = []
                for tag_, num in tmp_.items():
                    graph[entity][new_str].append((tag_, num))

        elif type == 'leftjoin':
            num_list = self.getRootLeafEdge(graph=graph, node=entity, edge_num=1, edge_list=relation_list, num_list=[])
            tmp_ = defaultdict(lambda: 0)
            for tag_, num in num_list:
                tmp_[tag_] += num
            for tag_, num in tmp_.items():
                graph[entity][new_str].append((tag_, num))

        return graph

    @classmethod
    def getReverseGraph(self, graph):
        reverse_graph = defaultdict(lambda: defaultdict(lambda: 0))
        for ent1_ in graph.keys():
            for rel_, ent_list_ in graph[ent1_].items():
                for ent2_, num_ in ent_list_:
                    reverse_graph[ent2_][ent1_] = num_
        return reverse_graph

    @classmethod
    def getSimilarityGraph(self, graph, reverse_graph, item_list, relation_list, target_item):
        # print(graph, reverse_graph)
        item_similarity = defaultdict(lambda : defaultdict(lambda: 0))
        for item_ in item_list:
            for rel_ in relation_list:
                for tag_, num_ in graph[item_][rel_]:
                    item_similarity[item_]['len'] += num_**2
                    item_similarity[item_]['similary'] += reverse_graph[tag_][target_item] * num_

        for item_ in item_similarity.keys():
            item_similarity[item_]['similary'] /= (item_similarity[item_]['len'] **0.5 * item_similarity[target_item]['len'] **0.5)
        return item_similarity

    @classmethod
    def getTopKSimilarityItem(self, target_item, similarity_graph, filter_dt, ent_list, get_ent_idxs_func_):
        print(target_item, similarity_graph, filter_dt, ent_list, get_ent_idxs_func_(target_item))
        similarity_list = []
        similarity_item = []
        for item_ in similarity_graph.keys():
            if datetime.datetime.strptime(item_.split('_')[1], '%Y-%m-%d') >= filter_dt: continue
            similarity_list.append(similarity_graph[item_]['similary'])
            similarity_item.append(item_)
        indCosineTuple = heapq.nlargest(2, enumerate(similarity_list), key=lambda x: x[1])
        print(f'indCosineTuple:{indCosineTuple}')

        similary_item = []
        entity_embedding_Item_avg = np.zeros_like(ent_list[get_ent_idxs_func_(target_item)])
        for ind_, cosine_ in indCosineTuple:
            print(target_item, similarity_graph, filter_dt, ent_list, get_ent_idxs_func_(similarity_item[ind_]))
            similary_item.append(similarity_item[ind_])
            entity_embedding_Item_avg += ent_list[get_ent_idxs_func_(similarity_item[ind_])]
        return entity_embedding_Item_avg, similary_item

    # 產生node到路徑edge_list尾端的連線
    @classmethod
    def getRootLeafEdge(self, graph, node, edge_num, edge_list, num_list):
        if edge_list == []:
            num_list.append((node, edge_num))
            return num_list

        edge = edge_list[0]
        if edge in graph[node].keys(): node_list = graph[node][edge]
        else: return num_list

        for node_, num_ in node_list:
            num_list = self.getRootLeafEdge(
                graph = graph, node = node_, edge_num = num_ * edge_num,
                edge_list = edge_list[1:], num_list = num_list
            )
        return num_list

    @classmethod
    def complexScore(self, ent1, rel, ent2, half_vec_len):
        RrRsRo = np.sum(np.multiply(rel[:half_vec_len], np.multiply(ent1[:half_vec_len], ent2[:half_vec_len])))
        RrIsIo = np.sum(np.multiply(rel[:half_vec_len], np.multiply(ent1[half_vec_len:], ent2[half_vec_len:])))
        IrRsIo = np.sum(np.multiply(rel[half_vec_len:], np.multiply(ent1[:half_vec_len], ent2[half_vec_len:])))
        IrIsRo = np.sum(np.multiply(rel[half_vec_len:], np.multiply(ent1[half_vec_len:], ent2[:half_vec_len])))
        return RrRsRo + RrIsIo + IrRsIo - IrIsRo

    @classmethod
    def complexScoreArr(self, ent1, rel, ent2, half_vec_len):
        scoreArr = np.multiply(rel[:half_vec_len], np.multiply(ent1[:half_vec_len], ent2[:half_vec_len]))
        scoreArr += np.multiply(rel[:half_vec_len], np.multiply(ent1[half_vec_len:], ent2[half_vec_len:]))
        scoreArr += np.multiply(rel[half_vec_len:], np.multiply(ent1[:half_vec_len], ent2[half_vec_len:]))
        scoreArr -= np.multiply(rel[half_vec_len:], np.multiply(ent1[half_vec_len:], ent2[:half_vec_len]))
        return list(scoreArr)

    ####################################################################################################################
    @classmethod
    def MakePreProcess_KnowledgeGraph_P0_1_2(self, makeInfo):
        def getFashionBoxBUInfo(hiveCtrl):
            sql_str = '''
                    SELECT 
                        commondata_001, commondata_002, commondata_003, commondata_004, commondata_005, commondata_006
                        , commondata_007, commondata_008, uniquefloat_001, uniquefloat_002, uniquefloat_003, uniquefloat_004
                        , uniquefloat_005, uniquefloat_006, uniquefloat_007, uniquefloat_008, uniquefloat_009
                        , uniquefloat_010, uniquefloat_011, uniquefloat_012, uniquefloat_013, uniquefloat_014
                        , uniquefloat_015, uniquefloat_016, uniquefloat_017, uniquefloat_018, uniquefloat_019
                        , uniquefloat_020, uniquefloat_021
                    FROM gtwpd.model_usedata  
                    WHERE 1=1
                        AND product = 'maple'
                        AND project='ExternalDataManage'
                        AND version='R1_0_1'
                        AND commondata_006 != ''
                    '''
            print(sql_str)
            cols = [
                'version', 'Part', 'ItemID', 'Name', 'KR Name', 'Date1', 'Date2', '客觀概述'
                , 'Prob.', '市價', '大師標籤', 'IP合作'
                , '漂浮特效', '暗色系', '螢光系', '柔和色系', '可愛風格'
                , '制服風格', '貴族風格', '動物風格', '怪物風格', '角色風格'
                , '自然風格', '重裝風格', '運動風格', '搞怪風格', '不擋身/腿'
                , '不擋住名字', '渲染特效'
            ]
            df = hiveCtrl.searchSQL(sql_str)
            df.columns = cols
            return df
        # return "MakePreProcessFreeFuction", None

        hiveCtrl = RawPreModel.getHiveCtrl()
        df = getFashionBoxBUInfo(hiveCtrl)
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
        ItemWithConcept = pd.melt(df, id_vars=['ItemID'], value_vars=meltcols)
        eventWithItem = df.loc[:, ["Date1", "ItemID", "Prob."]].reset_index(drop=True)

        eventWithoutward.columns = ['entity1', 'entity2']
        ItemWithConcept.columns = ['entity1', 'entity2', 'num']
        eventWithItem.columns = ['entity1', 'entity2', 'num']

        eventWithoutward['relation'] = '外觀'
        eventWithoutward['num'] = 1.0
        ItemWithConcept['relation'] = '是'
        eventWithItem['relation'] = '產生'

        ItemWithConcept = ItemWithConcept.loc[ItemWithConcept.num != 0].reset_index(drop=True)

        boxIndex1 = eventWithItem['entity1'] < '2021-09-08'
        boxIndex2 = eventWithItem['entity1'] >= '2021-09-08'
        eventWithItem['entity1'] = pd.to_datetime(eventWithItem['entity1']).dt.strftime('%Y-%m-%d')
        eventWithItem.loc[boxIndex1, 'entity1'] = '5222123_' + eventWithItem.loc[boxIndex1, 'entity1']
        eventWithItem.loc[boxIndex2, 'entity1'] = '5680946_' + eventWithItem.loc[boxIndex2, 'entity1']

        kgGraph = pd.concat([ItemWithConcept, eventWithItem, eventWithoutward])

        upload_df = RawPreModel.getUploadDataFrame()
        upload_df['commondata_001'] = kgGraph['entity1']
        upload_df['commondata_002'] = kgGraph['relation']
        upload_df['commondata_003'] = kgGraph['entity2']
        upload_df['uniquefloat_001'] = kgGraph['num']

        return "MakePreProcessFileInsertOverwrite", upload_df, {}

    # 所有圖譜資料
    @classmethod
    def MakePreProcess_KnowledgeGraph_P1_1_1(self, makeInfo):
        # return "MakePreProcessFreeFuction", None
        hiveCtrl = RawPreModel.getHiveCtrl()

        eventStartDate, eventEndDate = makeInfo['parameter']['makedate']
        eventStartDate = datetime.datetime.strptime(eventStartDate, "%Y-%m-%d")
        eventEndDate = datetime.datetime.strptime(eventEndDate, "%Y-%m-%d")
        print(eventStartDate, eventEndDate)

        # 建userID購買fashionbox資料
        hiveCtrl.executeSQL(f"DROP TABLE IF EXISTS gtwpd.peiyuwu_kgUserIdPurchaseFashionBox_{eventStartDate.strftime('%Y%m%d')}")
        sql_str = f'''
            CREATE TABLE IF NOT EXISTS gtwpd.peiyuwu_kgUserIdPurchaseFashionBox_{eventStartDate.strftime('%Y%m%d')} AS
            SELECT
                CommonData_1 as Entity1
                , '購買' as Relation
                , AA.CommonData_10 as Entity2
                , 1 as num
                , dt
            FROM gtwpd.modelextract_modelextract AA
            WHERE 1 = 1
                AND AA.game = 'maple'
                AND AA.tablenumber = '16009'
                AND AA.dt >= '20201230'
                AND AA.dt < {eventEndDate.strftime('%Y%m%d')}
                AND AA.UniqueStr_1 = 'bf point'
                AND AA.UniqueStr_11 != 'RollBack'
                AND AA.commondata_10 in ('5222123', '5680946')
        '''
        print(sql_str)
        hiveCtrl.executeSQL(sql_str)

        # 建userID登入資料
        hiveCtrl.executeSQL(f"DROP TABLE IF EXISTS gtwpd.peiyuwu_KGRDFN_userIdLogin_{eventStartDate.strftime('%Y%m%d')}")
        sql_str = f'''
               CREATE TABLE IF NOT EXISTS gtwpd.peiyuwu_KGRDFN_userIdLogin_{eventStartDate.strftime('%Y%m%d')} AS
               SELECT
                   commondata_1 as Entity1
                   , 'Login' as Relation
                   , 'maple' as Entity2
                   , sum(
                       cast((unix_timestamp(UniqueTime_2, 'yyyy-mm-dd HH:mm:ss') - unix_timestamp(UniqueTime_1, 'yyyy-mm-dd HH:mm:ss')	) / 3600 as int)
                   ) as num
               FROM
                   gtwpd.modelextract_modelextract
               WHERE 1=1
               AND game='maple'
                   AND dt >= {eventStartDate.strftime('%Y%m%d')}
                   AND dt < {eventEndDate.strftime('%Y%m%d')}
                   AND tablenumber = '1103'
               GROUP BY commondata_1
           '''
        print(sql_str)
        hiveCtrl.executeSQL(sql_str)

        # 建userID持有資料(by event)
        hiveCtrl.executeSQL(f"DROP TABLE IF EXISTS gtwpd.peiyuwu_KGRDFN_UserIdItemId_{eventStartDate.strftime('%Y%m%d')}")
        sql_str = f'''
               CREATE TABLE IF NOT EXISTS gtwpd.peiyuwu_KGRDFN_UserIdItemId_{eventStartDate.strftime('%Y%m%d')} AS
               SELECT
                   aa.commondata_1 as Entity1
                   , '持有' as Relation
                   , aa.commondata_8 as Entity2
                   , COUNT(aa.commondata_8) as num
               FROM
                   gtwpd.modelextract_modelextract aa
               WHERE 1=1
                   AND aa.game='maple'
                   AND aa.dt = {(eventEndDate + datetime.timedelta(days=-1)).strftime('%Y%m%d')}
                   AND aa.tablenumber = '3012'
                   AND aa.UniqueStr_1 = 'casheqp'
               GROUP BY aa.commondata_1, aa.commondata_8
           '''
        print(sql_str)
        hiveCtrl.executeSQL(sql_str)

        # 建userID購買資料(by event)
        hiveCtrl.executeSQL(f"DROP TABLE IF EXISTS gtwpd.peiyuwu_KGRDFN_userIdPurchase_{eventStartDate.strftime('%Y%m%d')}")
        sql_str = f'''
               CREATE TABLE IF NOT EXISTS gtwpd.peiyuwu_KGRDFN_userIdPurchase_{eventStartDate.strftime('%Y%m%d')} AS
               SELECT
                   CommonData_1 as Entity1
                   , '購買' as Relation
                   , AA.CommonData_10 as Entity2
                   , COUNT(AA.CommonData_10) as num
               FROM gtwpd.modelextract_modelextract AA
               WHERE 1 = 1
                   AND AA.game = 'maple'
                   AND AA.tablenumber = '16009'
                   AND AA.dt >= {eventStartDate.strftime('%Y%m%d')}
                   AND AA.dt < {eventEndDate.strftime('%Y%m%d')}
                   AND AA.UniqueStr_1 = 'bf point'
                   AND AA.UniqueStr_11 != 'RollBack'
                   AND AA.commondata_10 IS NOT NULL
                   AND AA.commondata_10 != '0'
               GROUP BY AA.CommonData_1, AA.CommonData_10
           '''
        print(sql_str)
        hiveCtrl.executeSQL(sql_str)

        return "MakePreProcessFreeFuction", None, {}

    # 產生新的圖譜知識
    @classmethod
    def MakePreProcess_KnowledgeGraph_P2_1_2(self, makeInfo):
        # return "MakePreProcessFreeFuction", None
        inputFilePath = 'D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_1_2'
        outputFilePath = 'D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P2_1_2'

        eventStartDate, eventEndDate = makeInfo['parameter']['makedate']
        eventStartDate = datetime.datetime.strptime(eventStartDate, "%Y-%m-%d")
        eventEndDate = datetime.datetime.strptime(eventEndDate, "%Y-%m-%d")
        # print(eventStartDate, eventEndDate)

        df = pd.read_csv(inputFilePath + f"/KGRDFN_eventBuydf_{eventStartDate.strftime('%Y%m%d')}.csv").drop('Unnamed: 0', axis=1)
        df.columns = ['entity1','relation','entity2','num']

        df1 = pd.read_csv(inputFilePath + f"/KGRDFN_maindf_{eventStartDate.strftime('%Y%m%d')}.csv").drop('Unnamed: 0', axis=1)
        df1.columns = ['entity1','relation','entity2','num']

        df2 = pd.read_csv(inputFilePath + f"/KGRDFN_eventTagdf_{eventStartDate.strftime('%Y%m%d')}.csv").drop('Unnamed: 0', axis=1)
        df2.columns = ['entity1','relation','entity2','num']

        # LEFT JOIN
        graph = self.getGraph(pd.concat([df, df1, df2]))
        entity_list = df1.loc[df1.relation == '購買', 'entity1'].unique()
        for user_ in entity_list:
            graph = self.getGraphInit(graph, user_, ['購買', '產生', '是'], type='leftjoin', new_str='是')
            graph = self.getGraphInit(graph, user_, ['購買', '產生', '外觀'], type='leftjoin', new_str='外觀')

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
        return "MakePreProcessFreeFuction", None ,{}

    # 預測過去該檔期出現的人
    @classmethod
    def MakePreProcess_KnowledgeGraph_P3_1_6(self, makeInfo):
        return "MakePreProcessFreeFuction", None
        from ampligraph.utils import save_model, restore_model

        model_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M2_1_2/"
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R3_1_4/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_6/"

        # 所有資料
        df = pd.DataFrame()
        trainDate_list = makeInfo['parameter']['traindatelist']
        for trainDate_ in tqdm(trainDate_list):
            dataDF = pd.read_csv(input_path + f"traindf_{trainDate_.replace('-', '')}.csv") \
                .fillna(0).drop('Unnamed: 0', axis=1)
            df = pd.concat([df, dataDF])

        # 建XGB資料
        for trainDate, validDate in makeInfo['parameter']['validDate']:
            trainDate = datetime.datetime.strptime(trainDate, "%Y-%m-%d")
            validDate = datetime.datetime.strptime(validDate, "%Y-%m-%d")
            print()
            print(trainDate, validDate)

            # 獲取訓練檔期模型
            if trainDate <= datetime.datetime.strptime('2022-06-01', '%Y-%m-%d'):
                print(f"use model:{trainDate.strftime('%Y%m%d')}_ComplExModel_KG_FocusE.pkl")
                kgModel = restore_model(model_name_path=model_path + f"{trainDate.strftime('%Y%m%d')}_ComplExModel_KG_FocusE.pkl")
            else:
                print(f"use model:20220601_ComplExModel_KG_FocusE.pkl")
                kgModel = restore_model(model_name_path=model_path + f"20220601_ComplExModel_KG_FocusE.pkl")
            ent_to_idx = kgModel.ent_to_idx
            get_ent_idxs_func_ = np.vectorize(ent_to_idx.get)
            ent_list = kgModel.trained_model_params[0]  # Entity Embedding
            rel_to_idx = kgModel.rel_to_idx
            get_rel_idxs_func_ = np.vectorize(rel_to_idx.get)
            rel_list = kgModel.trained_model_params[1]  # Relation Embedding

            if validDate < datetime.datetime.strptime('2021-09-08', "%Y-%m-%d"):
                Y_item = f'5222123_{validDate.strftime("%Y-%m-%d")}'
            else:
                Y_item = f'5680946_{validDate.strftime("%Y-%m-%d")}'

            # 圖搜索
            graph_data = pd.read_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_1_1/KGRDFN_eventTagdf_{validDate.strftime('%Y%m%d')}.csv").drop('Unnamed: 0', axis=1)
            graph_data.columns = ['entity1','relation','entity2','num']
            graph = self.getGraph(graph_data)
            entity_list2 = graph_data.loc[graph_data.relation == '產生', 'entity1'].unique()
            for user_ in entity_list2:
                graph = self.getGraphInit(graph, user_, ['是'], type='groupby', new_str='是')
                graph = self.getGraphInit(graph, user_, ['產生', '外觀'], type='leftjoin', new_str='外觀')
            reverse_graph = self.getReverseGraph(graph)
            similarity_graph = self.getSimilarityGraph(graph, reverse_graph, item_list=entity_list2, relation_list=['是', '外觀'], target_item=Y_item)
            entity_embedding_Item_avg, similary_item = self.getTopKSimilarityItem(Y_item, similarity_graph, validDate, ent_list, get_ent_idxs_func_)
            # print(f"graph['5222123_2021-05-26']:{graph['5222123_2021-05-26']}")
            # print(f'tag_graph:{reverse_graph}')
            # print(f'similarity_graph:{similarity_graph}')
            print(f'similary_item:{similary_item}')
            # print(f'entity_embedding_Item_avg:{entity_embedding_Item_avg}')

            # 訓練資料
            train_df = df.loc[df['data_set'] <= validDate.strftime("%Y-%m-%d")].reset_index(drop=True)
            print(train_df.shape)
            print(pd.concat([train_df.head(),train_df.tail()]))

            with open(output_path + f"xgbTraindf_{validDate.strftime('%Y%m%d')}.csv", 'w',
                      encoding='utf-8', newline='') as csvfile:

                model_dim = len(entity_embedding_Item_avg)
                vector_cols = ['id', 'Y', 'data_set'] + [f'col_{_}' for _ in range(model_dim*3 + 1)]
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

                    # 一般項/交叉項
                    data.append([id_, num_, data_set])
                    data[-1].extend(entity_embedding_UserID)
                    data[-1].extend(entity_embedding_Item_avg)
                    data[-1].extend(self.complexScoreArr(entity_embedding_UserID, relation_embedding, entity_embedding_Item_avg, int(model_dim/2)))
                    data[-1].append(self.complexScore(entity_embedding_UserID, relation_embedding, entity_embedding_Item_avg, int(model_dim/2)))

                    writer.writerow(data[-1])

        return "MakePreProcessFreeFuction", None

    # 預測過去該檔期出現的人
    @classmethod
    def MakePreProcess_KnowledgeGraph_P3_1_7(self, makeInfo):
        return "MakePreProcessFreeFuction", None

    # 預測過去該檔期出現的人
    @classmethod
    def MakePreProcess_KnowledgeGraph_P3_1_8(self, makeInfo):
        return "MakePreProcessFreeFuction", None, {}
        from ampligraph.utils import save_model, restore_model

        model_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M2_1_2/"
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{makeInfo['parameter']['RPM_path'][0]}/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{makeInfo['parameter']['RPM_path'][1]}/"

        # 所有資料
        df = pd.DataFrame()
        trainDate_list = makeInfo['parameter']['traindatelist']
        for trainDate_ in tqdm(trainDate_list):
            dataDF = pd.read_csv(input_path + f"traindf_{trainDate_.replace('-', '')}.csv") \
                .fillna(0).drop('Unnamed: 0', axis=1)
            df = pd.concat([df, dataDF])

        # 建XGB資料
        # df = df.drop_duplicates()
        print(df.shape)
        for trainDate, validDate in makeInfo['parameter']['validDate']:
            trainDate = datetime.datetime.strptime(trainDate, "%Y-%m-%d")
            validDate = datetime.datetime.strptime(validDate, "%Y-%m-%d")
            print()
            print(trainDate, validDate)

            # 獲取訓練檔期模型
            if trainDate <= datetime.datetime.strptime('2022-08-10', '%Y-%m-%d'):
                print(f"use model:{trainDate.strftime('%Y%m%d')}_ComplExModel_KG_FocusE.pkl")
                kgModel = restore_model(model_name_path=model_path + f"{trainDate.strftime('%Y%m%d')}_ComplExModel_KG_FocusE.pkl")
            else:
                print(f"use model:20220810_ComplExModel_KG_FocusE.pkl")
                kgModel = restore_model(model_name_path=model_path + f"20220601_ComplExModel_KG_FocusE.pkl")
            ent_to_idx = kgModel.ent_to_idx
            get_ent_idxs_func_ = np.vectorize(ent_to_idx.get)
            ent_list = kgModel.trained_model_params[0]  # Entity Embedding
            rel_to_idx = kgModel.rel_to_idx
            get_rel_idxs_func_ = np.vectorize(rel_to_idx.get)
            rel_list = kgModel.trained_model_params[1]  # Relation Embedding

            if validDate < datetime.datetime.strptime('2021-09-08', "%Y-%m-%d"):
                Y_item = f'5222123_{validDate.strftime("%Y-%m-%d")}'
            else:
                Y_item = f'5680946_{validDate.strftime("%Y-%m-%d")}'

            # 圖搜索
            graph_data = pd.read_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_1_2/KGRDFN_eventTagdf_{validDate.strftime('%Y%m%d')}.csv").drop('Unnamed: 0', axis=1)
            graph_data.columns = ['entity1','relation','entity2','num']
            graph = self.getGraph(graph_data)
            entity_list2 = graph_data.loc[graph_data.relation == '產生', 'entity1'].unique()
            for user_ in entity_list2:
                graph = self.getGraphInit(graph, user_, ['是'], type='groupby', new_str='是')
                graph = self.getGraphInit(graph, user_, ['產生', '外觀'], type='leftjoin', new_str='外觀')
            reverse_graph = self.getReverseGraph(graph)
            similarity_graph = self.getSimilarityGraph(graph, reverse_graph, item_list=entity_list2, relation_list=['是', '外觀'], target_item=Y_item)
            entity_embedding_Item_avg, similary_item = self.getTopKSimilarityItem(Y_item, similarity_graph, validDate, ent_list, get_ent_idxs_func_)
            # print(f"graph['5222123_2021-05-26']:{graph['5222123_2021-05-26']}")
            # print(f'tag_graph:{reverse_graph}')
            # print(f'similarity_graph:{similarity_graph}')
            print(f'similary_item:{similary_item}')
            # print(f'entity_embedding_Item_avg:{entity_embedding_Item_avg}')

            # 訓練資料
            train_df = df.loc[df['data_set'] <= validDate.strftime("%Y-%m-%d")].reset_index(drop=True)
            print(train_df.shape)
            print(pd.concat([train_df.head(1),train_df.tail(1)]))

            with open(output_path + f"xgbTraindf_{validDate.strftime('%Y%m%d')}.csv", 'w',
                      encoding='utf-8', newline='') as csvfile:

                model_dim = len(entity_embedding_Item_avg)

                tag_cols = []
                for i_ in range(9): tag_cols.append(f'tag{str(i_).zfill(3)}')
                col_raw = ['id', 'Y','lvmax_', 'data_set'] + tag_cols
                vector_cols = col_raw + [f'col_{_}' for _ in range(int(model_dim*2.5) + 1)]
                writer = csv.writer(csvfile)
                writer.writerow(vector_cols)

                data_np = train_df.to_numpy()
                for _ in tqdm(data_np):
                    # print(_)
                    data = []
                    id_ = _[0]
                    num_ = _[1]
                    lvmax_ = _[2]
                    tag_list = _[3:-2]
                    data_set = _[-2]
                    Y_item = _[-1]

                    relation_embedding = rel_list[get_rel_idxs_func_('購買')]
                    if id_ in ent_to_idx.keys():
                        entity_embedding_UserID = ent_list[get_ent_idxs_func_(id_)]
                    else:
                        # continue
                        entity_embedding_UserID = [0 for _ in range(len(ent_list[get_ent_idxs_func_(Y_item)]))]

                    # 一般項/交叉項
                    data.append([id_, num_,lvmax_, data_set])
                    data[-1].extend(tag_list)
                    data[-1].extend(entity_embedding_UserID)
                    data[-1].extend(entity_embedding_Item_avg)
                    data[-1].extend(self.complexScoreArr(entity_embedding_UserID, relation_embedding, entity_embedding_Item_avg, int(model_dim/2)))
                    data[-1].append(self.complexScore(entity_embedding_UserID, relation_embedding, entity_embedding_Item_avg, int(model_dim/2)))

                    writer.writerow(data[-1])

        return "MakePreProcessFreeFuction", None, {}


    @classmethod
    def MakePreProcess_KnowledgeGraph_P4_1_1(self, makeInfo):
        from ampligraph.utils import save_model, restore_model
        # return "MakePreProcessFreeFuction", None, {}

        model_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M2_1_2/"
        input_path1 = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{makeInfo['parameter']['RPM_path'][0]}/"
        input_path2 = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{makeInfo['parameter']['RPM_path'][1]}/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{makeInfo['parameter']['RPM_path'][2]}/"
        for trainDate, validDate in makeInfo['parameter']['validDate']:
            trainDate = datetime.datetime.strptime(trainDate, "%Y-%m-%d")
            validDate = datetime.datetime.strptime(validDate, "%Y-%m-%d")
            print()
            print(trainDate, validDate)

            # 獲取訓練檔期模型
            if trainDate <= datetime.datetime.strptime('2022-08-10', '%Y-%m-%d'):
                print(f"use model:{trainDate.strftime('%Y%m%d')}_ComplExModel_KG_FocusE.pkl")
                kgModel = restore_model(model_name_path=model_path + f"{trainDate.strftime('%Y%m%d')}_ComplExModel_KG_FocusE.pkl")
            else:
                print(f"use model:20220810_ComplExModel_KG_FocusE.pkl")
                kgModel = restore_model(model_name_path=model_path + f"20220601_ComplExModel_KG_FocusE.pkl")
            ent_to_idx = kgModel.ent_to_idx
            get_ent_idxs_func_ = np.vectorize(ent_to_idx.get)
            ent_list = kgModel.trained_model_params[0]  # Entity Embedding
            rel_to_idx = kgModel.rel_to_idx
            get_rel_idxs_func_ = np.vectorize(rel_to_idx.get)
            rel_list = kgModel.trained_model_params[1]  # Relation Embedding

            if validDate < datetime.datetime.strptime('2021-09-08', "%Y-%m-%d"):
                Y_item = f'5222123_{validDate.strftime("%Y-%m-%d")}'
            else:
                Y_item = f'5680946_{validDate.strftime("%Y-%m-%d")}'

            dataDFLabel = pd.read_csv(input_path1 + f"xgbPredictLabel_{validDate.strftime('%Y%m%d')}.csv").set_index('id')
            dataDFProb = pd.read_csv(input_path2 + f"xgbPredict_{validDate.strftime('%Y%m%d')}.csv").set_index('id')
            df = pd.concat([dataDFLabel[['predict']], dataDFProb[['xgbProb']]], axis=1)
            df = df.reset_index()
            df.columns = ['id','predict_label','predict_prob']
            df['Y_item'] = Y_item
            print(df)

            graph_data = pd.read_csv(f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/R2_1_2/KGRDFN_eventTagdf_{validDate.strftime('%Y%m%d')}.csv").drop('Unnamed: 0', axis=1)
            graph_data.columns = ['entity1','relation','entity2','num']
            graph = self.getGraph(graph_data)
            graph = self.getGraphInit(graph, Y_item, ['是'], type='groupby', new_str='是')
            graph = self.getGraphInit(graph, Y_item, ['產生', '外觀'], type='leftjoin', new_str='外觀')
            taglist = graph[Y_item]['是'] + graph[Y_item]['外觀']
            print(taglist)

            data = []
            for _ in tqdm(df.to_numpy()):
                id_ = _[0]
                predict_label_ = _[1]
                predict_prob_ = _[2]
                Y_item_ = _[3]
                data.append([id_, predict_label_, predict_prob_, Y_item_])
                for tag_, num_ in taglist:
                    relation_embedding = rel_list[get_rel_idxs_func_('購買')]
                    entity_embedding_tag = ent_list[get_ent_idxs_func_(tag_)]

                    if id_ in ent_to_idx.keys():
                        entity_embedding_UserID = ent_list[get_ent_idxs_func_(id_)]
                    else:
                        entity_embedding_UserID = [0 for _ in range(len(ent_list[get_ent_idxs_func_(Y_item)]))]

                    score = self.complexScore(
                        entity_embedding_UserID,
                        relation_embedding,
                        entity_embedding_tag,
                        int(len(ent_list[get_ent_idxs_func_(Y_item)]) / 2)
                    )
                    data[-1].append(score)

            tag_cols = []
            for tag_, num_ in taglist:
                tag_cols.append(tag_)
            df = pd.DataFrame(data, columns=['id','購買','購買概率','預測檔期'] + tag_cols)

            for col_ in tag_cols:
                scoreList = df[col_].tolist()
                score_rank = ss.rankdata(scoreList)
                score_percent = score_rank / len(score_rank)
                score_percent = np.round(np.array(score_percent), decimals=4)
                df[col_] = score_percent
            df.id = df.id.astype(str)
            df.to_csv(output_path + f'FashionBoxPredictInformation.csv')
            print(df)

        return "MakePreProcessFreeFuction", None, {}

    @classmethod
    def MakePreProcess_KnowledgeGraph_P99_99_99(self, makeInfo):
        # return "MakePreProcessFreeFuction", None

        input_file = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_8/xgbTraindf_20220810.csv"
        dataDF = pd.read_csv(input_file)
        dataDF['col_160'] = (dataDF['col_160'] - dataDF['col_160'].min())/(dataDF['col_160'].max() - dataDF['col_160'].min())
        col_list = [f'tag{str(_).zfill(3)}' for _ in range(9)] + ['col_160']
        # ['tag000', 'tag004']
        # {
        #     'tag000': 'freePoint',
        #     'tag004': 'questHappyDay'
        # }
        print(dataDF[col_list])
        for col_ in col_list:
            if col_ not in ['tag000','tag004','col_160']: continue
            print(col_)
            posHisScoreArr = dataDF.loc[dataDF.Y == 1, col_].reset_index(drop=True).tolist()
            posHisDensity = kde.gaussian_kde(posHisScoreArr)
            pos_his_x = np.linspace(0, 1, 100)
            pos_his_y = posHisDensity(pos_his_x)

            negHisScoreArr = dataDF.loc[dataDF.Y == 0, col_].reset_index(drop=True).tolist()
            negHisDensity = kde.gaussian_kde(negHisScoreArr)
            neg_his_x = np.linspace(0, 1, 100)
            neg_his_y = negHisDensity(neg_his_x)

            plt.plot(pos_his_x, pos_his_y)
            plt.plot(neg_his_x, neg_his_y)
            plt.title("")
            plt.show()
            plt.hist([posHisScoreArr, negHisScoreArr], bins=20, density=True, label=['Buy', 'NoBuy'])
            plt.title("")
            plt.show()

        return "MakePreProcessFreeFuction", None, {}
