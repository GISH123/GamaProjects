from sklearn.metrics import confusion_matrix
import xgboost as xgb
import numpy as np
from tqdm import tqdm
from collections import defaultdict
import os
import pandas as pd
import datetime
from dotenv import load_dotenv
from package.common.database.hiveCtrl import HiveCtrl
from package.common.database.hdfsCtrl import HDFSCtrl

class UseModel_KnowledgeGraphOLD():
    @classmethod
    def MakeUseModel_KnowledgeGraph_M0_0_1(self, makeInfo):
        return "MakeUseModelFreeFuction", None

    @classmethod
    def MakeUseModel_KnowledgeGraph_M2_0_1(self, makeInfo):
        # return "MakeUseModelFreeFuction", None
        read_equ = self.read_equipment_revised_ALL(
            Path='D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/2021_2022equipment_revised_ALL.csv')
        dt_list = read_equ.Date.unique()
        dt = datetime.datetime.strptime(dt_list[-1], "%Y-%m-%d")
        # print(dt_list)

        # FashionBox 圖譜
        kgFashionBox = pd.read_csv(
            f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P2_0_1/{dt.strftime('%Y%m%d')}_kgFashionBox.csv").drop(
            'Unnamed: 0', axis=1)
        kgFashionBoxNumpy = kgFashionBox.to_numpy()
        # print(kgFashionBox)

        # User 購買 FashionBox 檔期資料
        kgUserIdPurchaseFashionBox = pd.read_csv(
            f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P2_0_1/{dt.strftime('%Y%m%d')}_kgUserIdPurchaseFashionBox.csv").drop(
            'Unnamed: 0', axis=1)
        # print(kgUserIdPurchaseFashionBox)
        event = kgUserIdPurchaseFashionBox.Entity2.unique()
        # print(event)

        ################################################################################################################
        # 檔期分數統計
        # Fashion - Item & Item - concept
        fashionBoxGraph = defaultdict(set)
        for _ in kgFashionBoxNumpy:
            entity1 = _[0]
            entity2 = _[1]
            fashionBoxGraph[entity1].add(entity2)
        ''' {
            '1005619': {'柔和色系', '貴族風格', '大師標籤'}, 
            '5222123_2020-12-30': {
                '1005319', '1005276', '1103171', '1073335', '1053378', '1073325', '1005275', 
                '1702883', '1051584', '1050514', '1012679', '1702901', '1053377', '1073324'
            }
        }'''
        # print(fashionBoxGraph)

        # FashionBox - Tag score
        eventScore = defaultdict(lambda: defaultdict(lambda: 0))
        eventTagList = defaultdict(list)
        for ev_ in event:
            for item_ in fashionBoxGraph[ev_]:
                score_list = list(fashionBoxGraph[item_])
                eventTagList[ev_].extend(score_list)
        ''' {
            '5222123_2020-12-30': [
                    '柔和色系', '可愛風格', '動物風格', '可愛風格', '螢光系', '柔和色系', '可愛風格', 
                    '柔和色系', '可愛風格', '可愛風格', '螢光系', '可愛風格', '螢光系', '可愛風格', '螢光系', '不擋住名字', 
                    '可愛風格', '螢光系', '柔和色系', '可愛風格', '柔和色系', '可愛風格', '可愛風格', '不擋身/腿', '不擋住名字', 
                    '柔和色系', '可愛風格', '可愛風格', '螢光系', '可愛風格', '螢光系']
            }
        }'''
        # print(eventTagList)

        for ev_ in eventTagList.keys():
            for item_ in eventTagList[ev_]:
                eventScore[ev_][item_] += 1
        '''
            {'5222123_2020-12-30': {'柔和色系': 6, '可愛風格': 14, '動物風格': 1, '螢光系': 7, '不擋住名字': 2, '不擋身/腿': 1}}
        '''
        # print(eventScore)

        ################################################################################################################
        # 把登入資料貼上檔期分數
        # Tag - Score by event
        maindf = read_equ.loc[:, "Date":"渲染特效"].reset_index(drop=True)
        maindf.loc[:, "大師標籤":"渲染特效"] = 0
        maindf = maindf.drop_duplicates().reset_index(drop=True)
        boxIndex1 = maindf['Date'] < '2021-09-08'
        boxIndex2 = maindf['Date'] >= '2021-09-08'
        maindf.loc[boxIndex1, 'Date'] = '5222123_' + maindf.loc[boxIndex1, 'Date']
        maindf.loc[boxIndex2, 'Date'] = '5680946_' + maindf.loc[boxIndex2, 'Date']
        maindf = maindf.rename(columns={"Date": "FashionBoxEvent"})
        # print(maindf)
        """
        FashionBoxEvent             |   大師標籤 |  ...     |渲染特效
        5222123_2020 - 12 - 30      |      0    |          |    1
        """
        # Main
        maindf = pd.merge(
            maindf
            , kgUserIdPurchaseFashionBox.rename(
                columns={"Entity2": "FashionBoxEvent", "Entity1": "serviceaccountId"}).drop("Relation", axis=1)
            , on='FashionBoxEvent'
            , how='left'
        )
        '''
        FashionBoxEvent | Tag_Score |   serviceaccountId |   FashionBoxEvent 
        0       |   0     |    0000098765   | 5222123_2021-08-04
        '''
        # print(maindf)

        # user - score by FashionBox
        columns_ind = {}
        columns_list = list(maindf.columns)
        for ind_, value_ in enumerate(columns_list):
            columns_ind[value_] = ind_
        '''{
            'FashionBoxEvent': 0, '大師標籤': 1, 'IP合作': 2, '漂浮特效': 3, '暗色系': 4, '螢光系': 5, '柔和色系': 6, 
            '可愛風格': 7, '制服風格': 8, '貴族風格': 9, '動物風格': 10, '怪物風格': 11, '角色風格': 12, '自然風格': 13, 
            '重裝風格': 14, '運動風格': 15, '搞怪風格': 16, '不擋身/腿': 17, '不擋住名字': 18, '渲染特效': 19, 
            'serviceaccountId': 20, 'num': 21
        }'''
        # print(columns_ind)

        print('get maindf_score')
        data = []
        maindf_np = maindf.to_numpy()
        for _ in tqdm(maindf_np):
            fashionBoxEvent = _[0]
            serviceaccountid = _[-2]
            num_ = _[-1]
            data.append(_)

            for tag_, score_ in eventScore[fashionBoxEvent].items():
                ind_ = columns_ind[tag_]
                data[-1][ind_] = score_ * num_

        maindf_score = pd.DataFrame(data, columns=columns_list)
        '''
        FashionBoxEvent | Tag_Score     |   serviceaccountId    |   FashionBoxEvent 
        0               |   [0-999]     |    0000098765         | 5222123_2021-08-04
        '''
        # print(maindf_score)

        # 已登入資料為Base (用有購買時尚隨機箱的做篩選)
        print('get kgUserIdLogin')
        maindf_login = pd.DataFrame()
        userSet = set()
        for _ in tqdm(range(len(dt_list) - 1)):
            st_dt = datetime.datetime.strptime(dt_list[_], "%Y-%m-%d")
            FashionBoxEvent_ = '5222123_' + st_dt.strftime('%Y-%m-%d') if st_dt < datetime.datetime.strptime(
                '2021-09-08', "%Y-%m-%d") else '5680946_' + st_dt.strftime('%Y-%m-%d')
            userSet = userSet.union(set(
                maindf_score.loc[maindf_score['FashionBoxEvent'] == FashionBoxEvent_, 'serviceaccountId'].unique()))

            kgUserIdLogin = pd.read_csv(
                f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P2_0_1/{st_dt.strftime('%Y%m%d')}_kgUserIdLogin.csv").drop(
                'Unnamed: 0', axis=1)
            kgUserIdLogin['FashionBoxEvent'] = FashionBoxEvent_
            kgUserIdLogin = kgUserIdLogin.loc[kgUserIdLogin.service_account_id.isin(userSet)].reset_index(drop=True)

            maindf_login = pd.concat([maindf_login, kgUserIdLogin])
        maindf_login.columns = ['serviceaccountId', 'FashionBoxEvent']

        maindf_login_score = pd.merge(
            maindf_login,
            maindf_score,
            on=['FashionBoxEvent', 'serviceaccountId'],
            how='left'
        )
        print(maindf_login_score.loc[maindf_login_score['serviceaccountId'] == '0424818369'])
        """
        serviceaccountId    |   FashionBoxEvent         | 大師標籤  |   ...     |   不擋住名字   |   渲染特效    |   num
        0424818369          |   5222123_2020 - 12 - 30  | 0.0     |     ...    |    30.0      |     0.0     |   15.0
        """

        ################################################################################################################
        # 計算購買資料 by 檔期
        # user - purchase - statis
        print('statis topKPurchaseItemList top k...')
        kguseridpurchase = pd.DataFrame()
        for _ in tqdm(range(len(dt_list) - 1)):
            st_dt = datetime.datetime.strptime(dt_list[_], "%Y-%m-%d")
            df = pd.read_csv(
                f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P2_0_1/{st_dt.strftime('%Y%m%d')}_kguseridpurchase.csv").drop(
                'Unnamed: 0', axis=1)
            df = df.loc[df.Entity1.isin(userSet)].reset_index(drop=True)
            df['Entity2'] = df['Entity2'].astype(str)
            df['FashionBoxEvent'] = '5222123_' + st_dt.strftime('%Y-%m-%d') if st_dt < datetime.datetime.strptime(
                '2021-09-08', "%Y-%m-%d") else '5680946_' + st_dt.strftime('%Y-%m-%d')
            kguseridpurchase = pd.concat([kguseridpurchase, df])
        topKPurchaseItemList = kguseridpurchase[['Entity2', 'num']].groupby('Entity2').agg(
            {'num': sum}).sort_values(by='num', ascending=False).iloc[:128].reset_index().Entity2.unique()
        '''
        ['5060048' '5680946' '5060057' '5150040' '5680865' '5689003' '5743003'
         '5062500' '5680863' '5222123' '5060029' '5064300' '5680641' '5222138'
         '5680864' '5152053' '5532372' '5520002' '5680949' '5060025' '5062017'
         '5530880' '5520001' '5066100' '5060001' '5537000' '5062020' '5680734'
         '5060028' '5680643' '5860000' '5680642' '5192000' '5150173' '5152243'
         '5062026' '5130000' '5680542' '5069100' '5061100' '5064502' '5044005'
         '5076100' '5044003' '5680531' '5689000' '5064400' '5530776' '5062400'
         '1022048' '5689004' '5533142' '5044004' '5530803' '5680733' '5680902'
         '5062800' '5152301' '1082102' '5062019' '1072153' '1012208' '1012057'
         '5062021' '1002186' '5500005' '1102039' '5062402' '5152111' '5680646'
         '1032024' '5133000' '5151033' '5153013' '5060049' '5062802' '5240177'
         '5680644' '9110002' '5151032' '5064301' '1002665' '1112930' '1702524'
         '1012028' '1052137' '5010044' '5680645' '1012083' '5156000' '5152050'
         '5150043' '1092064' '5073000' '1053615' '1053616' '1102955' '5062503'
         '5538438' '1005583' '1112816' '5550000' '9112001' '1112006' '1005584'
         '5170000' '1802641' '5000909' '5860001' '1112015' '5064500' '1103305'
         '1703049' '1053640' '1005656' '5155000' '1112135' '1112013' '5000012'
         '1802460' '5252014' '1102954' '5680809' '1342069' '1802511' '1052201'
         '5000058' '1005456']
        '''
        print(topKPurchaseItemList)
        # return "MakeUseModelFreeFuction", None

        print('make kguserIdPurchaseTopK...')
        kguserIdPurchaseTopKPT = pd.DataFrame()
        for _ in tqdm(range(len(dt_list) - 1)):
            st_dt = datetime.datetime.strptime(dt_list[_], "%Y-%m-%d")
            df = pd.read_csv(
                f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P2_0_1/{st_dt.strftime('%Y%m%d')}_kguseridpurchase.csv").drop(
                'Unnamed: 0', axis=1)
            df = df.loc[df.Entity1.isin(userSet)].reset_index(drop=True)
            df['Entity2'] = df['Entity2'].astype(str)
            df['FashionBoxEvent'] = '5222123_' + st_dt.strftime('%Y-%m-%d') if st_dt < datetime.datetime.strptime(
                '2021-09-08', "%Y-%m-%d") else '5680946_' + st_dt.strftime('%Y-%m-%d')
            df = df.loc[df.Entity2.isin(topKPurchaseItemList), :].reset_index()
            df_pt = pd.pivot_table(
                df,
                index=['Entity1', 'FashionBoxEvent'],
                columns=['Entity2'],
                values='num',
                aggfunc=np.sum,
                fill_value=0
            ).reset_index().rename(columns={'Entity1': 'serviceaccountId'})
            kguserIdPurchaseTopKPT = pd.concat([kguserIdPurchaseTopKPT, df_pt])
        maindf_login_score = pd.merge(
            maindf_login_score,
            kguserIdPurchaseTopKPT,
            on=['FashionBoxEvent', 'serviceaccountId'],
            how='left'
        ).fillna(0)

        print(maindf_login_score.loc[maindf_login_score['serviceaccountId'] == '0424818369'])
        '''
        Entity2 | Tag_Score |   serviceaccountId |   FashionBoxEvent |    1002186 | ... |  1002598
        0       |   ...     |    0000098765   | 5222123_2021-08-04|        0   | ... |    1
        '''

        # 疊代每個檔期資料
        maindf = pd.concat([maindf_login_score[['FashionBoxEvent', 'serviceaccountId', 'num']],
                            maindf_login_score.drop(['FashionBoxEvent', 'serviceaccountId', 'num', 'index'],
                                                    axis=1)], axis=1)
        maindf.to_csv(
            F"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M2_0_1/{dt_list[-1]}_kgModelData.csv")
        return "MakeUseModelFreeFuction", None

    @classmethod
    def MakeUseModel_KnowledgeGraph_M3_0_1(self, makeInfo):
        # return "MakeUseModelFreeFuction", None

        fileName = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_0_1/2022-06-01_kgModelData.csv"
        dataDF = pd.read_csv(
            fileName
            # , nrows=1000000
        ).fillna(0)
        dataDF.loc[dataDF.Y > 0, 'Y'] = 1
        # print(dataDF)

        parm_raw = {
            'max_depth': 18  # < GridSearch's The Best Parameters
            , 'learning_rate': 0.1
            , 'n_estimators': 60  # < GridSearch's The Best Parameters
            , 'min_child_weight': 5  # < GridSearch's The Best Parameters
            , 'gamma': 0.5  # < GridSearch's The Best Parameters
            , 'subsample': .5
            , 'colsample_bytree': .5
            , 'objective': 'binary:logistic'
            , 'eta': 0.3
            , 'booster': 'gbtree'
            , 'eval_metric': 'logloss'
            , 'use_label_encoder': False
            , 'nthread': 4
            , 'scale_pos_weight': 1
            , 'seed': 27
            #     , 'silent': 0
        }

        try:
            score_dataframe_best = \
                pd.read_csv(
                    f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M3_0_1/score_dataframe_best.csv",
                    low_memory=False
                ).drop(['Unnamed: 0'], axis=1)
        except:
            col_name = ['max_depth', 'n_estimators', 'min_child_weight', 'gamma',
                        'event_dt', 'F1Score', 'Accuracy', 'Recall', 'Precision']
            score_dataframe_best = pd.DataFrame(columns=col_name)

        ####################################################################################################################
        dt_list = [_ for _ in dataDF.FashionBoxEvent.unique()]
        print(dt_list)
        for ind_ in tqdm(range(6, len(dt_list))):
            dt1 = dt_list[ind_ - 6]
            dt2 = dt_list[ind_]
            trainDF = dataDF.loc[(dataDF.FashionBoxEvent >= dt1) & (dataDF.FashionBoxEvent < dt2)].reset_index(
                drop=True).drop(['FashionBoxEvent', 'serviceaccountId'], axis=1)
            # trainDF = dataDF.loc[dataDF.FashionBoxEvent < dt2].reset_index(drop=True).drop(['FashionBoxEvent', 'serviceaccountId'], axis=1)
            # trainDF = self.reSampling(trainDF, BR=1)

            testDF = dataDF.loc[dataDF.FashionBoxEvent == dt2].reset_index(drop=True).drop(
                ['FashionBoxEvent', 'serviceaccountId'], axis=1)

            print()
            print(dt2, trainDF.shape, testDF.shape)
            score_dataframe_best = self.XgboostTrain(
                trainDF, testDF, [False for _ in range(testDF.shape[0])],
                dt2, score_dataframe_best,
                slient=False,
                **parm_raw
            )

            # return "MakeUseModelFreeFuction", None
            score_dataframe_best.to_csv(
                f'D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M3_0_1/score_dataframe_best.csv')
        print(score_dataframe_best)
        return "MakeUseModelFreeFuction", None

    @classmethod
    def MakeUseModel_KnowledgeGraph_M2_1_1(self, makeInfo):
        return "MakeUseModelFreeFuction", None
        import numpy as np
        import pandas as pd
        import tensorflow as tf
        import ampligraph
        from ampligraph.latent_features import TransE, ComplEx, HolE, DistMult, ConvE, ConvKB
        from ampligraph.utils import save_model, restore_model, create_tensorboard_visualizations
        from sklearn.utils import shuffle

        tf.logging.set_verbosity(tf.logging.ERROR)
        # sess = tf.Session(config=tf.ConfigProto(log_device_placement=True));
        # print(sess)
        # ampligraph.latent_features.set_entity_threshold(threshold=10000)
        pd.set_option('display.max_rows', 100)

        print('TensorFlow  version: {}'.format(tf.__version__))
        print('Ampligraph version: {}'.format(ampligraph.__version__))
        print()

        ####################################################################################################################
        # Read File
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P2_1_1/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M2_1_1/"
        date_str = '20220223'

        kgData = pd.read_csv(input_path + f"KGRDFN_traindf_{date_str}.csv").drop(['Unnamed: 0'], axis=1)
        kgData = shuffle(kgData)
        print(date_str)
        print(kgData.head(3))
        print(kgData.shape)

        # preparing data to train
        kgData.columns = ['Entity1', 'Relation', 'Entity2', 'num']
        kgData['Entity1'] = kgData['Entity1'].astype(str)
        kgData['Relation'] = kgData['Relation'].astype(str)
        kgData['Entity2'] = kgData['Entity2'].astype(str)
        kgData['num'] = kgData['num'].astype(int)
        print(kgData)
        X = kgData.loc[:, ["Entity1", "Relation", "Entity2"]].to_numpy()
        X_edge_values = np.array(kgData.loc[:, 'num'].tolist())

        # KG model training
        par_ = {
            'batches_count': 128, 'epochs': 150,
            'k': 64, 'eta': 20, 'loss': 'multiclass_nll',
            'optimizer': 'adam', 'optimizer_params': {'lr': 3e-3},
            # 'optimizer': 'sgd',         'optimizer_params': {'lr': 5e-3, 'end_lr': 1e-5, 'decay_cycle': 5, 'expand_factor':5 ,'decay_lr_rate': 3},
            'initializer': 'xavier', 'initializer_params': {'uniform': False},
            'regularizer': 'LP', 'regularizer_params': {'p': 3, 'lambda': 1e-2},
            'seed': 0,
            'verbose': True
        }

        model = ComplEx(
            batches_count=par_['batches_count'],
            epochs=par_['epochs'],
            k=par_['k'],
            eta=par_['eta'],
            loss=par_['loss'],
            optimizer=par_['optimizer'], optimizer_params=par_['optimizer_params'],
            initializer=par_['initializer'], initializer_params=par_['initializer_params'],
            regularizer=par_['regularizer'], regularizer_params=par_['regularizer_params'],
            seed=par_['seed'],
            verbose=par_['verbose']
        )
        print('my params:')
        print(model.optimizer_params)

        # training...
        print('training...')
        # X_train, X_valid = train_test_split_no_unseen(kgData.to_numpy(), test_size=10000)
        # model.fit(X_train, tensorboard_logs_path=output_path)
        # kgEvaluate(X_train, X_valid, model)

        # model.fit(adapt, tensorboard_logs_path=output_path)

        model.fit(X, focusE_numeric_edge_values=X_edge_values, tensorboard_logs_path=output_path)

        # save model...
        print('save model...')
        # save_model(model, model_name_path = output_path + f"{''.join(his_dt.split('/'))}_{modelName}_KG_FocusE.pkl")
        save_model(model, model_name_path = output_path + f"{date_str}_ComplExModel_KG_FocusE.pkl")
        create_tensorboard_visualizations(model, output_path)

        return "MakeUseModelFreeFuction", None

    @classmethod
    def MakeUseModel_KnowledgeGraph_M3_1_2(self, makeInfo):
        # return "MakeUseModelFreeFuction", None
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_2/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M3_1_2/"

        for modelDate_ in makeInfo['parameter']['usemodel']['modeldate']:
            modelDate = datetime.datetime.strptime(modelDate_, "%Y-%m-%d")
            dataDF = pd.read_csv(
                input_path + f"xgbTraindf_{modelDate.strftime('%Y%m%d')}.csv"
                # , nrows=10000
            ).drop('Unnamed: 0', axis=1)
            dataDF['data_set'] = pd.to_datetime(dataDF['data_set'])
            # print(dataDF)
            parm_raw = {
                'max_depth': 12  # < GridSearch's The Best Parameters
                , 'learning_rate': 0.1
                , 'n_estimators': 720  # < GridSearch's The Best Parameters
                , 'min_child_weight': 5  # < GridSearch's The Best Parameters
                , 'gamma': 0.5  # < GridSearch's The Best Parameters
                , 'subsample': .5
                , 'colsample_bytree': .5
                , 'objective': 'binary:logistic'
                , 'eta': 0.3
                , 'booster': 'gbtree'
                , 'eval_metric': 'logloss'
                , 'use_label_encoder': False
                , 'nthread': 4
                , 'scale_pos_weight': 1
                , 'seed': 27
                #     , 'silent': 0
            }

            try:
                score_dataframe_best = \
                    pd.read_csv(
                        output_path + f"score_dataframe_best.csv",
                        low_memory=False
                    ).drop(['Unnamed: 0'], axis=1)
            except:
                col_name = ['max_depth', 'n_estimators', 'min_child_weight', 'gamma',
                            'event_dt', 'F1Score', 'Accuracy', 'Recall', 'Precision']
                score_dataframe_best = pd.DataFrame(columns=col_name)

            for ind_ in tqdm(range(1)):
                trainDF = dataDF.loc[dataDF.data_set < modelDate].reset_index(drop=True)
                validDF = dataDF.loc[dataDF.data_set == modelDate].reset_index(drop=True)
                # print(trainDF.head())
                # print(validDF.head())

                trainDF = self.reSampling(trainDF, BR=1)
                trainDF = trainDF.drop(['id','data_set'], axis=1)
                validDF = validDF.drop(['id','data_set'], axis=1)

                print()
                print(modelDate, trainDF.shape, validDF.shape)
                score_dataframe_best = self.XgboostTrain(
                    trainDF, validDF, [False for _ in range(validDF.shape[0])],
                    modelDate.strftime('%Y%m%d'), score_dataframe_best,
                    slient=False,
                    **parm_raw
                )

                # return "MakeUseModelFreeFuction", None
                score_dataframe_best.to_csv(output_path + f'score_dataframe_best.csv')
            print(score_dataframe_best)

        return "MakeUseModelFreeFuction", None


    @classmethod
    def MakeUseModel_KnowledgeGraph_M3_1_1(self, makeInfo):
        # return "MakeUseModelFreeFuction", None
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_1/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M3_1_1/"
        hiveCtrl = self.getHiveCtrl()

        validDate_list = makeInfo['parameter']['preprocess']['validDate']
        for ind_ in range(1, len(validDate_list)):
            trainDate = datetime.datetime.strptime(validDate_list[ind_ - 1], '%Y-%m-%d')
            validDate = datetime.datetime.strptime(validDate_list[ind_], '%Y-%m-%d')
            print(trainDate, validDate)

            dataDF = pd.read_csv(
                input_path + f"xgbTraindf_{trainDate.strftime('%Y%m%d')}.csv"
                # , nrows=10000
            ).drop('Unnamed: 0', axis=1)
            dataDF['data_set'] = pd.to_datetime(dataDF['data_set'])
            # print(dataDF[['id','Y','data_set']])
            parm_raw = {
                'max_depth': 12  # < GridSearch's The Best Parameters
                , 'learning_rate': 0.1
                , 'n_estimators': 60  # < GridSearch's The Best Parameters
                , 'min_child_weight': 5  # < GridSearch's The Best Parameters
                , 'gamma': 0.5  # < GridSearch's The Best Parameters
                , 'subsample': .5
                , 'colsample_bytree': .5
                , 'objective': 'binary:logistic'
                , 'eta': 0.3
                , 'booster': 'gbtree'
                , 'eval_metric': 'logloss'
                , 'use_label_encoder': False
                , 'nthread': 4
                , 'scale_pos_weight': 1
                , 'seed': 27
                #     , 'silent': 0
            }

            try:
                score_dataframe_best = \
                    pd.read_csv(
                        output_path + f"score_dataframe_best.csv",
                        low_memory=False
                    ).drop(['Unnamed: 0'], axis=1)
            except:
                col_name = ['max_depth', 'n_estimators', 'min_child_weight', 'gamma',
                            'event_dt', 'F1Score', 'Accuracy', 'Recall', 'Precision']
                score_dataframe_best = pd.DataFrame(columns=col_name)

            for ind_ in tqdm(range(1)):
                trainDF = dataDF.loc[dataDF.data_set < validDate].reset_index(drop=True)
                validDF = dataDF.loc[dataDF.data_set == validDate].reset_index(drop=True)
                print()
                print(f'trainDate:{trainDate}, trainDF.shape:{trainDF.shape}, validDF.shape:{validDF.shape}')
                print(trainDF[['id','Y','data_set']].head())
                print(validDF[['id','Y','data_set']].head())

                trainDF = self.reSampling(trainDF, BR=1)
                # self.reSampling(validDF, BR=1)
                trainDF = trainDF.drop(['id','data_set'], axis=1)
                validDF = validDF.drop(['id','data_set'], axis=1)

                score_dataframe_best = self.XgboostTrain(
                    trainDF, validDF, [False for _ in range(validDF.shape[0])],
                    trainDate.strftime('%Y%m%d'), score_dataframe_best,
                    slient=False,
                    **parm_raw
                )

                # return "MakeUseModelFreeFuction", None
                score_dataframe_best.to_csv(output_path + f'score_dataframe_best.csv')
            print(score_dataframe_best)

        return "MakeUseModelFreeFuction", None

    @classmethod
    def MakeUseModel_KnowledgeGraph_M3_1_3(self, makeInfo):
        # return "MakeUseModelFreeFuction", None
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_3/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M3_1_3/"

        validDate_list = makeInfo['parameter']['usemodel']['trainDate']
        for ind_ in range(len(validDate_list)):
            trainDate = datetime.datetime.strptime(validDate_list[ind_], '%Y-%m-%d')
            # print(trainDate)

            dataDF = pd.read_csv(
                input_path + f"xgbTraindf_{trainDate.strftime('%Y%m%d')}.csv"
                # , nrows=10000
            )
            # print(pd.concat([dataDF.head(1),dataDF.tail(1)]))
            parm_raw = {
                'max_depth': 12  # < GridSearch's The Best Parameters
                , 'learning_rate': 0.1
                , 'n_estimators': 60  # < GridSearch's The Best Parameters
                , 'min_child_weight': 5  # < GridSearch's The Best Parameters
                , 'gamma': 0.5  # < GridSearch's The Best Parameters
                , 'subsample': .5
                , 'colsample_bytree': .5
                , 'objective': 'binary:logistic'
                , 'eta': 0.3
                , 'booster': 'gbtree'
                , 'eval_metric': 'logloss'
                , 'use_label_encoder': False
                , 'nthread': 4
                , 'scale_pos_weight': 1
                , 'seed': 27
                #     , 'silent': 0
            }

            try:
                score_dataframe_best = \
                    pd.read_csv(
                        output_path + f"score_dataframe_best.csv",
                        low_memory=False
                    ).drop(['Unnamed: 0'], axis=1)
            except:
                col_name = ['max_depth', 'n_estimators', 'min_child_weight', 'gamma',
                            'event_dt', 'F1Score', 'Accuracy', 'Recall', 'Precision']
                score_dataframe_best = pd.DataFrame(columns=col_name)

            for ind_ in tqdm(range(1)):
                trainDF = dataDF.loc[dataDF.data_set != 'valid'].reset_index(drop=True)
                validDF = dataDF.loc[dataDF.data_set == 'valid'].reset_index(drop=True)
                # print(trainDF.head())
                # print(validDF.head())
                del dataDF

                self.reSampling(trainDF, BR=1)
                self.reSampling(validDF, BR=1)
                trainDF = trainDF.drop(['id','data_set'], axis=1)
                validDF = validDF.drop(['id','data_set'], axis=1)

                # print()
                print(trainDF.shape, validDF.shape)
                score_dataframe_best = self.XgboostTrain(
                    trainDF, validDF, [False for _ in range(validDF.shape[0])],
                    trainDate.strftime('%Y%m%d'), score_dataframe_best,
                    slient=False,
                    **parm_raw
                )

                # return "MakeUseModelFreeFuction", None
                score_dataframe_best.to_csv(output_path + f'score_dataframe_best.csv')
            print(score_dataframe_best)

        return "MakeUseModelFreeFuction", None

    @classmethod
    def MakeUseModel_KnowledgeGraph_M3_1_4(self, makeInfo):
        import warnings
        warnings.filterwarnings('ignore')

        # return "MakeUseModelFreeFuction", None
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_4/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M3_1_4/"

        validDate_list = makeInfo['parameter']['usemodel']['trainDate']
        for ind_ in range(1, len(validDate_list)):
            trainDate = datetime.datetime.strptime(validDate_list[ind_ - 1], '%Y-%m-%d')
            validDate = datetime.datetime.strptime(validDate_list[ind_], '%Y-%m-%d')
            print(trainDate, validDate)

            dataDF = pd.read_csv(
                input_path + f"xgbTraindf_{validDate.strftime('%Y%m%d')}.csv"
                # , nrows=10000
            )

            try:
                score_dataframe_best = \
                    pd.read_csv(
                        output_path + f"score_dataframe_best.csv",
                        low_memory=False
                    ).drop(['Unnamed: 0'], axis=1)
            except:
                col_name = ['event_dt', 'score', 'F1Score', 'Accuracy', 'Recall', 'Precision']
                score_dataframe_best = pd.DataFrame(columns=col_name)

            filterScoreMax = accuracyMax = recallMax = precisionMax = f1ScoreMax = 0
            for score_ in tqdm(range(0, 500, 1)):
                filterScore = score_ / 10
                trainDF = dataDF

                trainDF['predict'] = 0
                trainDF.loc[trainDF['score'] > filterScore, 'predict'] = 1
                predic_y = trainDF[['predict']]
                test_y = trainDF[['Y']]

                tn, fp, fn, tp = confusion_matrix(test_y, predic_y).ravel()
                Accuracy = (tp + tn) / (tp + fp + fn + tn)
                Recall = tp / (tp + fn)
                Precision = 0 if np.isnan(tp / (tp + fp)) else tp / (tp + fp)
                F1Score = 0 if np.isnan(2 * Recall * Precision / (Recall + Precision)) else 2 * Recall * Precision / (
                        Recall + Precision)
                if f1ScoreMax < F1Score:
                    filterScoreMax = filterScore
                    f1ScoreMax = F1Score
                    accuracyMax = Accuracy
                    recallMax = Recall
                    precisionMax = Precision

            temp_df = pd.DataFrame(
                [[validDate.strftime('%Y%m%d'), filterScoreMax, round(f1ScoreMax, 4), round(accuracyMax, 4), round(recallMax, 4), round(precisionMax, 4)]],
                columns=score_dataframe_best.columns
            )
            print(f"Date:{validDate.strftime('%Y%m%d')}, Score:{filterScoreMax}, F1Score:{round(f1ScoreMax, 2)} Accuracy:{round(accuracyMax, 2)} Recall:{round(recallMax, 2)} Precision:{round(precisionMax, 2)}")
            score_dataframe_best = pd.concat([score_dataframe_best, temp_df])

            score_dataframe_best.to_csv(output_path + f'score_dataframe_best.csv')
            print(score_dataframe_best)

        return "MakeUseModelFreeFuction", None

    @classmethod
    def MakeUseModel_KnowledgeGraph_M3_1_5(self, makeInfo):
        # return "MakeUseModelFreeFuction", None
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_5/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M3_1_5/"

        validDate_list = makeInfo['parameter']['usemodel']['trainDate']
        for ind_ in range(1, len(validDate_list)):
            trainDate = datetime.datetime.strptime(validDate_list[ind_ - 1], '%Y-%m-%d')
            validDate = datetime.datetime.strptime(validDate_list[ind_], '%Y-%m-%d')
            print(trainDate, validDate)
            dataDF = pd.read_csv(
                input_path + f"xgbTraindf_{validDate.strftime('%Y%m%d')}.csv"
                # , nrows=10000
            )
            # print(dataDF[['data_set']])
            # print(pd.concat([dataDF.head(1),dataDF.tail(1)]))
            parm_raw = {
                'max_depth': 12  # < GridSearch's The Best Parameters
                , 'learning_rate': 0.1
                , 'n_estimators': 60  # < GridSearch's The Best Parameters
                , 'min_child_weight': 5  # < GridSearch's The Best Parameters
                , 'gamma': 0.5  # < GridSearch's The Best Parameters
                , 'subsample': .5
                , 'colsample_bytree': .5
                , 'objective': 'binary:logistic'
                , 'eta': 0.3
                , 'booster': 'gbtree'
                , 'eval_metric': 'logloss'
                , 'use_label_encoder': False
                , 'nthread': 4
                , 'scale_pos_weight': 1
                , 'seed': 27
                #     , 'silent': 0
            }

            try:
                score_dataframe_best = \
                    pd.read_csv(
                        output_path + f"score_dataframe_best.csv",
                        low_memory=False
                    ).drop(['Unnamed: 0'], axis=1)
            except:
                col_name = ['max_depth', 'n_estimators', 'min_child_weight', 'gamma',
                            'event_dt', 'F1Score', 'Accuracy', 'Recall', 'Precision']
                score_dataframe_best = pd.DataFrame(columns=col_name)

            for ind_ in tqdm(range(1)):
                trainDF = dataDF.loc[dataDF.data_set < validDate.strftime("%Y-%m-%d")].reset_index(drop=True)
                validDF = dataDF.loc[dataDF.data_set == validDate.strftime("%Y-%m-%d")].reset_index(drop=True)
                print(trainDF.shape, validDF.shape)
                del dataDF

                trainDF = self.reSampling(trainDF, BR=1)
                self.reSampling(validDF, BR=1)
                trainDF = trainDF.drop(['id','data_set'], axis=1)
                validDF = validDF.drop(['id','data_set'], axis=1)

                # print()
                print(trainDF.shape, validDF.shape)
                score_dataframe_best = self.XgboostTrain(
                    trainDF, validDF, [False for _ in range(validDF.shape[0])],
                    trainDate.strftime('%Y%m%d'), score_dataframe_best,
                    slient=False,
                    **parm_raw
                )

                # return "MakeUseModelFreeFuction", None
                score_dataframe_best.to_csv(output_path + f'score_dataframe_best.csv')
            print(score_dataframe_best)

        return "MakeUseModelFreeFuction", None

