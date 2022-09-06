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
import matplotlib.pyplot as plt
from maple.KnowledgeGraph.modeling.old.UseModel_KnowledgeGraphOLD import UseModel_KnowledgeGraphOLD

class UseModel_KnowledgeGraph(UseModel_KnowledgeGraphOLD):
    @classmethod
    def getHiveCtrl(self):
        load_dotenv(dotenv_path="env/hive.env")
        hiveCtrl = HiveCtrl(
            host=os.getenv("HIVE_HOST")
            , port=int(os.getenv("HIVE_PORT"))
            , user=os.getenv("HIVE_USER")
            , password=os.getenv("HIVE_PASS")
            , database='default'
            , auth_mechanism='PLAIN'
        )
        return hiveCtrl

    @classmethod
    def reSampling(self, df, BR=0.33):
        # BR: balance rate, default 0.33
        negNum = df.loc[df['Y'] == 0].shape[0]
        posNum = df.loc[df['Y'] != 0].shape[0]
        print(f'posNum:{posNum}, negNum:{negNum}, rate:{posNum/negNum}')
        # 資料重抽樣
        sampleFrac = 1 if (posNum / negNum) / BR > 1 else (posNum / negNum) / BR
        train_df = pd.concat([df.loc[df['Y'] == 0].sample(frac=sampleFrac),
                              df.loc[df['Y'] != 0]], axis=0).reset_index(drop='index')
        return train_df

    @classmethod
    def XgboostTrain(self, train, test, zero_inds, event_dt, score_df, slient=False, type='classifier', **parms):
        # train_df
        train_x = train.drop(['Y'], axis=1)
        train_y = train['Y']

        # test_df
        test_x = test.drop(['Y'], axis=1)
        test_y = test['Y']

        if type == 'classifier':
            print(f'xgb type:{type}')
            xg_cate_r = xgb.XGBClassifier(
                max_depth=parms['max_depth']  # < GridSearch's The Best Parameters
                , learning_rate=parms['learning_rate']
                , n_estimators=parms['n_estimators']  # < GridSearch's The Best Parameters
                , min_child_weight=parms['min_child_weight']  # < GridSearch's The Best Parameters
                , gamma=parms['gamma']  # < GridSearch's The Best Parameters
                , subsample=parms['subsample']
                , colsample_bytree=parms['colsample_bytree']
                , objective=parms['objective']
                , booster=parms['booster']
                , eval_metric=parms['eval_metric']
                , use_label_encoder=parms['use_label_encoder']
                , nthread=parms['nthread']
                , scale_pos_weight=parms['scale_pos_weight']
                , seed=parms['seed']
                #     , silent = 0
            )

            xg_cate_r.fit(train_x, train_y)

            predic_y = xg_cate_r.predict(test_x)
            predic_y[zero_inds] = 0
            print(predic_y)

            tn, fp, fn, tp = confusion_matrix(test_y, predic_y).ravel()
            Accuracy = (tp + tn) / (tp + fp + fn + tn)
            Recall = tp / (tp + fn)
            Precision = 0 if np.isnan(tp / (tp + fp)) else tp / (tp + fp)
            F1Score = 0 if np.isnan(2 * Recall * Precision / (Recall + Precision)) else 2 * Recall * Precision / (
                    Recall + Precision)

            temp_df = pd.DataFrame(
                [[
                    parms['max_depth'], parms['n_estimators'], parms['min_child_weight'], parms['gamma'],
                    event_dt, F1Score, Accuracy, Recall, Precision
                ]],
                columns=score_df.columns)
            score_df = pd.concat([score_df, temp_df])
            if slient is False:
                print(
                    f"F1Score:{round(F1Score, 2)} Accuracy:{round(Accuracy, 2)} Recall:{round(Recall, 2)} Precision:{round(Precision, 2)}")

            # plot_importance(xg_cate_r)
            # plt.show()
            return score_df, predic_y

        elif type == 'regressor':
            print(f'xgb type:{type}')
            xg_cate_r = xgb.XGBRegressor(
                max_depth=parms['max_depth']  # < GridSearch's The Best Parameters
                , learning_rate=parms['learning_rate']
                , n_estimators=parms['n_estimators']  # < GridSearch's The Best Parameters
                , min_child_weight=parms['min_child_weight']  # < GridSearch's The Best Parameters
                , gamma=parms['gamma']  # < GridSearch's The Best Parameters
                , subsample=parms['subsample']
                , colsample_bytree=parms['colsample_bytree']
                , objective=parms['objective']
                , booster=parms['booster']
                , eval_metric=parms['eval_metric']
                , use_label_encoder=parms['use_label_encoder']
                , nthread=parms['nthread']
                , scale_pos_weight=parms['scale_pos_weight']
                , seed=parms['seed']
                #     , silent = 0
            )

            xg_cate_r.fit(train_x, train_y)

            predic_y = xg_cate_r.predict(test_x)
            predic_y[zero_inds] = 0
            print(predic_y)

            return score_df, predic_y


    @classmethod
    def read_equipment_revised_ALL(self, Path):
        read_equ = pd.read_csv(Path)
        read_equ = read_equ.drop(['Unnamed: 0', 'KR Name', 'Unnamed: 5'], axis=1)
        read_equ = read_equ.fillna(0)

        read_equ['event'] = pd.to_datetime(read_equ['Date']).dt.strftime('%Y/%m/%d')
        read_equ['Date'] = pd.to_datetime(read_equ['Date']).dt.strftime('%Y-%m-%d')
        read_equ = read_equ.reset_index(drop=True)

        return read_equ

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

    @classmethod
    def MakeUseModel_KnowledgeGraph_M0_1_1(self, makeInfo):
        return "MakeUseModelFreeFuction", None, {}

    @classmethod
    def MakeUseModel_KnowledgeGraph_M1_1_1(self, makeInfo):
        return "MakeUseModelFreeFuction", None ,{}

    @classmethod
    def MakeUseModel_KnowledgeGraph_M2_1_1(self, makeInfo):
        return "MakeUseModelFreeFuction", None ,{}
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

        return "MakeUseModelFreeFuction", None , {}

    @classmethod
    def MakeUseModel_KnowledgeGraph_M2_1_2(self, makeInfo):
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
            'batches_count': 128, 'epochs': 300,
            'k': 32, 'eta': 20, 'loss': 'multiclass_nll',
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
    def MakeUseModel_KnowledgeGraph_M3_1_6(self, makeInfo):
        # return "MakeUseModelFreeFuction", None
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_6/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M3_1_6/"

        for trainDate, validDate in makeInfo['parameter']['validDate']:
            trainDate = datetime.datetime.strptime(trainDate, "%Y-%m-%d")
            validDate = datetime.datetime.strptime(validDate, "%Y-%m-%d")
            print()
            print(trainDate, validDate)

            dataDF = pd.read_csv(
                input_path + f"xgbTraindf_{validDate.strftime('%Y%m%d')}.csv"
                # , nrows=10000
            )
            print(dataDF[['data_set']])
            # print(pd.concat([dataDF.head(1),dataDF.tail(1)]))
            parm_raw = {
                'max_depth': 12  # < GridSearch's The Best Parameters
                , 'learning_rate': 0.1
                , 'n_estimators': 120  # < GridSearch's The Best Parameters
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
                outputDF = dataDF.loc[dataDF.data_set == validDate.strftime("%Y-%m-%d"), ['id']].reset_index(
                    drop=True)
                print(trainDF.shape, validDF.shape)
                del dataDF

                self.reSampling(trainDF, BR=1)
                self.reSampling(validDF, BR=1)
                trainDF = trainDF.drop(['id','data_set'], axis=1)
                validDF = validDF.drop(['id','data_set'], axis=1)

                # print()
                print(trainDF.shape, validDF.shape)
                score_dataframe_best, predic_y = self.XgboostTrain(
                    trainDF, validDF, [False for _ in range(validDF.shape[0])],
                    trainDate.strftime('%Y%m%d'), score_dataframe_best,
                    slient=False,
                    type='classifier',
                    **parm_raw
                )

                # return "MakeUseModelFreeFuction", None
                score_dataframe_best.to_csv(output_path + f'score_dataframe_best.csv')
                outputDF['predict'] = predic_y
                outputDF[['id', 'predict']].to_csv(output_path + f"xgbPredictLabel_{validDate.strftime('%Y%m%d')}.csv")
            print(score_dataframe_best)

        return "MakeUseModelFreeFuction", None

    @classmethod
    def MakeUseModel_KnowledgeGraph_M3_1_7(self, makeInfo):
        # return "MakeUseModelFreeFuction", None
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/P3_1_6/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/M3_1_7/"

        for trainDate, validDate in makeInfo['parameter']['validDate']:
            trainDate = datetime.datetime.strptime(trainDate, "%Y-%m-%d")
            validDate = datetime.datetime.strptime(validDate, "%Y-%m-%d")
            print()
            print(trainDate, validDate)
            dataDF = pd.read_csv(
                input_path + f"xgbTraindf_{validDate.strftime('%Y%m%d')}.csv"
                # , nrows=10000
            )
            print(dataDF[['data_set']])
            # print(pd.concat([dataDF.head(1),dataDF.tail(1)]))
            parm_raw = {
                'max_depth': 12  # < GridSearch's The Best Parameters
                , 'learning_rate': 0.1
                , 'n_estimators': 120  # < GridSearch's The Best Parameters
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
                outputDF = dataDF.loc[dataDF.data_set == validDate.strftime("%Y-%m-%d"),['id','Y']].reset_index(drop=True)
                print(trainDF.shape, validDF.shape)
                del dataDF

                self.reSampling(trainDF, BR=1)
                self.reSampling(validDF, BR=1)
                trainDF = trainDF.drop(['id','data_set'], axis=1)
                validDF = validDF.drop(['id','data_set'], axis=1)

                # print()
                print(trainDF.shape, validDF.shape)
                score_dataframe_best, predic_y = self.XgboostTrain(
                    trainDF, validDF, [False for _ in range(validDF.shape[0])],
                    trainDate.strftime('%Y%m%d'), score_dataframe_best,
                    slient=False,
                    type='regressor',
                    **parm_raw
                )

                outputDF['xgbProb'] = predic_y
                outputDF.to_csv(output_path + f"xgbPredict_{validDate.strftime('%Y%m%d')}.csv")
            print(score_dataframe_best)

        return "MakeUseModelFreeFuction", None

    @classmethod
    def MakeUseModel_KnowledgeGraph_M3_1_8(self, makeInfo):
        # return "MakeUseModelFreeFuction", None, {}
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{makeInfo['parameter']['RPM_path'][1]}/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{makeInfo['parameter']['RPM_path'][2]}/"
        print(input_path)
        for trainDate, validDate in makeInfo['parameter']['validDate']:
            trainDate = datetime.datetime.strptime(trainDate, "%Y-%m-%d")
            validDate = datetime.datetime.strptime(validDate, "%Y-%m-%d")
            print()
            print(trainDate, validDate)

            dataDF = pd.read_csv(
                input_path + f"xgbTraindf_{validDate.strftime('%Y%m%d')}.csv"
                # , nrows=10000
            )
            print(dataDF[['data_set']])
            # print(pd.concat([dataDF.head(1),dataDF.tail(1)]))
            parm_raw = {
                'max_depth': 12  # < GridSearch's The Best Parameters
                , 'learning_rate': 0.1
                , 'n_estimators': 120  # < GridSearch's The Best Parameters
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
                outputDF = dataDF.loc[dataDF.data_set == validDate.strftime("%Y-%m-%d"), ['id']].reset_index(
                    drop=True)
                print(trainDF.shape, validDF.shape)
                del dataDF

                trainDF = self.reSampling(trainDF, BR=0.5)
                self.reSampling(validDF, BR=1)
                trainDF = trainDF.drop(['id','data_set'], axis=1)
                validDF = validDF.drop(['id','data_set'], axis=1)

                # print()
                print(trainDF.shape, validDF.shape)
                score_dataframe_best, predic_y = self.XgboostTrain(
                    trainDF, validDF, [False for _ in range(validDF.shape[0])],
                    trainDate.strftime('%Y%m%d'), score_dataframe_best,
                    slient=False,
                    type='classifier',
                    **parm_raw
                )

                score_dataframe_best.to_csv(output_path + f'score_dataframe_best.csv')
                outputDF['predict'] = predic_y
                outputDF[['id', 'predict']].to_csv(output_path + f"xgbPredictLabel_{validDate.strftime('%Y%m%d')}.csv")
            print(score_dataframe_best)

        return "MakeUseModelFreeFuction", None, {}

    @classmethod
    def MakeUseModel_KnowledgeGraph_M3_1_9(self, makeInfo):
        # return "MakeUseModelFreeFuction", None
        input_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{makeInfo['parameter']['RPM_path'][1]}/"
        output_path = f"D:\Git\machinelearningsystem/maple/KnowledgeGraph/file/ModelData/{makeInfo['parameter']['RPM_path'][2]}/"

        for trainDate, validDate in makeInfo['parameter']['validDate']:
            trainDate = datetime.datetime.strptime(trainDate, "%Y-%m-%d")
            validDate = datetime.datetime.strptime(validDate, "%Y-%m-%d")
            print()
            print(trainDate, validDate)
            dataDF = pd.read_csv(
                input_path + f"xgbTraindf_{validDate.strftime('%Y%m%d')}.csv"
                # , nrows=10000
            )
            print(dataDF[['data_set']])
            # print(pd.concat([dataDF.head(1),dataDF.tail(1)]))
            parm_raw = {
                'max_depth': 12  # < GridSearch's The Best Parameters
                , 'learning_rate': 0.1
                , 'n_estimators': 120  # < GridSearch's The Best Parameters
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
                outputDF = dataDF.loc[dataDF.data_set == validDate.strftime("%Y-%m-%d"),['id','Y']].reset_index(drop=True)
                print(trainDF.shape, validDF.shape)
                del dataDF

                trainDF = self.reSampling(trainDF, BR=0.5)
                # self.reSampling(trainDF, BR=0.8)
                self.reSampling(validDF, BR=1)
                trainDF = trainDF.drop(['id','data_set'], axis=1)
                validDF = validDF.drop(['id','data_set'], axis=1)

                # print()
                print(trainDF.shape, validDF.shape)
                score_dataframe_best, predic_y = self.XgboostTrain(
                    trainDF, validDF, [False for _ in range(validDF.shape[0])],
                    trainDate.strftime('%Y%m%d'), score_dataframe_best,
                    slient=False,
                    type='regressor',
                    **parm_raw
                )

                outputDF['xgbProb'] = predic_y
                outputDF.to_csv(output_path + f"xgbPredict_{validDate.strftime('%Y%m%d')}.csv")
            print(score_dataframe_best)

        return "MakeUseModelFreeFuction", None, {}


    @classmethod
    def MakeUseModel_KnowledgeGraph_M4_1_1(self, makeInfo):
        return "MakeUseModelFreeFuction", None, {}

    @classmethod
    def MakeUseModel_KnowledgeGraph_M99_99_99(self, makeInfo):
        return "MakeUseModelFreeFuction", None, {}
