from sklearn import decomposition
import numpy as np
import package.common.analysis.norm as norm
import datetime as dt
import json
# 跑PCA專用
def pca_run(rawData, n_comp=1):
    pca = decomposition.PCA(n_components=n_comp)
    result = pca.fit_transform(rawData)
    sign = np.sign(np.sum(pca.components_))
    return norm.Norm(result * sign), sign * pca.components_


def pca_writeSQL(pgsql, modelName, pcaName, data, cols, date ,pcdim = 1):
    tmp, pcaTemp = pca_run(data[cols], pcdim)
    pcaTemp = json.dumps(np.round(pcaTemp, 4).tolist())
    for i in range(pcdim):
        data[f'{pcaName}{i+1}'] = tmp[:,i].copy()
    sqlstring = f"INSERT INTO result_rua.param_result VALUES ('{modelName}','{pcaTemp}','{'pca'}_{pcaName}'," \
                f"'{date}','{dt.datetime.now().strftime('%Y%m%d %T')}');"

    pgsql.executeSQL(sqlstring)
    sqlstring = f"INSERT INTO result_rua.param_result VALUES ('{modelName}','{json.dumps(cols)}','{'pcaCols'}_{pcaName}'," \
                f"'{date}','{dt.datetime.now().strftime('%Y%m%d %T')}');"
    pgsql.executeSQL(sqlstring)

def pca_readSQL(pgsql, modelName, pcaName):

    sqlstring = f"SELECT metadata FROM result_rua.param_result WHERE modelname = '{modelName}' AND modeltype = 'pcaCols_{pcaName}' " \
                f"ORDER BY createtime DESC LIMIT 1;"
    cols = pgsql.searchSQL(sqlstring)

    sqlstring = f"SELECT metadata FROM result_rua.param_result WHERE modelname = '{modelName}' AND modeltype = 'pca_{pcaName}' " \
                f"ORDER BY createtime DESC LIMIT 1;"
    content = pgsql.searchSQL(sqlstring)

    return json.loads(cols['metadata'].iloc[0]) ,  np.array(json.loads(content['metadata'].iloc[0]))

def pca_use(pcaContentDict, pcaColDict, tmp):
    pcaNames = pcaColDict.keys()

    for i in pcaNames:
        p = pcaContentDict[i]
        pcaCols = pcaColDict[i]
        q = tmp[pcaCols].copy()
        pcaData = q.dot(p.T)
        for j in range(p.shape[0]):
            name = f"{i}{j + 1}"
            tmp[name] = norm.Norm(pcaData[j].copy())
        tmp.drop(pcaCols, axis=1, inplace=True)
    return tmp