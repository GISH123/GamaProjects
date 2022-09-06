import numpy as np
def Norm(arr):
    if arr.max() - arr.min() == 0:
        return arr
    return (arr - arr.min()) / (arr.max() - arr.min())


def logNorm(arr):
    return Norm(np.log2(arr + 1))


def DublogNorm(arr):
    return Norm(logNorm(abs(arr)) * np.sign(arr))


def tfNorm(arr, threshold=0):
    return (arr > threshold) * 1


def groupNorm(arr, grp, useLog=True):
    if useLog:
        arr.loc[arr < 0] = 0
        return groupNorm(np.log2(arr + 1), grp, False)
    maxTable = arr.groupby(grp).max()
    tmp = maxTable[grp].copy()
    tmp.reset_index(inplace=True, drop=True)
    tmp.loc[tmp <= 0] = 1
    return arr / tmp


