# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
import re


def createDict(data, thresPer=0.25):
    # 列出所有的字、並去除英數
    wordALL = "".join(data)
    remove_chars = '[A-Za-z0-9]'
    wordALL = re.sub(remove_chars, '', wordALL)

    # 建立字典
    wordDict = {}
    for word in wordALL:
        if word not in wordDict.keys():
            wordDict[word] = wordALL.count(word)

    # 去除次數過少的字
    d = wordDict.copy()
    wordCount = sum(wordDict.values())
    cumWords = 0
    for w in sorted(d, key=d.get):
        cumWords += d[w]
        if cumWords < thresPer * wordCount:
            wordDict.pop(w)
        else:
            break
    return (wordDict)


# calculate word entropy
def calEntropy(s):
    s = np.array(list(s))
    counts = np.unique(s, return_counts=True)[1]
    freq = counts / counts.sum()
    return -(np.log2(freq) * freq).sum()


def wordListEntropy(word, dictName, MAXlength=12, byDict=True):
    if byDict:
        wordData = "".join(dictName.keys())
    else:
        wordData = dictName
    tmp = "[" + wordData + "]"

    word = word.str.replace(tmp, '.')
    word = word.str.rjust(MAXlength, '=')
    return [calEntropy(s) for s in word]
