from pyspark import SparkContext, StorageLevel, SparkConf
import os
import sys
from time import time
import csv
import collections
import math
from itertools import combinations
import copy
from functools import reduce

bucketNumber = 100000

os.environ['PYSPARK_PYTHON'] = '/usr/bin/python3'
os.environ['PYSPARK_DRIVER_PYTHON'] = '/usr/bin/python3'

def writeToCSV(dataRDD, output_csv):
    with open(output_csv, 'w') as f:
        writer = csv.writer(f, quoting=csv.QUOTE_NONE, escapechar='\\')
        writer.writerow(["DATE-CUSTOMER_ID", "PRODUCT_ID"])
        for row in dataRDD:
            writer.writerow(row)

def genThresh(chunk, support, totalLength):
    basketList = copy.deepcopy(list(chunk))
    return math.ceil(support*len(list(basketList))/totalLength), basketList

def hashFunc(combo):
    mapValue = sum(map(lambda x: hash(x), list(combo)))
    return mapValue%bucketNumber

def generateSingles(chunk, supportThresh):
    hashmap = {}
    singletons = []
    bitmap = [0]*bucketNumber
    for basket in chunk:
        for item in basket:
            if item not in hashmap:
                hashmap[item] = 1
            else:
                hashmap[item] +=1
        for pair in combinations(basket, 2):
            bucketNo = hashFunc(pair)
            bitmap[bucketNo] +=1
    for item in hashmap:
        if hashmap[item] >= supportThresh:
            singletons.append(item)
    for i in range(0, len(bitmap)):
        if bitmap[i] >= supportThresh:
            bitmap[i] == 1
        else:
            bitmap[i] == 0
    return singletons, bitmap

def wrapper (freq_singletins):
    return [tuple(item.split(",")) for item in freq_singletins]

def shrinkBasket(basket, freq_singletons):
    return sorted(list(set(basket).intersection(set(freq_singletons))))

def check_bitMap(pair, bitmap):
    return bitmap[hashFunc(pair)]

def cmp(pair1, pair2):
    return True if pair1[:-1] == pair2[:-1] else False

def generateCandidateList(combo_list):
    if combo_list is not None and len(combo_list) > 0:
        size = len(combo_list[0])
        permutation_list = list()
        for index, front_pair in enumerate(combo_list[:-1]):
            for back_pair in combo_list[index + 1:]:
                if cmp(front_pair, back_pair):
                    combination = tuple(sorted(list(set(front_pair).union(set(back_pair)))))
                    temp_pair = list()
                    for pair in combinations(combination, size):
                        temp_pair.append(pair)
                    if set(temp_pair).issubset(set(combo_list)):
                        permutation_list.append(combination)
                else:
                    break

        return permutation_list

def PCY_algo(chunk, support, totalLength):
    supportThresh, chunk= genThresh(chunk, support, totalLength)
    chunk = list(chunk)
    result = []
    allCandidates = collections.defaultdict(list)
    freq_singletons,bitmap = generateSingles(chunk, supportThresh)

    index =1
    allCandidates[str(index)] = wrapper(freq_singletons)
    candidateList = freq_singletons

    while (None is not candidateList and len(candidateList) > 0):
        index +=1
        possibleCandidatesDict = collections.defaultdict(list)
        for basket in chunk:
            basket = shrinkBasket(basket, freq_singletons)
            if len(basket) >=index:
                if index ==2:
                    for pair in combinations(basket, index):
                        if check_bitMap(pair, bitmap):
                            possibleCandidatesDict[pair].append(1)
                if index >=3:
                    for item in candidateList:
                        if set(item).issubset(set(basket)):
                            possibleCandidatesDict[item].append(1)
        realCandidates = dict(filter(lambda x: len(x[1])>= supportThresh, possibleCandidatesDict.items()))
        candidateList = generateCandidateList(sorted(list(realCandidates.keys())))
        if len(realCandidates) == 0:
            break
        allCandidates[str(index)] = list(realCandidates.keys())

    yield reduce(lambda x, y: x + y, allCandidates.values())

def PCY_SecondPass(basket, candidateItems):
    frequentItems = collections.defaultdict(list)
    for items in candidateItems:
        if set(items).issubset(set(basket)):
            frequentItems[items].append(1)

    yield [tuple((key, sum(value))) for key, value in frequentItems.items()]

def SON_algo(basketRDD, support, totalLength):
    candidateItems = basketRDD.mapPartitions(lambda chunk: PCY_algo(chunk, support, totalLength))\
                                .flatMap(lambda pairs: pairs).distinct() \
                                .sortBy(lambda pairs: (len(pairs), pairs)).collect()


    frequentItems = basketRDD.flatMap(lambda basket: PCY_SecondPass(basket, candidateItems))\
                                .flatMap(lambda pairs: pairs).reduceByKey(lambda x,y: x+y) \
                                .filter(lambda pair_count: pair_count[1] >= int(support)) \
                                .map(lambda pair_count: pair_count[0]) \
                                .sortBy(lambda pairs: (len(pairs), pairs)).collect()

    return candidateItems, frequentItems

def formatOut(outputItems):
    resultText = ""
    flag =1
    for item in outputItems:
        if len(item) == 1:
            resultText += str(f"('{item[0]}'),")
        elif len(item) != flag:
            resultText = resultText[:-1] + "\n\n"
            flag = len(item)
            resultText += str(item) + ","
        else:
            resultText += str(item) + ","

    return resultText[:-1]


def saveToOut(candidateItems, frequentItems, output_file):
    with open(output_file, 'w') as f:
        f.write("Candidates:\n" + formatOut(candidateItems) + "\n\nFrequent Itemsets:\n" + formatOut(frequentItems))

def main():


    output_csv = "data/customer_product.csv"
    input_file = "data/ta_feng_all_months_merged.csv"
    filter = 20
    support = 50
    output_file = "output/output.txt"

    configuration = SparkConf()
    configuration.set("spark.driver.memory", "4g")
    configuration.set("spark.executor.memory", "4g")
    sc = SparkContext.getOrCreate(configuration)
    sc.setLogLevel("ERROR")

    time_start = time()

    rdd = sc.textFile(input_file)
    header = rdd.first()
    dataRDD = rdd.filter(lambda line: line != header)

    dataRDD = dataRDD.map(lambda x: x.split(',')).map(lambda f: (f[0][1:-1][:-4] + f[0][1:-1][-2:] + '-' + str(int(f[1][1:-1])), str(int(f[5][1:-1]))))

    writeToCSV(dataRDD.collect(), output_csv)

    rdd1 = sc.textFile(output_csv)
    header = rdd1.first()
    dataRDD1 = rdd1.filter(lambda line: line != header)

    basketRDD = dataRDD1.map(lambda x: (x.split(',')[0], x.split(',')[1])) \
                        .groupByKey().mapValues(list) \
                        .map(lambda x: list(set(x[1])))\
                        .filter(lambda x: len(x) > filter).map(lambda x: x)

    basketRDD.persist(StorageLevel.DISK_ONLY)

    totalLength = basketRDD.count()

    candidateItems, frequentItems = SON_algo(basketRDD, support, totalLength)

    saveToOut(candidateItems, frequentItems, output_file)

    exec_time = time() - time_start
    print("Duration: ", exec_time)

if __name__ == '__main__':
    main()