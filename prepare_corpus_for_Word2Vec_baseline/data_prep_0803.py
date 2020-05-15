# Yuwei Cao, 08/03/2019, ycao43@uic.edu
# This file prepares the corpus for training Word2Vec baseline models.
import pandas
import pickle
import numpy as np
import pymongo
from pymongo import MongoClient

mimic = pymongo.MongoClient("mongodb://localhost:27017/").mimic_v2.mimiciii
inpath = "./data"
outpath = ""

# Concatenate the medical concepts belong to each patient
def prep_data(data, outpath):
    with open(outpath + '/skipgram_data_0803.txt', 'w') as fout:
        for _, row in data.iterrows():
            admission = ''
            admission += ('a' + str(row['age_group']))
            admission += ' '
            admission += ('g' + str(row['gender']))
            admission += ' '
            admission += ('t' + str(row['ethnicity']))
            admission += ' '
            for each in list(row['labevents']):
                admission += ('l' + str(each))
                admission += ' '
            for each in list(row['microbiology']):
                admission += ('m' + str(each))
                admission += ' '
            for each in list(row['sypmtoms']):
                admission += ('s' + str(each))
                admission += ' '
            for each in list(row['diagnoses']):
                admission += ('d' + str(each))
                admission += ' '
            for each in list(row['procedures']):
                admission += ('o' + str(each))
                admission += ' '
            for each in list(row['prescriptions']):
                admission += ('e' + str(each))
                admission += ' '
            fout.write(admission.strip() + '\n')

def append_pres_proc(df):
    pres_list, proc_list = [], []
    count = 0
    for _, row in df.iterrows():
        hadm_id = row['hadm_id']
        cursor = mimic.find({"hadm_id": hadm_id, "procedures": {"$exists": True}}, {"procedures":1})
        if cursor.count() > 0:
            procedures = cursor[0]["procedures"]
            proc_list.append(list(set(procedures)))
        else:
            proc_list.append([])
        cursor = mimic.find({"hadm_id": hadm_id, "prescription": {"$exists": True}}, {"prescription":1})
        if cursor.count() > 0:
            prescriptions = cursor[0]["prescription"]
            pres_list.append(list(set(prescriptions)))
        else:
            pres_list.append([])

        count += 1
        if count % 1000 == 0:
            print(count)

    pandas.Series(np.asarray(proc_list)).to_pickle(outpath + 'all_proc.pkl')
    pandas.Series(np.asarray(pres_list)).to_pickle(outpath + 'all_pres.pkl')

    df["procedures"] = np.asarray(proc_list)
    df["prescriptions"] = np.asarray(pres_list)
    df.to_pickle(outpath + '/data_all_patients_all.pkl')

def get_diag_freq(df):
    diag_freq = {}
    for _, row in df.iterrows():
        for each in list(row['diagnoses']):
            if each not in diag_freq.keys():
                diag_freq[each] = 1
            else:
                diag_freq[each] += 1
    with open(outpath + 'diag_freq.pkl', 'wb') as f:
        pickle.dump(diag_freq, f)
    print("total unique diagnoses:")
    print(len(diag_freq))
    count_10 = 0
    for diag in diag_freq.keys():
        if diag_freq[diag] >= 10:
            count_10 += 1
    print("number of unique diagnoses that have frequency larger than 10:")
    print(count_10)

def main():
    """
    # read processed data
    file_name = inpath + '/data_all_patients_with_eth.pkl'
    data = pandas.read_pickle(file_name)
    append_pres_proc(data)
    
    file_name = outpath + '/data_all_patients_all.pkl'
    data = pandas.read_pickle(file_name)
    #print(data.head(10))
    #print(data["procedures"].head(10))
    #print(data["prescriptions"].head(10))

    prep_data(data, outpath)
    
    # get max length
    with open(outpath + '/skipgram_data_0803.txt') as f:
        content = f.readlines()
    length = [len(x.split()) for x in content]
    print(max(length))
    """
    # get diagnoses frequencies
    file_name = outpath + '/data_all_patients_all.pkl'
    data = pandas.read_pickle(file_name)
    get_diag_freq(data)

if __name__ == "__main__":
    main()