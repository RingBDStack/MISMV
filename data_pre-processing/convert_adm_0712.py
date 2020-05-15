import pymongo
import psycopg2
from pymongo import MongoClient


def diags_to_bag(mongo):

    index_patient = 0
    for doc in mongo.find({"diagnoses": {"$exists": True}}):
        hadm_id = doc["hadm_id"]
        #print(hadm_id)
        for code in doc["diagnoses"]:
            #print(code)
            mongo.update_one({'hadm_id': hadm_id}, {
                                '$addToSet': {'diagnoses_0712': code}})
        index_patient += 1
        if index_patient % 1000 == 0:
            print(index_patient) 

def check_age_group_1(mongo):
    diag_adm_dict, diag_dict = {}, {}
    for doc in mongo.find({"age_group": 1}):
        #hadm_id = doc["hadm_id"]
        diag_adm = doc["diagnosis_adm"]
        if diag_adm not in diag_adm_dict.keys():
            diag_adm_dict[diag_adm] = 1
        else: 
            diag_adm_dict[diag_adm] += 1

        for code in doc["diagnoses"]:
            if code not in diag_dict.keys():
                diag_dict[code] = 1
            else:
                diag_dict[code] += 1
    return diag_adm_dict, diag_dict

def rm_age_group_1(mongo):
    mongo.remove({"age_group": 1})

def lab_to_bag(mongo):

    index_patient = 0
    cursor = mongo.find({"labevents": {"$exists": True}}, no_cursor_timeout=True).skip(42960)
    #cursor = mongo.find({"labevents": {"$exists": True}, "labevents_0712": {"$exists": False}}, no_cursor_timeout=True)
    with cursor:
        for doc in cursor:
            hadm_id = doc["hadm_id"]
            #print(hadm_id)
            for event in doc["labevents"]:
                mongo.update_one({'hadm_id': hadm_id}, {
                                    '$addToSet': {'labevents_0712': event}})
            index_patient += 1
            if index_patient % 1000 == 0:
                print(index_patient) 

def symp_to_bag(mongo):

    index_patient = 0
    for doc in mongo.find({"symp_hits_0616": {"$exists": True}, "symp_hits_0712": {"$exists": False}}):
        hadm_id = doc["hadm_id"]
        #print(hadm_id)
        for symp in doc["symp_hits_0616"]:
            codes = [each for each in symp]
            codes.sort(reverse = True)
            mongo.update_one({'hadm_id': hadm_id}, {
                                '$addToSet': {'symp_hits_0712': codes[0]}})
        index_patient += 1
        if index_patient % 10 == 0:
            print(index_patient) 

def get_symp_dict_0712(mongo):

    code_to_hadmid ={}
    index_patient = 0
    for doc in mongo.find({"symp_hits_0712": {"$exists": True}}):
        hadm_id = doc["hadm_id"]
        #print(hadm_id)
        for code in doc["symp_hits_0712"]:
            if code not in code_to_hadmid.keys():
                code_to_hadmid[code] = []
            code_to_hadmid[code].append(hadm_id)
            
        index_patient += 1
        if index_patient % 1000 == 0:
            print(index_patient) 
    return code_to_hadmid

"""
generates mimic.symp_tree
"""
def get_symp_tree_0712(code_to_hadmid, mongo_symp_tree_0712):
    for code in code_to_hadmid.keys():
        parent = code[0:-4]
        mongo_symp_tree_0712.insert_one({ "_id": code, "hadm_ids": code_to_hadmid[code], "parent": parent})

"""
diag_to_hadmids: a dictionary that contains diagnose icd9 code and all hadm_ids whose 'diagnoses_0712' field contains the code
"""
def get_diagnose_dict_0712(mongo):

    diag_to_hadmids = {}
    index = 0
    for adm in mongo.find({"diagnoses_0712": {"$exists": True}}):
        hadm_id = adm['hadm_id']
        for code in adm['diagnoses_0712']:
            if code not in diag_to_hadmids.keys():
                diag_to_hadmids[code] = []
            diag_to_hadmids[code].append(hadm_id)

        index += 1
        if index % 1000 == 0:
            print(index) 
    return diag_to_hadmids

"""
generates mimic.diag_tree
"""
def mongo_diagnose_tree(diag_to_hadmids, mongo_diag_tree):
    for code in diag_to_hadmids.keys():
        parent = code[0:3]
        mongo_diag_tree.insert_one({ "_id": code, "hadm_ids": diag_to_hadmids[code], "parent": parent})

if __name__ == '__main__':
    mongo = MongoClient().mimic.mimiciii
    #diags_to_bag(mongo)
    """
    # check the statistics of the diagnoses of patients in age group 1
    diag_adm_dict, diag_dict = check_age_group_1(mongo)
    print("-"*20 + "diag_adm_dict")
    print(diag_adm_dict)
    print("-"*20 + "diag_dict")
    print(diag_dict)
    """
    #rm_age_group_1(mongo)
    #lab_to_bag(mongo)
    #symp_to_bag(mongo)
    """
    code_to_hadmid = get_symp_dict_0712(mongo)
    dict_path = ""
    with open(dict_path + '/code_to_hadmid_0712.txt','w') as f:
        for i, code in enumerate(code_to_hadmid.keys()):
            #print(str(i) + ': ' + str(code) + '\tadam_ids: ' + str(code_to_hadmid[code]))
            f.write(str(code) + '\t' + str(code_to_hadmid[code]) + '\n')
    mongo_symp_tree_0712 = MongoClient().mimic.symp_tree_0712
    get_symp_tree_0712(code_to_hadmid, mongo_symp_tree_0712)

    
    diag_to_hadmids = get_diagnose_dict_0712(mongo)
    dict_path = ""
    with open(dict_path + '/diag_to_hadmids_0712.txt','w') as f:
        for i, code in enumerate(diag_to_hadmids.keys()):
            if code:
                #print(code + '\t' + str(diag_to_hadmids[code]))
                f.write(code + '\t' + str(diag_to_hadmids[code]) + '\n')
    mongo_diag_tree_0712 = MongoClient().mimic.diag_tree_0712
    mongo_diagnose_tree(diag_to_hadmids, mongo_diag_tree_0712)
    """
    #mongo.update({}, {'$unset': {'symp_hits_0616': 1}}, upsert=False, multi=True)