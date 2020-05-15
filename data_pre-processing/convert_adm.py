import pymongo
import psycopg2
from pymongo import MongoClient
import datetime
import re
import csv
from bson.decimal128 import Decimal128


POSTGRES_DBNAME = "mimic"
POSTGRES_USER = ""
POSTGRES_PWD = ""
HOST = "localhost"


def convert_patients(cursor, mongo):
    cursor.execute(
        "select a.hadm_id, a.subject_id, p.gender, ROUND( (cast(a.admittime as date) - cast(p.dob as date)) / 365.242,2) as age, a.diagnosis, p.dob, a.admittime, p.dod, a.ethnicity \
        from mimiciii.admissions a\
        left join mimiciii.patients p\
        on a.subject_id = p.subject_id;")
    for patient in cursor.fetchall():
        mongo_patient = dict(
            zip(('hadm_id', 'subject_id', 'gender', 'age', 'diagnosis_adm', 'dob', 'admittime', 'dod', 'ethnicity'), 
            (patient[0], patient[1], patient[2], Decimal128(patient[3]), patient[4], patient[5], patient[6], patient[7], patient[8])))
        mongo.update_one({'hadm_id': patient[0]}, {
                         '$set': mongo_patient}, upsert=True)

"""
The rules for the partition of age_groups is the same to Yizhou Sun's HeteroMed paper, except we include one more group 
for patients whose age < 1, i.e. newborn.
"""
def add_age_group(mongo):
    cursor = mongo.find({}, {"hadm_id": 1, "age": 1})
    for x in cursor:
        age_group = 1
        age = x["age"].to_decimal()
        if age >= 64:
            age_group = 5
        elif age >= 30:
            age_group = 4
        elif age >= 15:
            age_group = 3
        elif age >= 1:
            age_group = 2
        mongo.update_one({'hadm_id': x["hadm_id"]}, {'$set': {"age_group": age_group}})

def convert_diagnoses(cursor, mongo):
    cursor.execute(
        "select hadm_id, icd9_code from mimiciii.diagnoses_icd;")
    index = 0
    print("SQL executed.")
    for patient in cursor.fetchall():
        mongo.update_one({'hadm_id': patient[0]}, {
                         '$push': {'diagnoses': patient[1]}})
        index += 1
        if index % 100000 == 0:
            print(index)       

"""
This function ignores labevents for outpatients(those who don't have hadm_id). 
To process labevents for outpatients, use 'convert_labevents_outpatients' function.

For each labevent record, this function tries to tell if it is abnormal. First match
the tested item to item_range dictionary; If the tested item is not in item_range 
dictionary, then use 'flag' field.  
"""
def convert_labevents(cursor, mongo, item_range):
    cursor.execute(
        "select * from labevents where hadm_id is not null;")
        #"select * from labevents where hadm_id is not null and valuenum is null and flag = 'abnormal' limit 10;")
    index = 0
    print("SQL executed.")
    for event in cursor.fetchall():
        #print(event)
        abnormal_event = ''
        hadm_id = event[2]
        item_id = event[3]
        flag = event[8]
        if item_id in item_range.keys():
            normal_range = item_range[item_id]
            valuenum = event[6] 
            value = event[5]
            #print(valuenum)

            if valuenum is not None:
                if valuenum < normal_range[0]:
                    abnormal_event = str(item_id) + 'l'
                elif valuenum > normal_range[1]:
                    abnormal_event = str(item_id) + 'h'
            elif value is not None:
                if value[0] == '<' or value[-1] == '-':
                    abnormal_event = str(item_id) + 'l'
                elif value[0] == '>' or value[-1] == '+':
                    abnormal_event = str(item_id) + 'h'

        elif flag == 'abnormal':
            abnormal_event = str(item_id) + 'a'

        if abnormal_event != '':
            #print(str(hadm_id) + ": " + abnormal_event)
            mongo.update_one({'hadm_id': hadm_id}, {
                         '$push': {'labevents': abnormal_event}})
            
        index += 1
        if index % 10000 == 0:
            print(index)       

def convert_procedures_icd(cursor, mongo):
    cursor.execute(
        "select hadm_id, icd9_code from procedures_icd;")
    index = 0
    print("SQL executed.")
    for patient in cursor.fetchall():
        #print(str(patient[0]) + ": " + str(patient[1]))
        mongo.update_one({'hadm_id': patient[0]}, {
                         '$push': {'procedures': patient[1]}})
        index += 1
        if index % 100000 == 0:
            print(index)

"""
only consider records with valid org_itemid, i.e. with possitive culture(the organism grews)
one org_itemid is recorded only once for each hadm_id
"""
def convert_microbiology(cursor, mongo):
    cursor.execute(
        "select distinct(hadm_id, org_itemid) from microbiologyevents where org_itemid is not null;")
    index = 0
    print("SQL executed.")
    for each in cursor.fetchall():
        test = [item.strip('()') for item in each[0].split(',')]
        hadm_id = int(test[0])
        org_itemid = test[1]
        #print(str(hadm_id) + ": " + org_itemid)
        mongo.update_one({'hadm_id': hadm_id}, {
                         '$push': {'microbiology': org_itemid}})
        index += 1
        if index % 10000 == 0:
            print(index)

"""
Ignores drugs of type 'BASE'.
record drugs(its index in pres_index_dict dictionary) that have been given to each patient(hadm_id)
one type of drug is recorded only once for each hadm_id
"""
def convert_prescriptions(cursor, mongo, pres_index_dict):
    cursor.execute(
        "select hadm_id, drug from prescriptions where drug_type in ('MAIN', 'ADDITIVE');")
    index = 0
    print("SQL executed.")
    for each in cursor.fetchall():
        hadm_id = each[0]
        drug = re.sub('\*[a-z]+\*|\*nf|neo\*[a-z]+\*|<ind>|<\sind>|\s+|/|-|\d+|\.|%', '', each[1].lower().strip())[0:7]
        if drug in pres_index_dict.keys():
            drug_id = pres_index_dict[drug]
            #print(str(hadm_id) + ": " + str(drug_id))
            mongo.update_one({'hadm_id': hadm_id}, {
                            '$addToSet': {'prescription': drug_id}})
        index += 1
        if index % 10000 == 0:
            print(index)

"""
Ignores drugs of type 'BASE'.
generates an abbreviation and an index for each drug, maps drug names to their abbreviation
(same drug with different spellings or specifications therefore map into the same abbreviation)

pres_dict: a dictionary that contains abbreviations of drugs, as well as how many times a drug has been given to patients
pres_name_dict: a dictionary that contains abbreviations of drugs, as well as all drug names that map into each abbreviation
pres_index_dict: a dictionary that contains abbreviations of drugs, as well as its(unique) index
"""
def get_prescription_dict(cursor, mongo):
    cursor.execute(
        "select distinct(drug) as a, count(*) from prescriptions where drug_type in ('MAIN', 'ADDITIVE') group by a order by a;")
    print("SQL executed.")
    pres_dict = {}
    pres_name_dict = {}
    for each in cursor.fetchall():
        name = re.sub('\*[a-z]+\*|\*nf|neo\*[a-z]+\*|<ind>|<\sind>|\s+|/|-|\d+|\.|%', '', each[0].lower().strip())[0:7]
        count = each[1]
        if name in pres_dict.keys():
            pres_dict[name] += count    
            pres_name_dict[name].append(each[0])    
        else:
            pres_dict[name] = count
            pres_name_dict[name] = []
            pres_name_dict[name].append(each[0])
    
    del pres_dict['']
    pres_index_dict = dict((pres, i) for i, pres in enumerate(pres_dict.keys()))

    return pres_dict, pres_name_dict, pres_index_dict

"""
fetch symptom_hits in mimic4, retain only mesh codes that are within categories 'A', 'B' and 'C'

code_to_word: a dictionary that contains mesh codes and their corresponding words
code_to_hadmid: a dictionary that contains mesh codes and all hadm_ids whose symp_hits contains each code 
"""
def convert_symp(mongo_patient, mongo_adm):

    code_to_hadmid ={}
    code_to_word = {}

    index_patient = 0
    for doc in mongo_patient.find():
        for admission in doc['admission']:
            hadm_id = admission['hadm_id']

            for word in admission['symptom_hit']:

                #print(word) # should be the word
                codes = admission['symptom_hit'][word] # should be the codes(array) of the word
                #print(codes) 

                #codes = [re.sub('\.','', code) for code in codes if code[0] in ['A', 'B', 'C']]
                codes = [code for code in codes if code[0] in ['A', 'B', 'C']]
                #print(codes)
                for code in codes:
                    if code and code not in code_to_hadmid.keys():
                        code_to_hadmid[code] = []
                        code_to_word[code] = word
                    code_to_hadmid[code].append(hadm_id)
                
                if code:
                    mongo_adm.update_one({'hadm_id': hadm_id}, {
                                '$addToSet': {'symp_hits': codes}})
        index_patient += 1
        if index_patient % 1000 == 0:
            print(index_patient)  
                
    return code_to_hadmid, code_to_word

"""
generates mimic.symp_tree
"""
def get_symp_tree(code_to_hadmid, code_to_word, mongo_symp_tree):
    for code in code_to_hadmid.keys():
        if len(code) == 3:
            parent = code[0]
        else:
            parent = code[0:-4]
        mongo_symp_tree.insert_one({ "_id": code, "word": code_to_word[code], "hadm_ids": code_to_hadmid[code], "parent": parent})

"""
diag_to_hadmids: a dictionary that contains diagnose icd9 code and all hadm_ids whose 'diagnoses' field contains the code
"""
def get_diagnose_dict(mongo):

    diag_to_hadmids = {}
    index = 0
    for adm in mongo.find():
        hadm_id = adm['hadm_id']
        for code in adm['diagnoses']:
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
        if code:
            parent = code[0:3]
            mongo_diag_tree.insert_one({ "_id": code, "hadm_ids": diag_to_hadmids[code], "parent": parent})

"""
# Use the three functions below to process labevents for outpatients
# not tested yet, may contain bugs
# These functions match labevents by time: if a patient has came to the hospital for only one time,
# i.e. has only one hadm_id, then record all abnormal labevents under that hadm_id; if a patient has 
# multiple hadm_ids, record each abnormal labevent to its nearest admission

def get_adm_time_dict(cursor, mongo):
    all_adm_time = {}
    cursor.execute(
        "select subject_id, admittime, hadm_id \
        from admissions \
        where subject_id in (select distinct(subject_id) from labevents where hadm_id is null limit 50) \
        order by subject_id asc, admittime asc; ")

    for each in cursor.fetchall():
        subject_id = each[0]
        admittime = each[1]
        hadm_id = each[2]
        if subject_id not in all_adm_time.keys():
            all_adm_time[subject_id] = {}
        all_adm_time[subject_id][admittime] = hadm_id
    
    return all_adm_time

def convert_labevents_outpatients(cursor, mongo, item_range, all_adm_time):
    cursor.execute(
        "select * from labevents where hadm_id is null limit 5;")
    
    for event in cursor.fetchall():
        subject_id = event[1]
        item_id = event[3]
        charttime = event[4]
        valuenum = event[6]
        value = event[5]
        flag = event[8] 
        
        print(charttime)

        abnormal_event = check_abnormal_event(item_range, item_id, flag, valuenum, value)

        if abnormal_event != '': 
            adm_times = all_adm_time[subject_id]
            hadm_id = 0
            if len(adm_times) == 1:
                hadm_id = next(iter(adm_times.values()))
            else:
                last_time = next(iter(adm_times.keys()))
                for time in adm_times.keys():
                    if time > charttime:
                        if abs(time - charttime) <= abs(last_time - charttime):
                            hadm_id = adm_times[time]
                        else:
                            hadm_id = adm_times[last_time]
                        break
                    last_time = time

                if hadm_id == 0:
                    hadm_id = adm_times[last_time]

            if  hadm_id != 0:
                print(str(hadm_id) + ": " + abnormal_event)
                #mongo.update_one({'hadm_id': hadm_id}, {
                #            '$push': {'labevents': abnormal_event}})

def check_abnormal_event(item_range, item_id, flag, valuenum, value):
    abnormal_event = ''

    if item_id in item_range.keys():
        normal_range = item_range[item_id]

        if valuenum is not None:
            if valuenum < normal_range[0]:
                abnormal_event = str(item_id) + 'l'
            elif valuenum > normal_range[1]:
                abnormal_event = str(item_id) + 'h'
        elif value is not None:
            if value[0] == '<' or value[-1] == '-':
                abnormal_event = str(item_id) + 'l'
            elif value[0] == '>' or value[-1] == '+':
                abnormal_event = str(item_id) + 'h'

    elif flag == 'abnormal':
        abnormal_event = str(item_id) + 'a'
    
    return abnormal_event
"""

if __name__ == '__main__':
    conn = psycopg2.connect(host=HOST, dbname=POSTGRES_DBNAME, user=POSTGRES_USER,
                            password=POSTGRES_PWD, options="--search_path=mimiciii")
    cursor = conn.cursor()
    mongo = MongoClient().mimic.mimiciii
    """
    dict_path = ""
    item_range = {}
    with open(dict_path,'r') as f:
        for line in f:
            x = [word.strip('\n') for word in line.split(' ')]
            item_range[int(x[0])] = (float(x[1]), float(x[2]))
    """
    #print(item_range)
    #print()
    #mongo.delete_many({})
    #convert_patients(cursor,mongo)
    #add_age_group(mongo)
    #convert_diagnoses(cursor, mongo)
    #convert_labevents(cursor, mongo, item_range)
    #adm_time_dict = get_adm_time_dict(cursor, mongo) #didn't run yet
    #print(adm_time_dict) #didn't run yet
    #convert_labevents_outpatients(cursor, mongo, item_range) #didn't run yet
    #convert_procedures_icd(cursor, mongo)
    #convert_microbiology(cursor, mongo)
    """
    pres_dict, pres_name_dict, pres_index_dict = get_prescription_dict(cursor, mongo) 

    dict_path = ""
    #print(pres_dict)
    with open(dict_path + '/pres_dict.txt','w') as f:
        for i, pres in enumerate(pres_dict.keys()):
            #print(pres + '\tcount: ' + str(pres_dict[pres]))
            f.write(pres + '\t' + str(pres_dict[pres]) + '\n')
    
    with open(dict_path + '/pres_name_dict.txt','w') as f:
        for i, pres in enumerate(pres_name_dict.keys()):
            f.write(pres + "\t" + str(pres_name_dict[pres]) + '\n')

    with open(dict_path + '/pres_index_dict.txt','w') as f:
        for i, pres in enumerate(pres_index_dict.keys()):
            f.write(pres + "\t" + str(pres_index_dict[pres]) + '\n')
    
    convert_prescriptions(cursor, mongo, pres_index_dict)
    """
    """
    for _ in range(2):
        mongo.update({'hadm_id': 100765}, {
                         '$pop': {'labevents': 1}})
    """
    #mongo.update({}, {'$unset': {'labevents':1}} , multi=True)
    """
    mongo_patient = MongoClient().mimic4.mimiciii
    mongo_symp_tree = MongoClient().mimic.symp_tree

    code_to_hadmid, code_to_word = convert_symp(mongo_patient, mongo)
    
    dict_path = ""
    #print(code_to_hadmid)

    with open(dict_path + '/code_to_hadmid.txt','w') as f:
        for i, code in enumerate(code_to_hadmid.keys()):
            #print(str(i) + ': ' + str(code) + '\tadam_ids: ' + str(code_to_hadmid[code]))
            f.write(str(code) + '\t' + str(code_to_hadmid[code]) + '\n')
    
    with open(dict_path + '/code_to_word.txt','w') as f:
        for i, code in enumerate(code_to_word.keys()):
            #print(str(i) + ': ' + str(code) + '\tword: ' + str(code_to_word[code]))
            f.write(str(code) + '\t' + code_to_word[code] + '\n')
       
    get_symp_tree(code_to_hadmid, code_to_word, mongo_symp_tree)
    
    diag_to_hadmids = get_diagnose_dict(mongo)
    dict_path = ""
    with open(dict_path + '/diag_to_hadmids.txt','w') as f:
        for i, code in enumerate(diag_to_hadmids.keys()):
            if code:
                #print(code + '\t' + str(diag_to_hadmids[code]))
                f.write(code + '\t' + str(diag_to_hadmids[code]) + '\n')
    
    mongo_diag_tree = MongoClient().mimic.diag_tree
    mongo_diagnose_tree(diag_to_hadmids, mongo_diag_tree)
    """
    cursor.execute(
        "select hadm_id from diagnoses_icd where icd9_code is null;")
    for each in cursor.fetchall():
        hadm_id = each[0]
        mongo.update({"hadm_id": hadm_id},{"$unset": {"diagnoses":1}}, multi=True)

    cursor.close()
    conn.close()