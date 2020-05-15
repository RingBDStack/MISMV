import pymongo
import psycopg2
from pymongo import MongoClient
import datetime
import re

POSTGRES_DBNAME = ""
POSTGRES_USER = ""
POSTGRES_PWD = ""
HOST = ""


def convert_patients(cursor, mongo):
    cursor.execute(
        "select subject_id, gender, dob, dod from mimiciii.patients;")
    for patient in cursor.fetchall():
        mongo_patient = dict(
            zip(('subject_id', 'gender', 'dob', 'dod'), patient))
        mongo.update_one({'subject_id': patient[0]}, {
                         '$set': mongo_patient}, upsert=True)


def convert_admission(cursor, mongo):
    # for doc in mongo.find():
    #     mongo.update_one({'_id': doc['_id']}, {
    #         '$unset': {'events_list': '', 'admittime':'','dischtime':'', 'diagnosis':''}})
    cursor.execute(
        "select subject_id, admittime, dischtime, diagnosis from mimiciii.admissions;")
    for patient in cursor.fetchall():
        mongo_patient = dict(
            zip(('admittime', 'dischtime', 'diagnosis'), patient[1:]))
        mongo.update_one({'subject_id': patient[0]}, {'$push':  {'admission':mongo_patient}})


# def convert_events(cursor, mongo):
#     for doc in mongo.find():
#         if 'events_list' not in doc or len(doc['events_list']) > 0:
#             mongo.update_one({'subject_id': doc['subject_id']}, {
#                              '$set': {'events_list': []}})
#     print("Clean finished.")
#     # cursor.execute('select a.subject_id, a.row_id, a.itemid, b."label", a.charttime, a.value, a.valuenum, \
#     # a.valueuom from mimiciii.chartevents a left join mimiciii.d_items b on
#     # a.itemid=b.itemid;')
#     cursor.execute(
#         'select subject_id, row_id, itemid, charttime, value, valuenum, valueuom from mimiciii.chartevents')
#     index = 0
#     print("SQL executed.")

#     for patient in cursor.fetchall():
#         mongo_patient = dict(
#             zip(('row_id', 'itemid', 'time', 'value', 'valuenum', 'valueuom'), patient[1:]))
#         mongo.update_one({'subject_id': patient[0]}, {
#                          '$push': {'events_list': mongo_patient}})
#         index += 1
#         if index % 100000 == 0:
#             print(index)

def convert_diagnoses(cursor, mongo):
    cursor.execute(
        "select subject_id, icd9_code from mimiciii.diagnoses_icd;")
    index = 0
    print("SQL executed.")
    for patient in cursor.fetchall():
        # mongo_patient = dict(
            # zip(('icd9_code'), patient[1:]))
        mongo.update_one({'subject_id': patient[0]}, {
                         '$push': {'diagnoses': patient[1]}})
        index += 1
        if index % 100000 == 0:
            print(index)


def convert_drgcodes(cursor, mongo):
    cursor.execute(
        "select subject_id, drg_code, description from mimiciii.drgcodes;")
    index = 0
    print("SQL executed.")
    for patient in cursor.fetchall():
        mongo_patient = dict(
            zip(('drg_code', 'description'), patient[1:]))
        mongo.update_one({'subject_id': patient[0]}, {
                         '$push': {'drgcodes': mongo_patient}})
        index += 1
        if index % 100000 == 0:
            print(index)


def convert_fluid_input_cv(cursor, mongo):
    cursor.execute(
        "select subject_id, charttime, itemid, amount from mimiciii.inputevents_cv;")
    index = 0
    print("SQL executed.")
    for patient in cursor.fetchall():
        mongo_patient = dict(
            zip(('charttime', 'itemid', 'amount'), patient[1:]))
        mongo.update_one({'subject_id': patient[0]}, {
                         '$push': {'input_cv': mongo_patient}})
        index += 1
        if index % 10000 == 0:
            print(index)


def convert_fluid_input_mv(cursor, mongo):
    cursor.execute(
        "select subject_id, starttime, itemid, amount, amountuom, ordercategoryname, secondaryordercategoryname, \
        patientweight from mimiciii.inputevents_mv;")
    index = 0
    print("SQL executed.")
    for patient in cursor.fetchall():
        mongo_patient = dict(
            zip(('starttime', 'itemid', 'amount', 'amountuom', 'category', 'subcategory', 'weight'), patient[1:]))
        mongo.update_one({'subject_id': patient[0]}, {
                         '$push': {'input_mv': mongo_patient}})
        index += 1
        if index % 1000 == 0:
            print(index)


def convert_labevents(cursor, mongo):
    cursor.execute(
        "select subject_id, charttime, itemid, value, valuenum, flag from mimiciii.labevents;")
    index = 0
    print("SQL executed.")
    for patient in cursor.fetchall():
        mongo_patient = dict(
            zip(('time', 'itemid', 'value', 'valuenum', 'flag'), patient[1:]))
        mongo.update_one({'subject_id': patient[0]}, {
                         '$push': {'labevents': mongo_patient}})
        index += 1
        if index % 10000 == 0:
            print(index)


def convert_microbiology(cursor, mongo):
    cursor.execute(
        "select subject_id, charttime, spec_itemid, spec_type_desc, org_itemid, org_name, ab_itemid, ab_name, \
        dilution_text, dilution_value, interpretation from mimiciii.microbiologyevents;")
    index = 0
    print("SQL executed.")
    for patient in cursor.fetchall():
        mongo_patient = dict(
            zip(('charttime', 'spec_itemid', 'spec_name', 'org_itemid', 'org_name', 'ab_itemid',
                 'ab_name', 'dilution_text', 'dilution_value', 'interpretation'), patient[1:]))
        mongo.update_one({'subject_id': patient[0]}, {
                         '$push': {'microbiology': mongo_patient}})
        index += 1
        if index % 10000 == 0:
            print(index)


def split_text(text):
    text = re.sub('\[.*?\]','###',text)
    return text.replace('\n',' ')


def convert_text(cursor, mongo):
    for doc in mongo.find():
        mongo.update_one({'_id': doc['_id']}, {'$set': {'note': []}})

    print("Clean finished.")
    from collections import Counter
    cursor.execute(
        "select subject_id, chartdate, category, description, text from mimiciii.noteevents;")
    index = 0
    counts = Counter()
    print("SQL executed.")

    for patient in cursor.fetchall():
        # print(patient[0])
        text = patient[4]
        keys = re.findall("\n\n([A-Z -]+?):[ |\n]", text)
        if 'D' in keys:
            keys.remove('D')
        counts.update(keys)

        keys_and_values = [i for i in re.split("\n\n([A-Z -]+?):[ |\n]", text)]
        content = {}
        for i in range(len(keys_and_values)):
            kav = keys_and_values[i]
            if kav in keys:
                content[keys_and_values[i]] = split_text(keys_and_values[i + 1])
                i = i + 1
        mongo_patient = dict(
            zip(('chartdate', 'category', 'description'), patient[1:4]))
        mongo_patient['content'] = content
        # print(mongo_patient)
        mongo.update_one({'subject_id': patient[0]}, {
                         '$push': {'note': mongo_patient}})
        index += 1
        if index % 1000 == 0:
            print(index)
    for i in counts:
        print(i, counts[i])


def convert_prescriptions(cursor, mongo):
    cursor.execute(
        "select subject_id, startdate, drug, formulary_drug_cd, ndc from mimiciii.prescriptions;")
    index = 0
    print("SQL executed.")
    for patient in cursor.fetchall():
        mongo_patient = dict(
            zip(('date', 'drug', 'formulary_drug_cd', 'ndc'), patient[1:]))
        mongo.update_one({'subject_id': patient[0]}, {
                         '$push': {'medication': mongo_patient}})
        index += 1
        if index % 10000 == 0:
            print(index)


def convert_procedures_mv(cursor, mongo):
    cursor.execute(
        "select subject_id, starttime, itemid from mimiciii.procedureevents_mv;")
    index = 0
    print("SQL executed.")
    for patient in cursor.fetchall():
        mongo_patient = dict(
            zip(('time', 'itemid'), patient[1:]))
        mongo.update_one({'subject_id': patient[0]}, {
                         '$push': {'procedure_mv': mongo_patient}})
        index += 1
        if index % 10000 == 0:
            print(index)


def convert_procedures_icd(cursor, mongo):
    cursor.execute(
        "select subject_id, icd9_code from mimiciii.procedures_icd;")
    index = 0
    print("SQL executed.")
    for patient in cursor.fetchall():
        mongo.update_one({'subject_id': patient[0]}, {
                         '$push': {'procedures': patient[1]}})
        index += 1
        if index % 100000 == 0:
            print(index)


if __name__ == '__main__':
    conn = psycopg2.connect(host=HOST, dbname=POSTGRES_DBNAME, user=POSTGRES_USER,
                            password=POSTGRES_PWD, options="--search_path=mimiciii")
    cursor = conn.cursor()
    mongo = MongoClient().mimic.mimiciii

    # convert_patients(cursor,mongo)
    # convert_admission(cursor,mongo)
    # convert_events(cursor, mongo)
    # convert_diagnoses(cursor, mongo)
    # convert_drgcodes(cursor, mongo)
    # convert_fluid_input_cv(cursor, mongo)
    # convert_fluid_input_mv(cursor, mongo)
    # convert_labevents(cursor, mongo)
    # convert_text(cursor, mongo)
    # convert_microbiology(cursor, mongo)
    # convert_prescriptions(cursor, mongo)
    # convert_procedures_mv(cursor, mongo)
    # convert_procedures_icd(cursor, mongo)
    cursor.close()
    conn.close()
