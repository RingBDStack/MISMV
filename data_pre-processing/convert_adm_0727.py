import pymongo
import psycopg2
from pymongo import MongoClient

POSTGRES_DBNAME = "mimic"
POSTGRES_USER = ""
POSTGRES_PWD = ""
HOST = "localhost"

def convert_labevents_0726(cursor, mongo):
    cursor.execute(
        #"select * from labevents where hadm_id is not null order by row_id limit 5000000;")

        #"select * from labevents where hadm_id is not null\
        #order by row_id\
        #offset 15000000 rows fetch next 7245034 rows only;")

        "select * from labevents where hadm_id is not null\
        order by row_id\
        offset 21800000 rows fetch next 445034 rows only;")
    index = 0
    print("SQL executed.")
    for event in cursor.fetchall():
        hadm_id = event[2]
        item_id = event[3]
        flag = event[8]
        if flag == 'abnormal':
            labevent = str(item_id) + 'a'
        else:
            labevent = str(item_id) + 'n'
        mongo.update_one({'hadm_id': hadm_id}, {
            '$addToSet': {'labevents_0726': labevent}})
        
        index += 1
        if index % 10000 == 0:
            print(index)

if __name__ == '__main__':
    mongo = MongoClient().mimic_v2.mimiciii

    conn = psycopg2.connect(host=HOST, dbname=POSTGRES_DBNAME, user=POSTGRES_USER,
                            password=POSTGRES_PWD, options="--search_path=mimiciii")
    cursor = conn.cursor()

    convert_labevents_0726(cursor, mongo)

    cursor.close()
    conn.close()