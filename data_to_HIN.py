# Yuwei Cao, 07/30/2019, ycao43@uic.edu
# This file converts the pre-processed MIMIC III, ICD-9-CM and MeSH datasets into HIN, 
# and conduct random-walks to get meta-path instances. 
from pymongo import MongoClient
import networkx as nx
import pickle as pkl
from random import sample 
import time
from time import gmtime, strftime
import sys
import itertools

mongo = MongoClient().mimic_v2.mimiciii
outpath = "./out/"

def add_nodes(nodes, graph):
    for node in nodes:
        graph.add_node(node)

def get_ethnicity_dict():
    ethnicity_to_hadmids = {}
    ethnicity_to_hadmids['tWHITE'] = []
    ethnicity_to_hadmids['tBLACK'] = []
    ethnicity_to_hadmids['tHISPA'] = []
    ethnicity_to_hadmids['tASIAN'] = []
    ethnicity_to_hadmids['tOTHER'] = []
    index = 0
    print("\nstart to generate ethnicity_dict")

    for adm in mongo.find():
        hadm_id = adm['hadm_id']
        eth = adm['ethnicity'].upper()
        if eth[:5] == 'WHITE' or eth[:5] == 'PORTU':
            ethnicity_to_hadmids['tWHITE'].append('p' + str(hadm_id))
        elif eth[:5] == 'BLACK':
            ethnicity_to_hadmids['tBLACK'].append('p' + str(hadm_id))
        elif eth[:5] == 'HISPA':
            ethnicity_to_hadmids['tHISPA'].append('p' + str(hadm_id))
        elif eth[:5] == 'ASIAN':
            ethnicity_to_hadmids['tASIAN'].append('p' + str(hadm_id))
        else:
            ethnicity_to_hadmids['tOTHER'].append('p' + str(hadm_id))

        index += 1
        if index % 1000 == 0:
            print(index) 

    print("\ndone generating ethnicity_dict")
    return ethnicity_to_hadmids
    
def get_age_dict():
    age_to_hadmids = {}
    age_to_hadmids['a3'], age_to_hadmids['a4'], age_to_hadmids['a5'] = [], [], []
    index = 0
    print("\nstart to generate age_dict")

    for adm in mongo.find():
        hadm_id = adm['hadm_id']
        age_group = adm['age_group']
        if age_group == 3:
            age_to_hadmids['a3'].append('p' + str(hadm_id))
        elif age_group == 4:
            age_to_hadmids['a4'].append('p' + str(hadm_id))
        elif age_group == 5:
            age_to_hadmids['a5'].append('p' + str(hadm_id))
        else:
            print("error")

        index += 1
        if index % 1000 == 0:
            print(index)

    print("\ndone generating age_dict")
    return age_to_hadmids

def get_gender_dict():
    gender_to_hadmids = {}
    gender_to_hadmids['gF'], gender_to_hadmids['gM'] = [], []
    index = 0
    print("\nstart to generate gender_dict")

    for adm in mongo.find():
        hadm_id = adm['hadm_id']
        gender = adm['gender']
        if gender == "F":
            gender_to_hadmids['gF'].append('p' + str(hadm_id))
        elif gender == "M":
            gender_to_hadmids['gM'].append('p' + str(hadm_id))
        else:
            print("error")

        index += 1
        if index % 1000 == 0:
            print(index)

    print("\ndone generating gender_dict")
    return gender_to_hadmids

def get_diagnose_dict():
    diag_to_hadmids = {}
    index = 0
    print("\nstart to generate diagnose_dict")
    for adm in mongo.find({"diagnoses_0712": {"$exists": True}}):
        hadm_id = adm['hadm_id']
        for each in adm['diagnoses_0712']:
            code = 'd' + str(each)
            if code not in diag_to_hadmids.keys():
                diag_to_hadmids[code] = []
            diag_to_hadmids[code].append('p' + str(hadm_id))

        index += 1
        if index % 1000 == 0:
            print(index) 

    print("\ndone generating diagnose_dict")
    return diag_to_hadmids

def get_symp_dict():
    symp_to_hadmids = {}
    index = 0
    print("\nstart to generate symp_dict")
    for adm in mongo.find({"symp_hits_0712": {"$exists": True}}):
        hadm_id = adm['hadm_id']
        for each in adm['symp_hits_0712']:
            symp = 's' + str(each)
            if symp not in symp_to_hadmids.keys():
                symp_to_hadmids[symp] = []
            symp_to_hadmids[symp].append('p' + str(hadm_id))

        index += 1
        if index % 1000 == 0:
            print(index) 

    print("\ndone generating symp_dict")
    return symp_to_hadmids

def get_micro_dict():
    micro_to_hadmids = {}
    index = 0
    print("\nstart to generate micro_dict")
    for adm in mongo.find({"microbiology": {"$exists": True}}):
        hadm_id = adm['hadm_id']
        for each in adm['microbiology']:
            micro = 'm' + str(each)
            if micro not in micro_to_hadmids.keys():
                micro_to_hadmids[micro] = []
            micro_to_hadmids[micro].append('p' + str(hadm_id))

        index += 1
        if index % 1000 == 0:
            print(index) 

    print("\ndone generating micro_dict")
    return micro_to_hadmids

def get_proce_dict():
    proce_to_hadmids = {}
    index = 0
    print("\nstart to generate proce_dict")
    for adm in mongo.find({"procedures": {"$exists": True}}):
        hadm_id = adm['hadm_id']
        for each in adm['procedures']:
            proce = 'o' + str(each)
            if proce not in proce_to_hadmids.keys():
                proce_to_hadmids[proce] = []
            proce_to_hadmids[proce].append('p' + str(hadm_id))

        index += 1
        if index % 1000 == 0:
            print(index) 

    print("\ndone generating proce_dict")
    return proce_to_hadmids

def get_presc_dict():
    presc_to_hadmids = {}
    index = 0
    print("\nstart to generate presc_dict")
    for adm in mongo.find({"prescription": {"$exists": True}}):
        hadm_id = adm['hadm_id']
        for each in adm['prescription']:
            presc = 'e' + str(each)
            if presc not in presc_to_hadmids.keys():
                presc_to_hadmids[presc] = []
            presc_to_hadmids[presc].append('p' + str(hadm_id))

        index += 1
        if index % 1000 == 0:
            print(index) 

    print("\ndone generating presc_dict")
    return presc_to_hadmids

def get_labevent_dict():
    lab_to_hadmids = {}
    index = 0
    print("\nstart to generate labevent_dict")

    for adm in mongo.find({"labevents_0726": {"$exists": True}}):
        hadm_id = adm['hadm_id']
        for each in adm['labevents_0726']:
            code = 'l' + str(each)
            if code not in lab_to_hadmids.keys():
                lab_to_hadmids[code] = []
            lab_to_hadmids[code].append('p' + str(hadm_id))

        index += 1
        if index % 1000 == 0:
            print(index) 

    print("\ndone generating labevent_dict")
    return lab_to_hadmids

# This function constructs the initial HIN that contains nodes and edges from the MIMIC III dataset
def gen_graph():
    graph = nx.Graph()
    nodes = set()

    # add patients to nodes
    for adm in mongo.find():
        nodes.add(str(adm['hadm_id']))
    add_nodes(nodes, graph)

    # add diagnoses to nodes
    diag_to_hadmids = get_diagnose_dict()
    add_nodes(diag_to_hadmids.keys(), graph)
    for diag in diag_to_hadmids.keys():
        for adm in diag_to_hadmids[diag]:
            graph.add_edge(diag, adm)

    # add labevents to nodes
    lab_to_hadmids = get_labevent_dict()
    add_nodes(lab_to_hadmids.keys(), graph)
    for lab in lab_to_hadmids.keys():
        for adm in lab_to_hadmids[lab]:
            graph.add_edge(lab, adm)

    # add ethnicities to nodes
    ethnicity_to_hadmids = get_ethnicity_dict()
    for _, key in enumerate(ethnicity_to_hadmids.keys()):
        print(key)
        print(len(ethnicity_to_hadmids[key]))
    add_nodes(ethnicity_to_hadmids.keys(), graph)
    for eth in ethnicity_to_hadmids.keys():
        for adm in ethnicity_to_hadmids[eth]:
            graph.add_edge(eth, adm)

    # add ages to nodes
    age_to_hadmids = get_age_dict()
    add_nodes(age_to_hadmids.keys(), graph)
    for age in age_to_hadmids.keys():
        for adm in age_to_hadmids[age]:
            graph.add_edge(age, adm)

    # add gender to nodes
    gender_to_hadmids = get_gender_dict()
    add_nodes(gender_to_hadmids.keys(), graph)
    for gender in gender_to_hadmids.keys():
        for adm in gender_to_hadmids[gender]:
            graph.add_edge(gender, adm)
    
    # add symp to nodes
    symp_to_hadmids = get_symp_dict()
    add_nodes(symp_to_hadmids.keys(), graph)
    for symp in symp_to_hadmids.keys():
        for adm in symp_to_hadmids[symp]:
            graph.add_edge(symp, adm)

    # add micro to nodes
    micro_to_hadmids = get_micro_dict()
    add_nodes(micro_to_hadmids.keys(), graph)
    for micro in micro_to_hadmids.keys():
        for adm in micro_to_hadmids[micro]:
            graph.add_edge(micro, adm)

    # add proce to nodes
    proce_to_hadmids = get_proce_dict()
    add_nodes(proce_to_hadmids.keys(), graph)
    for proce in proce_to_hadmids.keys():
        for adm in proce_to_hadmids[proce]:
            graph.add_edge(proce, adm)

    # add presc to nodes
    presc_to_hadmids = get_presc_dict()
    add_nodes(presc_to_hadmids.keys(), graph)
    for presc in presc_to_hadmids.keys():
        for adm in presc_to_hadmids[presc]:
            graph.add_edge(presc, adm)

    return graph

# This function appends diagnose-diagnose edges into the HIN. The diagnose-diagnose are extracted from the ICD-9-CM dataset.
def append_dd(graph):
    all_diags = get_diagnose_dict().keys()
    all_diag_class = {}
    for diag in all_diags:
        diag_class = diag[:4]
        if diag_class not in all_diag_class.keys():
            all_diag_class[diag_class] = []
        all_diag_class[diag_class].append(diag)
    
    for diag_class in all_diag_class.keys():
        diag_pairs = list(itertools.combinations(all_diag_class[diag_class], 2))
        for pair in diag_pairs:
            graph.add_edge(pair[0], pair[1])
    
    return graph

# This function returns the total number of diagnose-diagnose edges in the HIN.
def count_dd():
    all_diags = get_diagnose_dict().keys()
    all_diag_class = {}
    for diag in all_diags:
        diag_class = diag[:4]
        if diag_class not in all_diag_class.keys():
            all_diag_class[diag_class] = []
        all_diag_class[diag_class].append(diag)
    
    count_diag_pairs = 0
    for diag_class in all_diag_class.keys():
        diag_pairs = list(itertools.combinations(all_diag_class[diag_class], 2))
        count_diag_pairs += len(diag_pairs)
        
    return count_diag_pairs

# This function appends symptom-symptom edges into the HIN. The symptom-symptom are extracted from the MeSH dataset.
def append_ss(graph):
    all_symps = get_symp_dict().keys()
    all_symp_class = {}
    for symp in all_symps:
        symp_class = symp[:-4]
        if symp_class not in all_symp_class.keys():
            all_symp_class[symp_class] = []
        all_symp_class[symp_class].append(symp)
    
    for symp_class in all_symp_class.keys():
        symp_pairs = list(itertools.combinations(all_symp_class[symp_class], 2))
        for pair in symp_pairs:
            graph.add_edge(pair[0], pair[1])
    
    return graph

# This function returns the total number of symptom-symptom edges in the HIN.
def count_ss():
    all_symps = get_symp_dict().keys()
    all_symp_class = {}
    for symp in all_symps:
        symp_class = symp[:-4]
        if symp_class not in all_symp_class.keys():
            all_symp_class[symp_class] = []
        all_symp_class[symp_class].append(symp)

    count_ss_pairs = 0
    for symp_class in all_symp_class.keys():
        symp_pairs = list(itertools.combinations(all_symp_class[symp_class], 2))
        count_ss_pairs += len(symp_pairs)
        
    return count_ss_pairs

# This function appends procedure-procedure edges into the HIN. The procedure-procedure are extracted from the ICD-9-CM dataset.
def append_oo(graph):
    all_proces = get_proce_dict().keys()
    all_proce_class = {}
    for proce in all_proces:
        proce_class = proce[:3]
        if proce_class not in all_proce_class.keys():
            all_proce_class[proce_class] = []
        all_proce_class[proce_class].append(proce)
    
    for proce_class in all_proce_class.keys():
        proce_pairs = list(itertools.combinations(all_proce_class[proce_class], 2))
        for pair in proce_pairs:
            graph.add_edge(pair[0], pair[1])
    
    return graph

# This function returns the total number of procedure-procedure edges in the HIN.
def count_oo():
    all_proces = get_proce_dict().keys()
    all_proce_class = {}
    for proce in all_proces:
        proce_class = proce[:3]
        if proce_class not in all_proce_class.keys():
            all_proce_class[proce_class] = []
        all_proce_class[proce_class].append(proce)

    count_oo_pairs = 0
    for proce_class in all_proce_class.keys():
        proce_pairs = list(itertools.combinations(all_proce_class[proce_class], 2))
        count_oo_pairs += len(proce_pairs)
        
    return count_oo_pairs

# This function follows meta-path to conduct random-walks, and write the resulted meta-path instances to out_ins file.
def gen_metains(metapath_s, graph, adm_list, out_ins, walklen = 1, test_speed = True):
    metapath = ''
    for _ in range(walklen):
        metapath += metapath_s
    if test_speed:
        count = 0
        start_time = time.time()

    #metains_list = []
    for patient in adm_list:
        metains = ''
        this = patient
        metains += str(this)

        left_n = this
        repeat_flag = 0
        m = 1
        inside_m = 1

        while m < len(metapath):
            next_type = metapath[m]
            next_candidate = [] 
            for each in list(graph.adj[this]):
                #print(each)
                if each[0] == next_type:
                    next_candidate.append(each)
            #print(next_candidate)
            if len(next_candidate) == 0:
                metains = metains.rsplit(' ', 1)[0]
                m -= 1
                if m == inside_m:
                    repeat_flag += 1
                else:
                    inside_m = m
                    repeat_flag = 1
                                
                if repeat_flag >= 10:
                    break
                else:
                    this = left_n
                    left_n = left_2n
                    continue

            left_2n = left_n
            left_n = this
            this = sample(next_candidate, 1)[0]
            metains += ' '
            metains += str(this)
            m += 1

        #metains_list.append(metains)
        if test_speed:
            count += 1
            if count % 5000 == 0:
                elapsed_time = time.time() - start_time
                start_time = time.time()
                print(time.strftime("%H:%M:%S", time.gmtime(elapsed_time)))
        #print(metains)
        out_ins.write(metains + "\n")

    #return metains_list

# This function writes a list of all patients to 'adm_list.pkl'
def get_adm_list(graph, path):
    all_nodes = list(graph.nodes)
    patients = [adm for adm in all_nodes if adm[0] == 'p']
    with open(path + 'adm_list.pkl', 'wb') as f:
        pkl.dump(patients,f)

if __name__ == '__main__':
    """
    # Construct the initial HIN
    #name = "mimic_nx_0730"

    print('begin')
    graph = gen_graph()
    file = open('./%s.pkl' % name, 'wb')
    pkl.dump(graph, file)
    file.close()
    print('graph_finished')
    
    # Append diagnose-diagnose and symptom-symptom edges into the HIN
    graph_dd = append_dd(graph)
    graph_dd_ss = append_ss(graph_dd)
    print()
    print(nx.info(graph_dd_ss))
    new_name = "mimic_nx_0806_dd_ss"
    file = open('./%s.pkl' % new_name, 'wb')
    pkl.dump(graph_dd_ss, file)
    
    # Append procedure-procedure edges into the HIN
    graph_dd_ss_oo = append_oo(graph)
    new_name = "mimic_nx_0806_dd_ss_oo"
    file = open('./%s.pkl' % new_name, 'wb')
    pkl.dump(graph_dd_ss_oo, file)
    
    # Conduct ramdom-walks
    #metapath = 'pdplpdpl'
    metapath = 'papeplpsptplpdpspgpmpopddpss'
    #adm_list = ['p145834', 'p185777', 'p107064', 'p150750', 'p194540']
    get_adm_list(graph, outpath)
    with open(outpath + 'adm_list.pkl', 'rb') as f:
        adm_list = pkl.load(f)
    #print(adm_list)
    #adm_list = sample(adm_list_all, 50)
    for k in range(200):
        print('----------Start to sample ' + str(k+1) + 'th file.----------')
        #k = 0
        out_ins = open(outpath + str(k) + strftime("_%m%d_%H%M%S", gmtime()) + ".txt", 'w')
        gen_metains(metapath, graph, adm_list, out_ins, walklen = 6)
        out_ins.close()
    
    # Count procedure-patient edges.
    proce_to_hadmids = get_proce_dict()
    print("# of unique procedures:" + str(len(proce_to_hadmids.keys())))
    num_op_edges = 0
    for each in proce_to_hadmids.keys():
        num_op_edges += len(proce_to_hadmids[each])
    print("# of procedure-patient edges:" + str(num_op_edges))

    # Count symptom-symptom edges.
    num_ss_pairs = count_ss()
    print("# of ss pairs: " + str(num_ss_pairs))

    # Count procedure-procedure edges.
    num_oo_pairs = count_oo()
    print("# of oo pairs: " + str(num_oo_pairs))
    """
    