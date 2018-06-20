#!/usr/bin/env python

import sys
import os
import json
from mysolr import Solr

PDBE_SOLR_URL = "http://www.ebi.ac.uk/pdbe/search/pdb"
solr = Solr(PDBE_SOLR_URL)

PY3 = sys.version > '3'

if PY3:
    import urllib.request as urllib2
else:
    import urllib2

SERVER_URL = "https://www.ebi.ac.uk/pdbe/api"

def join_with_AND(query_params) :
    '''convenience function to create query string with AND'''
    return " AND ".join(["%s:%s" % (k,v) for k,v in query_params.items()])

def execute_solr_query(query, query_fields) :
    '''convenience function'''
    query["q"] = join_with_AND(query_fields) # add q
    response = solr.search(**query)
    documents = response.documents
    print ("Found %d matching entities in %d entries." % (len(documents), len({rd["pdb_id"] for rd in documents})))
    return documents

def make_request(url, data):
    request = urllib2.Request(url)

    try:
        url_file = urllib2.urlopen(request, data)
    except urllib2.HTTPError as e:
        if e.code == 404:
            print("[NOTFOUND %d] %s" % (e.code, url))
        else:
            print("[ERROR %d] %s" % (e.code, url))

        return None

    return url_file.read().decode()

def get_request(url, arg, pretty=False):
    full_url = "%s/%s/%s?pretty=%s" % (SERVER_URL, url, arg, str(pretty).lower())

    return make_request(full_url, None)

def post_request(url, data, pretty=False):
    full_url = "%s/%s/?pretty=%s" % (SERVER_URL, url, str(pretty).lower())

    if isinstance(data, (list, tuple)):
        data = ",".join(data)

    return make_request(full_url, data.encode())


query_detail = {
    "q_molecule_type"  : "Protein",
    "q_mutation_type": "Engineered\ mutation",
    #"tax_id"        : "294",
    #"pdb_id": "1bgj",
}

# http://www.ebi.ac.uk/pdbe/entry/search/index/?searchParams=%7B%22q_molecule_type%22:%5B%7B%22value%22:%22protein%22,%22condition1%22:%22AND%22,%22condition2%22:%22Contains%22%7D%5D,%22q_mutation_type%22:%5B%7B%22value%22:%22engineered%20mutation%22,%22condition1%22:%22AND%22,%22condition2%22:%22Contains%22%7D%5D,%22resultState%22:%7B%22tabIndex%22:0,%22paginationIndex%22:1,%22perPage%22:%2210%22,%22sortBy%22:%22Sort%20by%22%7D%7D
query = {
    "rows" : pow(10,8), # i.e. all matching documents are required in response
    "fl"   : "pdb_id", # restrict the returned documents to these fields only
}

docs = execute_solr_query(query, query_detail)

with open('pdb_ids.engineered_mutation.08-06-2018.list', 'w') as f:
    for entry in docs:
        #print(entry)
        for k,v in entry.items():
            #print(k,":",v, sep=" ",end="\n", file=f)
            print(v, file=f)

mutations = "/pdb/entry/mutated_AA_or_NA"

file= open('pdb_ids.engineered_mutation.list', 'r')

lines = file.read().splitlines()
listlen = len(lines)
print(listlen)

def get_pdbs_with_engineered_muts(data, filename):
    outfile = open(filename, 'a')
    response = post_request(mutations, data)
    entries = json.loads(response)
    for k in entries.keys():
        for res_entity in entries[k]:
            #print(res_entity)
            if (res_entity["mutation_details"]["type"] == "Engineered mutation") and (res_entity["author_residue_number"] is not None):
                print(k, res_entity["chain_id"], res_entity["residue_number"], res_entity["author_residue_number"], res_entity["author_insertion_code"], res_entity["mutation_details"]["from"],"->", res_entity["mutation_details"]["to"], file=outfile)
                #print('{:4}'.format(k), '{:1}'.format(res_entity["chain_id"]), '{:5}'.format(res_entity["residue_number"]), '{:5}'.format(res_entity["author_residue_number"]), '{:1}'.format(res_entity["author_insertion_code"]), '{:1}'.format(res_entity["mutation_details"]["from"]),"->", '{:1}'.format(res_entity["mutation_details"]["to"]), file=outfile)
    return None

import os;

outfilename = 'pdb_ids.engineered_mutation.data'

if os.path.exists(outfilename):
    os.remove(outfilename)

batch_query_limit = 500

batch=[]

for line in lines:
        if (len(batch) < batch_query_limit):
            batch.append(line)
        else:
            #print(batch)
            get_pdbs_with_engineered_muts(batch, outfilename)
            batch=[]
            batch.append(line)

#print(batch)
get_pdbs_with_engineered_muts(batch, outfilename)
