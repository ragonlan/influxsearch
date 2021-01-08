#!/usr/bin/env python3
import elasticsearch
import pprint

from elasticsearch import Elasticsearch

pp = pprint.PrettyPrinter(indent=4)


class PrettyLog():

    def __init__(self, obj):
        self.obj = obj

    def __repr__(self):
        return pprint.pformat(self.obj)
        self.obj = obj


def PrintDictToCSV(csv_columns, dict_data, char=';'):
    print(char.join(csv_columns))
    for index, data in enumerate(dict_data):
        # print(PrettyLog(data))
        values = []
        for col in csv_columns:
            values.append(data.get(col, 'null'))
            # print(data.get(col,'null'))
        print(char.join(values))
    return

# Busqueda Lucene a ejecutar
qsearch = '"a3ASESOR"'
# Nombre de los campos que quieres extraer de la busqueda:
fields = ['installed_software_reg::a3ASESOR::displayNameFull', 'installed_software_reg::A3ASESOR::displayNameFull',
          'xymon_hostname', 'public_ip']

query = {
    "query": {
        "bool": {
            "must": [
                {
                    "match_all": {}
                },
                {
                    "query_string": {
                        "query": qsearch
                    }
                },
            ],
        }
    },
    "size": 10000,
    "sort": [
        {
            "@timestamp": {
                "order": "desc",
                "unmapped_type": "boolean"
            }
        }
    ]
}

es = Elasticsearch(['localhost'], timeout=30)

buscaips = es.search(index="puppet", body=query, request_timeout=160)

docs = buscaips['hits']['hits']
result = list()

for doc in docs:
    # print(PrettyLog(doc))
    facts = doc['_source']['facts']
    s = facts.keys() & fields
    # print(PrettyLog(facts.keys()))
    fselected = {k: facts[k] for k in facts.keys() & fields}
    result.append(fselected)

# print(PrettyLog(result))
PrintDictToCSV(fields, result)
