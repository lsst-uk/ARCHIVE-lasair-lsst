import sys, json
sys.path.append('/home/ubuntu/lasair-lsst/services/annotations')
from stream2database import process_annotations

annotations = json.loads(open('sample_annotations.json').read())

queries_from_code = process_annotations(annotations)

queries_from_file = json.loads(open('sample_queries.json').read())

for i in range(len(queries_from_file)):
    if queries_from_code[i] != queries_from_file[i]:
        print('different', queries_from_code[i], queries_from_file[i])
