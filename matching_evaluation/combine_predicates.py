import argparse
import dedupe
import pickle

def get_predicates(settings_file):
    with open(settings_file,'rb') as sf:
        data_model = pickle.load(sf)
        classifier = pickle.load(sf)
        predicates = pickle.load(sf)
    return predicates

def combine_predicates(predicates_1, predicates_2, indexes):
    predicates_1_ls = list(predicates_1)
    predicates_2_ls = list(predicates_2)
    for index in indexes:
        predicates_1_ls.append(predicates_2_ls[int(index)])
    return predicates_1_ls

def build_combined_settings(first_settings,second_settings, indexes):
    predicates_2 = get_predicates(second_settings)
    with open(first_settings, 'rb') as sfi:
        deduper = dedupe.StaticDedupe(sfi, num_cores=4)
    predicates_1 = deduper.predicates
    predicates_combined = combine_predicates(predicates_1,predicates_2,indexes)
    deduper.predicates = predicates_combined
    with open(first_settings+'_comb', 'wb') as sfo:
        pickle.dump(deduper.data_model, sfo)
        pickle.dump(deduper.classifier, sfo)
        pickle.dump(deduper.predicates, sfo)
    


if __name__ == '__main__':
    parser = argparse.ArgumentParser(prog='compare predicates')
    parser.add_argument('first_settings', help='first settings file')
    parser.add_argument('second_settings', help='second settings file')
    parser.add_argument('indexes', help='an array of the indexes of the predicate tuples you want to add')
    args = parser.parse_args()
    indexes_arr = args.indexes.split(',')
    build_combined_settings(args.first_settings, args.second_settings, indexes_arr)
   
    print('Finished')