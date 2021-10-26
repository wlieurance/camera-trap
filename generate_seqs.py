#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@created: 2020-03-11
@author: Wade Lieurance

This script will generate a sample of animal sequences given script filtering parameters and store them in a csv file.
"""

import sqlite3 as sqlite
import argparse
import csv
import pandas as pd
import os
import random
import math
import hashlib
from datetime import datetime
from datetime import date
from collections.abc import Iterable

# local
from sample import get_photos


def enclose_with_sql(sql):
    """enclose the returned sql from get_photos() with a WITH statement to allow further processing."""
    new_sql = ' \n '.join(("WITH valid AS (", sql.replace(';', ''), " ", ")"))
    return new_sql


def limit_by_condition(sql, filter_condition):
    """applies a filter to the original sql if condition_filter==True no remove any records with a value in the
    condition_seqs table"""
    # Will convert to pandas df if this gets too messy.  Would probably require altering some functions in sample.py
    if filter_condition:
        filt_sql = '\n'.join((
            ", filtered AS ( ",
            "SELECT a.* ",
            "  FROM valid AS a ",
            "  LEFT JOIN (",
            "       SELECT seq_id ",
            "         FROM condition_seqs ",
            "        GROUP BY seq_id) AS b ON a.seq_id = b.seq_id ",
            " WHERE b.seq_id IS NULL",
            " ",
            ")"))
    else:
        filt_sql = '\n'.join((
            ", filtered AS ( ",
            "SELECT * FROM valid",
            " ",
            ")"))
    new_sql = ''.join((sql, filt_sql))
    return new_sql


def limit_by_generated(sql, filter_generated):
    """applies a filter to the original sql if generated_filter==True no remove any records with a value in the
    sequences_gen table"""
    if filter_generated:
        filt_sql = '\n'.join((
            ", gen_filtered AS ( ",
            "SELECT a.* ",
            "  FROM filtered AS a ",
            "  LEFT JOIN (",
            "       SELECT seq_id ",
            "         FROM sequence_gen ",
            "        GROUP BY seq_id) AS b ON a.seq_id = b.seq_id ",
            " WHERE b.seq_id IS NULL",
            " ",
            ")"))
    else:
        filt_sql = '\n'.join((
            ", gen_filtered AS ( ",
            "SELECT * FROM filtered",
            " ",
            ")"))
    new_sql = ''.join((sql, filt_sql))
    return new_sql


def get_seqs(dbpath, sql, params, seq_no, verbose=False):
    """returns a list of seqs based on the given criteria"""
    print("getting sequences...")
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    if verbose:
        con.set_trace_callback(print)
    c = con.cursor()
    group_sql = "SELECT seq_id, count(md5hash) AS n FROM gen_filtered GROUP BY seq_id"
    if seq_no is None:
        new_sql = ' \n '.join((sql, group_sql))
    else:
        rando_sql = "ORDER BY random() LIMIT ?"
        new_sql = ' \n '.join((sql, group_sql, rando_sql))
        params.append(seq_no)
    rows = c.execute(new_sql, params)
    seq_list = []
    for row in rows:
        seq_list.append(row['seq_id'])
    con.close()
    return new_sql, params, seq_list


def write_csv(outfile, seqs, params=None, comment=False, overwrite=False, subsample=None):
    print("writing sequences...")
    file_list = [outfile]
    seq_list = [seqs]
    if overwrite:
        open_type = 'w+'
    else:
        open_type = 'a+'
    if subsample is not None:
        to_sample = math.ceil(len(seqs) * subsample)
        seqs_sub = random.sample(seqs, to_sample)
        new_file_list = os.path.splitext(outfile)
        new_file = ''.join((new_file_list[0], '_sub', new_file_list[1]))
        file_list.append(new_file)
        seq_list.append(seqs_sub)
    for i in range(0, len(file_list)):
        with open(file_list[i], open_type, newline='') as csvfile:
            if comment is True and params is not None:
                param_stmt = ' '.join(('\n#params for seqs:', ', '.join([str(x) for x in params]), '\n'))
                csvfile.write(param_stmt)
            writer = csv.writer(csvfile, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)
            for s in seq_list[i]:
                writer.writerow([s])


def construct_opt_list(my_args):
    """takes arguments from csv file instead of command line.  field names must match exactly.
    currently deprecated due to difficulty of processing nargs='*' inputs in a csv file."""
    print("reading options from csv...")
    dtypes = {'seq_file': str, 'classifier': str, 'animal': str, 'date_range': str, 'site_name': str, 'camera': str,
              'overwrite': bool, 'seq_no': int, 'filter_condition': bool}
    arg_df = pd.read_csv(my_args['csvfile'], sep=',', dtype=dtypes)
    arg_list = []
    colnames = list(arg_df)
    for key, value in my_args.items():
        if key not in colnames and key not in ['csvfile', 'base_path']:
            arg_df[key] = value
    for index, row in arg_df.iterrows():
        dict_row = row.to_dict()
        for k in ['classifier', 'animal', 'site_name', 'camera', 'date_range']:
            if dict_row.get(k) is not None:
                dict_row[k] = dict_row[k].split(' ')
        if dict_row['seq_file'][0:2] in ['./', '.\\']:
            dict_row['seq_file'] = os.path.join(my_args['base_path'], (dict_row['seq_file'])[2:])
        arg_list.append(dict_row)
    return arg_list


def pop_generation(dbpath, script_vars, seqs):
    """populates the db with the results of a sequences generation for later references."""
    gen_dt = datetime.now()
    hash = hashlib.md5()
    hash.update(str(gen_dt).encode('utf-8'))
    gen_id = hash.hexdigest()
    gen_sql = '\n'.join((
        "INSERT INTO generation (gen_id, gen_dt, dbpath, seq_file, classifier, animal, date_range, site_name, camera, ",
        "                        overwrite, seq_no, filter_condition, filter_generated, subsample, label) VALUES ",
        "(:gen_id, :gen_dt, :dbpath, :seq_file, :classifier, :animal, :date_range, :site_name, :camera, ",
        "                        :overwrite, :seq_no, :filter_condition, :filter_generated, :subsample, :label);"))
    script_vars['gen_dt'] = date.today().isoformat()
    script_vars['gen_id'] = gen_id
    formatted_vars = dict()
    for key, value in script_vars.items():
        if isinstance(value, Iterable) and not isinstance(value, str):
            formatted_vars[key] = '; '.join([str(x) for x in value])
        else:
            formatted_vars[key] = value
    conn = sqlite.connect(dbpath)
    c = conn.cursor()
    c.execute(gen_sql, formatted_vars)
    conn.commit()

    seq_list = [{'seq_id': x, 'gen_id': gen_id} for x in seqs]
    seq_sql = "INSERT INTO sequence_gen (seq_id, gen_id) VALUES (:seq_id, :gen_id);"
    c.executemany(seq_sql, seq_list)
    conn.commit()
    conn.close()


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='This script will generate a csv of randomly sampled animal sequences given user criteria.')
    # positional arguments
    parser.add_argument('dbpath', help='path to sqlite database.')
    parser.add_argument('-q', '--seq_file', help='The local path to a delimited file in which to store the sampled '
                                                 'sequences.')
    parser.add_argument('-C', '--classifier', nargs='*', help='the name of the person who classified the photo to '
                                                              'filter by.')
    parser.add_argument('-a', '--animal', nargs='*', help='The id of the animal(s) to restrict photos to '
                                                          '(e.g. "Equus ferus caballus").')
    parser.add_argument('-d', '--date_range', nargs=2, help='date ranges to filter by (2 arguments in YYYY-MM-DD)'
                        ' format or MM-DD format.')
    parser.add_argument('-s', '--site_name', nargs='*', help='site name(s) to filter by (e.g. "Austin" '
                        '"Becky Springs").')
    parser.add_argument('-c', '--camera', nargs='*', help='camera identifier(s) for those sites with multiple cameras.')
    parser.add_argument('-o', '--overwrite', action='store_true', help='overwrite an existing seq_file instead of '
                                                                       'appending to it, which is the default behavior'
                                                                       ').')
    parser.add_argument('-n', '--seq_no', type=int, help='limit output to n sequences.')
    parser.add_argument('-f', '--filter_condition', action='store_true', help='limit output sequences to those not '
                                                                              'already stored in the condition_seqs '
                                                                              'table.')
    parser.add_argument('-F', '--filter_generated', action='store_true', help='limit output sequences to those not '
                                                                              'already stored in the sequences_gen '
                                                                              'table (previously generated).')
    parser.add_argument('-S', '--subsample', type=float, help='the percentage to subsample the sequences for output '
                                                              'into a separate csv file (my_document_sub.csv).')
    parser.add_argument('-k', '--save', action='store_true',
                        help='Store the generated sequences in the ''generation'' and ''sequence_gen'' tables.')
    parser.add_argument('-l', '--label', help='A custom label to use in identifying the sequence.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Print out extra information such as queries used to generate sequences.')

    args = parser.parse_args()

    # args = parser.parse_args([r"D:\horse_subset\horse_subset.sqlite", "-q",
    #                           r"C:\Users\wlieurance\Documents\temp\test.csv", "-a",
    #                           "Equus ferus caballus", "-d", "2016-06-30", "2018-07-14", "-s", "Becky Springs", "-c",
    #                           "1", "-n", "20", "-f", "-F", "-S", "0.2"])

    my_sql, my_params, my_photos = get_photos(dbpath=args.dbpath, animal=args.animal,
                                              date_range=args.date_range, site_name=args.site_name,
                                              camera=args.camera, classifier=args.classifier, df=False)
    with_sql = enclose_with_sql(sql=my_sql)
    filt_sql = limit_by_condition(sql=with_sql, filter_condition=args.filter_condition)
    gen_sql = limit_by_generated(sql=filt_sql, filter_generated=args.filter_generated)
    final_sql, final_params, seqs = get_seqs(dbpath=args.dbpath, sql=gen_sql, params=my_params,
                                             seq_no=args.seq_no, verbose=args.verbose)
    print(len(seqs), "sequences found.")
    if args.save:
        print('Saving generated sequences...')
        pop_generation(dbpath=args.dbpath, script_vars=vars(args), seqs=seqs)
    else:
        print('Not saving generated sequences...')
    write_csv(outfile=args.seq_file, seqs=seqs, params=final_params, comment=True, overwrite=args.overwrite,
              subsample=args.subsample)


