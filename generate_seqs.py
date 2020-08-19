#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@created: 2020-03-11
@author: Wade Lieurance

This script will generate a sample of animal sequences given script filtering parameters and store them in a csv file.
"""

import sqlite3 as sqlite
import argparse
import pandas
from sample import get_photos


def enclose_with_sql(sql):
    """enclose the returned sql from get_photos() with a WITH statement to allow further processing."""
    new_sql = ' \n '.join(("WITH valid AS (", sql.replace(';', ''), ")"))
    return new_sql


def limit_by_condition(sql, filter_condition):
    """applies a filter to the original sql if condition_filter==True no remove any records with a value in the
    condition_seqs table"""
    if filter_condition:
        filt_sql = """
        , filtered AS (
        SELECT a.*
          FROM valid AS a
          LEFT JOIN  condition_seqs AS b ON a.seq_id = b.seq_id
         WHERE b.seq_id IS NULL)
        """
    else:
        filt_sql = """
        , filtered AS (
        SELECT * FROM valid)
        """
    new_sql = ' '.join((sql, filt_sql))
    return new_sql


def get_seqs(dbpath, sql, seq_no):
    """returns a list of seqs based on the given criteria"""
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    c = con.cursor()
    group_sql = "SELECT seq_id, count(md5hash) AS n FROM filtered GROUP BY seq_id"
    if seq_no is None:
        new_sql = ' \n '.join((sql, group_sql))
    else:
        rando_sql = "ORDER BY random() LIMIT {seq_no}".format(seq_no=seq_no)
        new_sql = ' \n '.join((sql, group_sql, rando_sql))
    rows = c.execute(new_sql)
    seq_list = []
    for row in rows:
        seq_list.append(row['seq_id'])

    con.close()
    return new_sql, seq_list


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='This script will generate a csv of randomly sampled animal sequences given user criteria.')
    # positional arguments
    parser.add_argument('dbpath', help='path to sqlite database.')
    parser.add_argument('seq_file', help='The local path to a delimited file in which to store the sampled sequences.')
    parser.add_argument('-C', '--classifier', help='the name of the person who classified the photo to filter by.')
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
    parser.add_argument('-n', '--no_seqs', type=int, help='limit output to n sequences.')
    parser.add_argument('-f', '--filter_condition', action='store_true', help='limit output sequences to those not '
                                                                              'already stored in the condition_seqs '
                                                                              'table.')
    # args = parser.parse_args()

    # args = parser.parse_args([r'C:\Users\wlieurance\Documents\temp\horse_subset\horse_subset.sqlite',
    #                           r'C:\Users\wlieurance\Documents\temp\test.csv', '-a', 'Equus ferus caballus',
    #                           '-d',])

    my_sql, my_params, my_photos = get_photos(dbpath=args.dbpath, animal=args.animal, date_range=args.date_range,
                                              site_name=args.site_name, camera=args.camera, seq_id=args.seq_id,
                                              classifier=args.classifier, df=False)
    with_sql = enclose_with_sql(sql=my_sql)
    filt_sql = limit_by_condition(sql=my_sql, filter_condition=args.filter_condition)
    final_sql, seqs = get_seqs(dbpath=args.dbpath, sql=filt_sql, seq_no=args.seq_no)

