#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Mar 11 11:01:47 2020

@author: Wade Lieurance
"""

import sqlite3 as sqlite
import argparse
import pandas
import os
import shutil
from datetime import datetime
from tzlocal import get_localzone


def get_sample_hashes(phtos, cnt):
    """Samples photos from certain sites based on animal number"""
    total_animals = 0
    hashes = []
    while total_animals < cnt and len(phtos) > 0:
        sample = phtos.sample(n=1)
        animals = sample['cnt'].iloc[0]
        hash = sample['md5hash'].iloc[0]
        total_animals += animals
        hashes.append(hash)
        phtos = phtos[(phtos['md5hash'] != hash)]
    return hashes


def copy_photos(inpath, outpath, out_paths):
    for path in out_paths:
        full_in = os.path.join(inpath, path)
        full_out = os.path.join(outpath, os.path.basename(path))
        print('copying', full_in, 'to', full_out)
        shutil.copyfile(full_in, full_out)


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=os.linesep.join(('This script will sample photos fitting certain '
                                     'descriptions and copy them to a new directory')))
    # positional arguments
    parser.add_argument('dbpath', help='path to sqlite database.')
    parser.add_argument('base_path', help='base folder for photos.')
    parser.add_argument('out_path', help='folder path to copy samples to.')
    parser.add_argument('num_sample', type=int, help='number of photos to sample.')
    parser.add_argument('-a', '--animal', nargs='*', help='The id of the animal(s) to restrict photos to '
                                                           '(e.g. "Equus ferus caballus").')
    parser.add_argument('-d', '--date_range', nargs=2, help='date ranges to filter by (2 arguments in YYYY-MM-DD)'
                        ' format or MM-DD format.')
    parser.add_argument('-s', '--site_name', nargs='*', help='site name(s) to filter by (e.g. "Austin" '
                        '"Becky Springs").')
    parser.add_argument('-c', '--camera', nargs='*', help='camera identifier(s) for those sites with multiple cameras.')
    parser.add_argument('-v', '--verbose', action='store_true', help='include more verbose output for debugging.')
    args = parser.parse_args()
    conn = sqlite.connect(args.dbpath)
    c = conn.cursor()

    sql = "SELECT a.md5hash, a.cnt, a.seq_id, b.path, b.fname, b.site_name, b.taken_dt, b.camera_id" \
          "  FROM animal AS a" \
          " INNER JOIN photo AS b ON a.md5hash = b.md5hash"
    param_list = []
    where = []
    if args.animal is not None:
        where.append("a.id IN ({})".format(', '.join('?' * len(args.animal))))
        param_list.extend(args.animal)
    if args.date_range is not None:
        assert len(args.date_range) == 2, "Date range does not have 2 values."
        assert len(args.date_range[0].split('-')) == len(args.date_range[1].split('-')), \
            'Date ranges are of different format.'
        assert 2 <= len(args.date_range[0].split('-')) <= 3, "Date ranges given in incorrect format."
        if len(args.date_range[0].split('-')) == 3:
            where.append("date(substr(b.taken_dt, 1, 19)) BETWEEN ? AND ?")
        elif len(args.date_range[0].split('-')) == 2:
            where.append("strftime('%m-%d', date(substr(b.taken_dt, 1, 19))) BETWEEN ? AND ?")
        param_list.extend(args.date_range)
    if args.site_name is not None:
        where.append("b.site_name IN ({})".format(', '.join('?' * len(args.site_name))))
        param_list.extend(args.site_name)
    if args.camera is not None:
        where.append("b.camera_id IN ({})".format(', '.join('?' * len(args.camera))))
        param_list.extend(args.camera)
    if where:
        sql += " WHERE " + " AND ".join(where)

    if args.verbose:
        print("parameter list:", param_list)
        conn.set_trace_callback(print)

    photos = pandas.read_sql_query(sql, conn, params=param_list)
    if len(photos) == 0:
        quit_script = True
        print("No photos match script criteria. Quitting...")
        conn.close()
        quit()

    sampled_hashes = get_sample_hashes(photos, args.num_sample)
    restrict = photos[photos['md5hash'].isin(sampled_hashes)]
    paths = restrict['path'].tolist()
    copy_photos(args.base_path, args.out_path, paths)
    conn.close()
    print('script finished.')
