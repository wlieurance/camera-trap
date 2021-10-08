#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@created: 2020-06-29
@author: Wade Lieurance

This script will subset a photo database to specific criteria and then (optionally) copy the subset of photos to a new
base path.
"""

import argparse
import os
import copy
import shutil
import sqlite3 as sqlite
import pandas
from sample import construct_seq_list


def delete_photos(dbpath, sel_sql, params, verbose=False):
    delphoto_sql = "WITH recs AS ({}) " \
                   "DELETE FROM photo WHERE NOT EXISTS (" \
                   "  SELECT md5hash" \
                   "    FROM recs" \
                   "   WHERE photo.md5hash = recs.md5hash" \
                   ");".format(sel_sql)
    delseq_sql = """
    WITH seqs AS (
    SELECT seq_id FROM animal GROUP BY seq_id
    )
    
    DELETE FROM sequence
    WHERE NOT EXISTS (
      SELECT seq_id FROM seqs WHERE sequence.seq_id = seqs.seq_id
    );"""


    delcam_sql = """
    WITH cams AS (
    SELECT site_name, camera_id
      FROM photo
     GROUP BY site_name, camera_id
    )

    DELETE FROM camera
    WHERE NOT EXISTS (
      SELECT site_name, camera_id
      FROM cams
      WHERE camera.site_name = cams.site_name
      AND camera.camera_id = cams.camera_id
    );"""

    delsite_sql = """
    DELETE FROM site
    WHERE NOT EXISTS (
      SELECT site_name
        FROM camera
       WHERE camera.site_name = site.site_name
    );"""
    conn = sqlite.connect(dbpath)
    c = conn.cursor()
    if verbose:
        conn.set_trace_callback(print)
    conn.execute('PRAGMA foreign_keys = ON;')
    print("deleting unselected photo records...")
    c.execute(delphoto_sql, params)
    print("updating sequences...")
    c.execute(delseq_sql)
    print("updating camera tables...")
    c.execute(delcam_sql)
    print("updating site tables...")
    c.execute(delsite_sql)
    conn.commit()
    print('vacuuming database...')
    conn.execute("VACUUM;")
    conn.close()


def copy_single(base_old, base_new, photo_path):
    old_path = os.path.join(base_old, photo_path)
    new_path = os.path.join(base_new, photo_path)
    print('copying', old_path, 'to', new_path)
    os.makedirs(os.path.dirname(new_path), exist_ok=True)
    shutil.copyfile(old_path, new_path)


def copy_photos(dbpath, base_old, base_new):
    sql = "SELECT * FROM photo;"
    conn = sqlite.connect(dbpath)
    photos = pandas.read_sql_query(sql, conn)
    photos.apply(lambda row: copy_single(base_old, base_new, row['path']), axis=1)
    conn.close()


def get_photos(dbpath, animal, date_range, site_name, camera, seq_id, verbose=False, df=True):
    """pulls photo data from the database given the given script arguments and stores in pandas df.
    animal, site_name, camera and seq_id can be single items or lists. date_range needs to be a list of 2 items."""

    sql = "SELECT a.md5hash, a.path, a.fname, a.site_name, a.dt_orig, a.camera_id, b.id, b.cnt, b.seq_id" \
          "  FROM photo AS a" \
          " LEFT JOIN animal AS b ON a.md5hash = b.md5hash"
    param_list = []
    where = []
    if animal is not None:
        where.append("b.id IN ({})".format(', '.join('?' * len(animal))))
        param_list.extend(animal)
    if date_range is not None:
        assert len(date_range) == 2, "Date range does not have 2 values."
        assert len(date_range[0].split('-')) == len(date_range[1].split('-')), \
            'Date ranges are of different format.'
        assert 2 <= len(date_range[0].split('-')) <= 3, "Date ranges given in incorrect format."
        if len(date_range[0].split('-')) == 3:
            where.append("date(substr(a.dt_orig, 1, 19)) BETWEEN ? AND ?")
        elif len(date_range[0].split('-')) == 2:
            where.append("strftime('%m-%d', date(substr(a.dt_orig, 1, 19))) BETWEEN ? AND ?")
        param_list.extend(date_range)
    if site_name is not None:
        where.append("a.site_name IN ({})".format(', '.join('?' * len(site_name))))
        param_list.extend(site_name)
    if camera is not None:
        where.append("a.camera_id IN ({})".format(', '.join('?' * len(camera))))
        param_list.extend(camera)
    if seq_id is not None:
        where.append("b.seq_id IN ({})".format(', '.join('?' * len(seq_id))))
        param_list.extend(seq_id)
    if where:
        sql += " WHERE " + " AND ".join(where)

    if df:
        conn = sqlite.connect(dbpath)
        if verbose:
            print("parameter list:", param_list)
            conn.set_trace_callback(print)

        photos = pandas.read_sql_query(sql, conn, params=param_list)
        conn.close()
        return sql, param_list, photos
    else:
        return sql, param_list, None


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=os.linesep.join(('This script will subset a photo database and optionally copy over photos to',
                                     'a new base path.')))
    # positional arguments
    parser.add_argument('dbpath', help='path to sqlite database.')
    parser.add_argument('base_path', help='base folder for photos.')
    parser.add_argument('new_dbpath', help='path to store new subset database')
    parser.add_argument('-n', '--new_base', help='The new base path to which to copy the subset photos.')
    parser.add_argument('-a', '--animal', nargs='*', help='The id of the animal(s) to restrict photos to '
                                                           '(e.g. "Equus ferus caballus").')
    parser.add_argument('-d', '--date_range', nargs=2, help='date ranges to filter by (2 arguments in YYYY-MM-DD)'
                        ' format or MM-DD format.')
    parser.add_argument('-s', '--site_name', nargs='*', help='site name(s) to filter by (e.g. "Austin" '
                        '"Becky Springs").')
    parser.add_argument('-c', '--camera', nargs='*', help='camera identifier(s) for those sites with multiple cameras.')
    parser.add_argument('-q', '--seq_id', nargs='+',
                        help='Specific sequence id(s) to subset to. At least one seq_id must'
                             'be provided.')
    parser.add_argument('-Q', '--seq_file',
                        help='The local path to a delimited file containing seq_ids to subset (1 per row, no header).')
    args = parser.parse_args()

    my_seqs = copy.deepcopy(args.seq_id)
    args.seq_id = construct_seq_list(args.seq_file, my_seqs)
    print('retrieving photos from database...')
    my_sql, my_params, my_photos = get_photos(args.dbpath, args.animal, args.date_range, args.site_name, args.camera,
                                   args.seq_id, verbose=False, df=True)
    if len(my_photos) == 0:
        print("No photos match script criteria. Quitting...")
        quit()

    print("copying database from", args.dbpath, "to", args.new_dbpath)
    shutil.copyfile(args.dbpath, args.new_dbpath)

    delete_photos(args.new_dbpath, my_sql, my_params)
    if args.new_base:
        copy_photos(args.new_dbpath, args.base_path, args.new_base)
