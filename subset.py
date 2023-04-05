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
import re
from sample import get_photos, construct_seq_list
from create_db import create_db, create_indices
from generate_seqs import enclose_with_sql


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


def get_srid(dbpath):
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    con.enable_load_extension(True)
    con.execute("SELECT load_extension('mod_spatialite')")
    c = con.cursor()
    rows = c.execute("SELECT srid FROM geometry_columns WHERE f_table_name = 'camera'").fetchone()
    srid = list(rows)[0]
    con.close()
    return srid


def get_field_names(db, tbls):
    con = sqlite.connect(db)
    c = con.cursor()
    d = {}
    for t in tbls:
        rows = c.execute(f"SELECT name FROM PRAGMA_TABLE_INFO('{t}');")
        fields = [list(x)[0] for x in rows.fetchall()]
        d[t] = fields
    con.close()
    return d


def copy_data(orig_db, new_db, sql, params, tags=False):
    print("copying records from", orig_db, "to", new_db)
    tbls = ['animal', 'camera', 'condition', 'condition_seqs', 'generation', 'hash', 'import', 'photo', 'sequence',
            'sequence_gen', 'site', 'tag']
    valid_sql = re.sub(r"ORDER BY .+", "", sql)
    fields = get_field_names(db=orig_db, tbls=tbls)
    con = sqlite.connect(orig_db)
    con.row_factory = sqlite.Row
    con.enable_load_extension(True)
    con.execute("SELECT load_extension('mod_spatialite')")
    c = con.cursor()
    c.execute("ATTACH DATABASE ? AS new;", (new_db,))

    print("\tcopying from photo...")
    insert_sql = '\n'.join((
        f"INSERT INTO new.photo ({', '.join(fields['photo'])})",
        f"SELECT {', '.join(['a.' + x for x in fields['photo']])}",
        "  FROM photo a",
        " INNER JOIN valid b ON a.path = b.path;"))
    c.execute('\n'.join((valid_sql, insert_sql)), params)
    print("\tcopying from hash...")
    insert_sql = '\n'.join((
        "WITH h AS (SELECT md5hash FROM new.photo GROUP BY md5hash)",
        f"INSERT INTO new.hash ({', '.join(fields['hash'])})",
        f"SELECT {', '.join(['a.' + x for x in fields['hash']])}",
        "  FROM hash a",
        " INNER JOIN h ON a.md5hash = h.md5hash;"))
    c.execute(insert_sql)
    print("\tcopying from import...")
    insert_sql = '\n'.join((
        "WITH h AS (SELECT import_date FROM new.hash GROUP BY import_date)",
        f"INSERT INTO new.import ({', '.join(fields['import'])})",
        f"SELECT {', '.join(['a.' + x for x in fields['import']])}",
        "  FROM import a",
        " INNER JOIN h ON a.import_date = h.import_date;"))
    c.execute(insert_sql)
    if tags:
        print("\tcopying from tag...")
        insert_sql = '\n'.join((
            f"INSERT INTO new.tag ({', '.join(fields['tag'])})",
            f"SELECT {', '.join(['a.' + x for x in fields['tag']])}",
            "  FROM tag a",
            " INNER JOIN new.hash h ON a.md5hash = h.md5hash;"))
        c.execute(insert_sql)
    print("\tcopying from animal...")
    insert_sql = '\n'.join((
        f"INSERT INTO new.animal ({', '.join(fields['animal'])})",
        f"SELECT {', '.join(['a.' + x for x in fields['animal']])}",
        "  FROM animal a",
        " INNER JOIN new.hash h ON a.md5hash = h.md5hash;"))
    c.execute(insert_sql)
    print("\tcopying from camera...")
    insert_sql = '\n'.join((
        "WITH h AS (SELECT site_name, camera_id FROM new.photo GROUP BY site_name, camera_id)",
        f"INSERT INTO new.camera ({', '.join(fields['camera'])})",
        f"SELECT {', '.join(['a.' + x for x in fields['camera']])}",
        "  FROM camera a",
        " INNER JOIN h ON a.site_name = h.site_name AND a.camera_id = h.camera_id;"))
    c.execute(insert_sql)
    print("\tcopying from site...")
    insert_sql = '\n'.join((
        "WITH h AS (SELECT site_name FROM new.camera GROUP BY site_name)",
        f"INSERT INTO new.site ({', '.join(fields['site'])})",
        f"SELECT {', '.join(['a.' + x for x in fields['site']])}",
        "  FROM site a",
        " INNER JOIN h ON a.site_name = h.site_name;"))
    c.execute(insert_sql)
    print("\tcopying from sequence...")
    insert_sql = '\n'.join((
        "WITH h AS (SELECT seq_id FROM new.animal GROUP BY seq_id)",
        f"INSERT INTO new.sequence ({', '.join(fields['sequence'])})",
        f"SELECT {', '.join(['a.' + x for x in fields['sequence']])}",
        "  FROM sequence a",
        " INNER JOIN h ON a.seq_id = h.seq_id;"))
    c.execute(insert_sql)
    print("\tcopying from sequence_gen...")
    insert_sql = '\n'.join((
        "WITH h AS (SELECT seq_id FROM new.sequence GROUP BY seq_id)",
        f"INSERT INTO new.sequence_gen ({', '.join(fields['sequence_gen'])})",
        f"SELECT {', '.join(['a.' + x for x in fields['sequence_gen']])}",
        "  FROM sequence_gen a",
        " INNER JOIN h ON a.seq_id = h.seq_id;"))
    c.execute(insert_sql)
    print("\tcopying from generation...")
    insert_sql = '\n'.join((
        "WITH h AS (SELECT gen_id FROM new.sequence_gen GROUP BY gen_id)",
        f"INSERT INTO new.generation ({', '.join(fields['generation'])})",
        f"SELECT {', '.join(['a.' + x for x in fields['generation']])}",
        "  FROM generation a",
        " INNER JOIN h ON a.gen_id = h.gen_id;"))
    c.execute(insert_sql)
    print("\tcopying from condition...")
    insert_sql = '\n'.join((
        f"INSERT INTO new.condition ({', '.join(fields['condition'])})",
        f"SELECT {', '.join(['a.' + x for x in fields['condition']])}",
        "  FROM condition a",
        " INNER JOIN new.sequence h ON a.seq_id = h.seq_id;"))
    c.execute(insert_sql)
    print("\tcopying from condition_seqs...")
    insert_sql = '\n'.join((
        f"INSERT INTO new.condition_seqs ({', '.join(fields['condition_seqs'])})",
        f"SELECT {', '.join(['a.' + x for x in fields['condition_seqs']])}",
        "  FROM condition_seqs a",
        " INNER JOIN new.sequence h ON a.seq_id = h.seq_id;"))
    c.execute(insert_sql)
    con.commit()
    con.close()


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
    parser.add_argument('-a', '--animal', nargs='+', help='The id of the animal(s) to restrict photos to '
                                                          '(e.g. "Equus ferus caballus").')
    parser.add_argument('-A', '--animal_not', nargs='+', help='The id of the animal(s) to restrict from photos.')
    parser.add_argument('-l', '--animal_like', nargs='+', help='A string to partially match to animal id for inclusion '
                                                               'using SQL syntax (e.g. "Equus%%").')
    parser.add_argument('-L', '--animal_not_like', nargs='+', help='A string to partially match to animal id for '
                                                                   'exclusion using SQL syntax.')
    parser.add_argument('-d', '--date_range', nargs='+', help='date ranges to filter by (2 arguments in YYYY-MM-DD)'
                        ' format or MM-DD format for each date range')
    parser.add_argument('-s', '--site_name', nargs='+', help='site name(s) to filter by (e.g. "Austin" '
                        '"Becky Springs").')
    parser.add_argument('-c', '--camera', nargs='+', help='camera identifier(s) for those sites with multiple cameras.')
    parser.add_argument('-C', '--classifier', nargs='+', help='the name of the person who classified the photo to '
                                                              'filter by.')
    parser.add_argument('-v', '--verbose', action='store_true', help='include more verbose output for debugging.')
    parser.add_argument('-q', '--seq_id', nargs='+',
                        help='Specific sequence id(s) to retrieve instead of a random sample. At least one seq_id must'
                             'be provided.')
    parser.add_argument('-Q', '--seq_file',
                        help='The local path to a delimited file containing seq_ids to sample (1 per row, no header).')
    parser.add_argument('-t', '--tags', action='store_true', help='Copy photo EXIF tags from source database.')
    args = parser.parse_args()

    if args.date_range:
        if len(args.date_range) % 2 != 0:
            print("date_range argument must by a multiple of two. Quitting...")
            quit()
    my_seqs = copy.deepcopy(args.seq_id)
    args.seq_id = construct_seq_list(args.seq_file, my_seqs)
    my_sql, my_params, my_photos = get_photos(dbpath=args.dbpath, animal=args.animal, animal_not=args.animal_not,
                                              animal_like=args.animal_like, animal_not_like=args.animal_not_like,
                                              date_range=args.date_range, site_name=args.site_name, camera=args.camera,
                                              seq_id=args.seq_id, classifier=args.classifier, verbose=False, df=False)

    with_sql = enclose_with_sql(sql=my_sql)
    srid = get_srid(dbpath=args.dbpath)
    create_db(dbpath=args.new_dbpath, srid=srid, verbose=args.verbose)
    copy_data(orig_db=args.dbpath, new_db=args.new_dbpath, sql=with_sql, params=my_params, tags=args.tags)
    create_indices(dbpath=args.new_dbpath)
    if args.new_base:
        copy_photos(args.new_dbpath, args.base_path, args.new_base)
