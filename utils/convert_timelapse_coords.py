#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@created: 2022-08-19
@author: Wade Lieurance

This is a script to convert count and animal coordinate data in timelapse format and store it into a postgres
camera_trap database.  It is still in testing and will probably break in edge cases. It utilizes the
(also in testing ) 'extract_timelapse.R' script in the utils folder for pre-processing.
"""
import pandas as pd
import json
import numpy as np
from uuid import UUID
from photo_mgmt.create_db import get_pg_con

# get animal data generated from the 'extract_coords_timelapse.R' utils script stored as json.
animals = pd.read_json("my_path.json") \
    .assign(md5hash=lambda x: x.md5hash.map(UUID))
con = get_pg_con(user="my_user", database="my_db")
c = con.cursor()
file_sql = '\n'.join((
    "SELECT CASE WHEN c.local = True THEN c.base_path || '/' || a.path",
    "            ELSE a.path END path, ",
    "       a.md5hash, b.width, b.height ",
    "  FROM photo a",
    " INNER JOIN tag b ON a.md5hash = b.md5hash",
    " INNER JOIN import c ON a.dt_import = c.import_date;"
))
c.execute(file_sql)
rows = c.fetchall()
df = pd.DataFrame(rows) \
    .drop('md5hash', axis=1)

# get hashes to double check that files have already been read in before animal updates/inserts.
hash_sql = "SELECT * FROM hash;"
c.execute(hash_sql)
rows = c.fetchall()
hash = pd.DataFrame(rows) \
    .rename(columns={'md5hash': 'pghash'})


# Convert coordiinates from json values and convert to long format.
# in case of nans use
# .assign(coords=lambda x: x.coords.replace(np.nan, '[]', regex=True))\
locs = animals \
    .query('coords.notna() & coords != "" & coords != "[]"') \
    .assign(clist=lambda x: x.coords.map(json.loads)) \
    .drop('coords', axis=1) \
    .explode('clist') \
    .merge(df, on='path', how='left')

locs[['x1r', 'y1r']] = locs['clist'].str.split(r'\s*,\s*', regex=True, n=1, expand=True)
locs2 = locs \
    .astype({'x1r': 'float', 'y1r': 'float'})\
    .assign(x1=lambda z: round(z.x1r * z.width, 0),
            y1=lambda z: round(z.y1r * z.height, 0))\
    .astype({'x1': 'int32', 'y1': 'int32'})

locs_dict = locs2.to_dict('records')

# update classifier in case classifiers have changed. Generally shouldn't be necessary.
usql = '\n'.join((
    "UPDATE animal_loc SET classifier = %(class_name)s ",
    " WHERE md5hash = %(md5hash)s",
    "   AND id = %(id)s AND x1 = %(x1)s AND y1 = %(y1)s AND x2 IS NULL AND y2 IS NULL;"
))
# optional should only need doing once.
# c.executemany(usql, locs_dict)
# con.commit()

# get data for animal table and check for bad values before inserting
cnt = animals \
    .query('(count.notna() & count != 0) | coords != "[]"')\
    .merge(hash, how='left', left_on='md5hash', right_on='pghash')
cnt['count'] = cnt['count'].fillna(0)
missing = cnt \
    .query('pghash.isna()')
cnt_notmissing = cnt \
    .query('pghash.notna()')
cnt_dict = cnt_notmissing.to_dict('records')
isql = '\n'.join((
    "INSERT INTO animal (md5hash, id, cnt, classifier) VALUES (%(md5hash)s, %(id)s, %(count)s, %(class_name)s)",
    "ON CONFLICT ON CONSTRAINT animal_pkey DO UPDATE SET cnt=EXCLUDED.cnt, classifier=EXCLUDED.classifier;"
))
c.executemany(isql, cnt_dict)
inserted = c.rowcount
con.commit()

# insert data into animal_loc table
isql_loc = '\n'.join((
    "INSERT INTO animal_loc (md5hash, id, classifier, x1, y1) VALUES ",
    "(%(md5hash)s, %(id)s, %(class_name)s, %(x1)s, %(y1)s)",
    "ON CONFLICT DO NOTHING;"
))
c.executemany(isql_loc, locs_dict)
inserted_locs = c.rowcount
con.commit()

con.close()

