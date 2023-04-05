#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@created: 2020-08-13
@author: Wade Lieurance

This script will create and fill a camera trap database.
"""

import os
import argparse
import sqlite3 as sqlite
import pandas as pd
import numpy as np
import psycopg
import psycopg.rows
from photo_mgmt import create_db as cdb
from typing import Union
from getpass import getpass


def db_is_spatial(con):
    version = None
    is_spatial = False
    c = con.cursor()
    if isinstance(con, sqlite.Connection):
        c.execute("SELECT * FROM sqlite_master WHERE type = 'table' AND name = 'spatialite_history';")
        rows = c.fetchall()
        if len(rows) > 0:
            is_spatial = True
            c.execute("SELECT ver_splite FROM spatialite_history WHERE table_name = 'spatial_ref_sys';")
            rows2 = c.fetchone()
            if len(rows2) > 0:
                version = rows2['ver_splite']
    elif isinstance(con, psycopg.Connection):
        c.execute("SELECT * FROM pg_extension WHERE extname = 'postgis';")
        rows = c.fetchone()
        if len(rows) > 0:
            is_spatial = True
            version = rows['extversion']
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")
    return is_spatial, version


def create_animal_tables(con: Union[sqlite.Connection, psycopg.Connection], verbose: bool = False):
    cdb.create_tables(con=con, wipe=False, geo=True, verbose=verbose)
    c = con.cursor()
    spatial, spatver = db_is_spatial(con=con)
    if isinstance(con, sqlite.Connection):
        timestamp_tz = "TEXT"
        timestamp = "TEXT"
        camera_geom = ''
        site_geom = ''
    elif isinstance(con, psycopg.Connection):
        timestamp_tz = "TIMESTAMP WITH TIME ZONE"
        timestamp = "TIMESTAMP"
        if spatial:
            camera_geom = "geom GEOMETRY(POINTZ, 4326),"
            site_geom = "geom GEOMETRY(MULTIPOLYGON, 4326),"
        else:
            camera_geom = ''
            site_geom = ''
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")

    create_list = [
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS site(",
            "    site_name VARCHAR,",
            "    state_code VARCHAR (2),",
            f"    {site_geom}",
            "PRIMARY KEY(site_name));")),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS camera (",
            "    site_name VARCHAR,",
            "    camera_id VARCHAR,",
            "    fov_length_m FLOAT,",
            "    fov_area_sqm FLOAT,",
            "    lat FLOAT,",
            "    long FLOAT,",
            "    elev_m FLOAT,",
            f"    {camera_geom}",
            "    PRIMARY KEY(site_name, camera_id),",
            "    FOREIGN KEY(site_name) REFERENCES site(site_name) ON DELETE CASCADE ON UPDATE CASCADE);")),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS sequence (",
            "    seq_id VARCHAR,",
            "    site_name VARCHAR,",
            "    camera_id VARCHAR,",
            "    id VARCHAR,",
            "    seq INTEGER,",
            "    seq_part INTEGER,",
            f"    min_dt {timestamp_tz},",
            f"    max_dt {timestamp_tz},",
            "    FOREIGN KEY(site_name, camera_id) REFERENCES camera(site_name, camera_id)",
            "            ON DELETE CASCADE ON UPDATE CASCADE,",
            "    PRIMARY KEY(seq_id));")),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS generation (",
            "    gen_id VARCHAR,",
            f"    gen_dt {timestamp},",
            "    dbpath VARCHAR,",
            "    seq_file VARCHAR,",
            "    classifier VARCHAR,",
            "    animal VARCHAR,",
            "    date_range VARCHAR,",
            "    site_name VARCHAR,",
            "    camera VARCHAR,",
            "    overwrite BOOLEAN,",
            "    seq_no INTEGER,",
            "    filter_condition BOOLEAN,",
            "    filter_generated BOOLEAN,",
            "    partition VARCHAR,",
            "    subsample FLOAT,",
            "    label VARCHAR,",
            "    PRIMARY KEY(gen_id));")),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS sequence_gen (",
            "    seq_id VARCHAR,",
            "    gen_id VARCHAR,",
            "    FOREIGN KEY(seq_id) REFERENCES sequence(seq_id) ON DELETE RESTRICT ON UPDATE CASCADE,",
            "    FOREIGN KEY(gen_id) REFERENCES generation(gen_id) ON DELETE CASCADE,",
            "    PRIMARY KEY(seq_id, gen_id));")),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS animal (",
            "    md5hash VARCHAR(32),",
            "    id VARCHAR,",
            "    cnt INTEGER,",
            "    classifier VARCHAR,",
            "    seq_id VARCHAR,",
            "    PRIMARY KEY(md5hash, id),",
            "    FOREIGN KEY(md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE,",
            "    FOREIGN KEY(seq_id) REFERENCES sequence(seq_id) ON DELETE SET NULL ON UPDATE CASCADE);")),
        '\n'.join((
            "CREATE TABLE animal_loc (",
            "    md5hash VARCHAR(32) NOT NULL,",
            "    id VARCHAR NOT NULL,",
            "    classifier VARCHAR,",
            "    x1 INTEGER NOT NULL,",
            "    y1 INTEGER NOT NULL,",
            "    x2 INTEGER,",
            "    y2 INTEGER,",
            "    CONSTRAINT loc_unique UNIQUE (md5hash, id, classifier, x1, y1, x2, y2),",
            "    FOREIGN KEY (md5hash, id) REFERENCES animal(md5hash, id) ON UPDATE CASCADE ON DELETE CASCADE)"
        )),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS condition (",
            "    md5hash VARCHAR(32),",
            "    seq_id VARCHAR,",
            "    rating NUMERIC,",
            "    scorer_name VARCHAR,",
            f"    score_dt {timestamp},",
            "    bbox_x1 INTEGER,",
            "    bbox_y1 INTEGER,",
            "    bbox_x2 INTEGER,",
            "    bbox_y2 INTEGER,",
            "    PRIMARY KEY(md5hash, seq_id, scorer_name, bbox_x1, bbox_y1, bbox_x2, bbox_y2),",
            "    FOREIGN KEY(seq_id, scorer_name) REFERENCES condition_seqs(seq_id, scorer_name)",
            "            ON DELETE RESTRICT ON UPDATE CASCADE,",
            "    FOREIGN KEY(md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE);")),
        '\n'.join((
            "CREATE TABLE IF NOT EXISTS condition_seqs (",
            "    seq_id VARCHAR,",
            "    scorer_name VARCHAR,",
            "    scores BOOLEAN,",
            "    PRIMARY KEY(seq_id, scorer_name),",
            "    FOREIGN KEY(seq_id) REFERENCES sequence(seq_id) ON DELETE RESTRICT ON UPDATE CASCADE);"))
    ]
    for sql in create_list:
        if verbose:
            print(sql)
            print('-----------------------------------------------------')
        c.execute(sql)

    # db specific alterations
    if isinstance(con, sqlite.Connection):
        if spatial:
            c.execute("SELECT AddGeometryColumn('camera', 'geometry', 4326, 'POINTZ');")
            c.execute("SELECT AddGeometryColumn('site', 'geometry', 4326, 'MULTIPOLYGON');")
        # dealing with the SQLite specific need to add a foreign key to an existing table
        c.execute("PRAGMA table_info(photo);")
        rows = c.fetchall()
        cols = [r['name'] for r in rows]
        new_cols = ['site_name', 'camera_id', 'year_orig', 'season_no', 'season_order']
        if not all([x in cols for x in new_cols]):
            c.execute("SELECT type, sql FROM sqlite_schema WHERE tbl_name='photo';")
            old_sql = c.fetchall()
            old_index_triggers = [x['sql'] for x in old_sql if x['type'] != 'table' and x['sql'] is not None]
            new_photo_sql = '\n'.join((
                "CREATE TABLE photo_new (",
                "    path TEXT PRIMARY KEY,",
                "    fname TEXT,",
                "    ftype TEXT,",
                "    md5hash TEXT,",
                "    dt_orig TEXT,",
                "    dt_mod TEXT,",
                "    dt_import TEXT,",
                "    site_name TEXT,",
                "    camera_id TEXT,",
                "    year_orig INTEGER,",
                "    season_no INTEGER, ",
                "    season_order INTEGER,",
                "    FOREIGN KEY (md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE ON UPDATE CASCADE,",
                "    FOREIGN KEY (dt_import) REFERENCES import(import_date) ON DELETE RESTRICT ON UPDATE CASCADE,",
                "    FOREIGN KEY (site_name, camera_id) REFERENCES camera(site_name, camera_id)",
                "            ON DELETE SET NULL ON UPDATE CASCADE);"
            ))
            insert_sql = '\n'.join((
                "INSERT INTO photo_new (path, fname, ftype, md5hash, dt_orig, dt_mod, dt_import)",
                "SELECT path, fname, ftype, md5hash, dt_orig, dt_mod, dt_import",
                "  FROM photo;"
            ))
            c.execute("PRAGMA foreign_keys=OFF;")
            c.execute("BEGIN TRANSACTION;")
            c.execute(new_photo_sql)
            c.execute(insert_sql)
            c.execute("DROP TABLE photo;")
            c.execute("ALTER TABLE photo_new RENAME TO photo;")
            for old in old_index_triggers:
                c.execute(old)
            c.execute("PRAGMA foreign_key_check;")
            c.execute("COMMIT;")
            c.execute("PRAGMA foreign_keys=ON;")
        c.execute("UPDATE photo SET year_orig = cast(strftime('%Y', dt_orig) AS INT) WHERE year_orig IS NULL;")
        con.commit()

    elif isinstance(con, psycopg.Connection):
        alter_list = [
            '\n'.join((
                "ALTER TABLE photo",
                "  ADD COLUMN IF NOT EXISTS site_name VARCHAR,",
                "  ADD COLUMN IF NOT EXISTS camera_id VARCHAR,",
                "  ADD COLUMN IF NOT EXISTS year_orig INTEGER,",
                "  ADD COLUMN IF NOT EXISTS season_no INTEGER,",
                "  ADD COLUMN IF NOT EXISTS season_order INTEGER;"
            )),
            "ALTER TABLE photo DROP CONSTRAINT IF EXISTS camera_site_fk;",
            '\n'.join((
                "ALTER TABLE photo ADD CONSTRAINT camera_site_fk ",
                "FOREIGN KEY (site_name, camera_id) REFERENCES camera (site_name, camera_id) ",
                "ON UPDATE CASCADE ON DELETE SET NULL;"
            )),
            "UPDATE photo SET year_orig = date_part('year', coalesce(dt_orig, dt_mod)) WHERE year_orig IS NULL;"
        ]
        for sql in alter_list:
            if verbose:
                print(sql)
                print('-----------------------------------------------------')
            c.execute(sql)
    con.commit()


def create_animal_views(con: Union[sqlite.Connection, psycopg.Connection], verbose: bool = False):
    if isinstance(con, sqlite.Connection):
        group_concat = 'group_concat'
        concat = "{} || ',' || {} || ',' || {} || ',' || {}"
        exists = "IF NOT EXISTS"
        replace = ""
    elif isinstance(con, psycopg.Connection):
        group_concat = 'string_agg'
        concat = "concat_ws(',', {}, {}, {}, {})"
        exists = ""
        replace = "OR REPLACE"
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")

    sql_list = [
        '\n'.join((
            f'CREATE {replace} VIEW {exists} gen_seq_count AS',
            'WITH gen_count AS (',
            'SELECT gen_id, count(seq_id) AS n',
            '  FROM sequence_gen',
            ' GROUP BY gen_id',
            ')',
            '',
            'SELECT a.*, CASE WHEN b.n IS NULL THEN 0 ELSE b.n END AS n',
            '  FROM generation a',
            '  LEFT JOIN gen_count b ON a.gen_id = b.gen_id;')),
        '\n'.join((
            f"CREATE {replace} VIEW {exists} export_animal AS",
            "WITH coords_long AS (",
            "SELECT CASE WHEN a.local = True THEN a.base_path || '/' || b.path",
            '            ELSE b.path END "path",',
            f"       c.id, c.cnt, c.classifier, {concat} coords".format('d.x1', 'd.y1', 'd.x2', 'd.y2'),
            "  FROM import a",
            " INNER JOIN photo b ON a.import_date = b.dt_import",
            " INNER JOIN animal c ON b.md5hash = c.md5hash",
            "  LEFT JOIN animal_loc d ON c.md5hash = d.md5hash AND c.id = d.id",
            "",
            "), coords_wide AS (",
            f"""SELECT "path", id, cnt, classifier, {group_concat}(coords, '|') coords""",
            "  FROM coords_long",
            ' GROUP BY "path", id, cnt, classifier',
            ")",
            "",
            "SELECT * FROM coords_wide;"
        ))
    ]

    c = con.cursor()
    for sql in sql_list:
        if verbose:
            print(sql)
            print('-----------------------------------------------------')
        c.execute(sql)
    con.commit()


def copy_data_sqlite(dbpath, photo_db, remove_thumbnail, tags):
    """Deprecated."""
    con = sqlite.connect(dbpath)
    c = con.cursor()
    c.execute("ATTACH DATABASE ? AS photo;", (photo_db,))
    print("\tcopying from import...")
    c.execute("""INSERT INTO import (import_date, base_path, local, type)
                 SELECT import_date, base_path, local, type
                   FROM photo.import;""")
    print("\tcopying from hash...")
    c.execute("""INSERT INTO hash (md5hash, import_date)
                 SELECT md5hash, import_date
                   FROM photo.hash;""")
    print("\tcopying from photo...")
    c.execute("""INSERT INTO photo (path, fname, ftype,  dt_orig, dt_mod, md5hash)
                 SELECT path, fname, ftype, dt_orig, dt_mod, md5hash
                   FROM photo.photo;""")
    print("\tcopying from tag...")
    tags_sql = """INSERT INTO tag (md5hash, tag, value)
                  SELECT md5hash, tag, value
                    FROM photo.tag"""
    where = []
    if tags is not None:
        tags.append('EXIF DateTimeOriginal')
        tags = list(set(tags))
        where.append("tag IN ({})".format(', '.join('?'*len(tags))))
    if remove_thumbnail:
        where.append("tag NOT LIKE '%Thumbnail%'")
    where_stmt = ' AND '.join(where)
    if where_stmt:
        tags_where = ' WHERE '.join((tags_sql, where_stmt)) + ';'
    else:
        tags_where = tags_sql + ';'
    if tags is not None:
        con.execute(tags_where, tags)
    else:
        con.execute(tags_where)
    con.commit()
    c.execute("DETACH photo;")
    c.execute("UPDATE photo SET year_orig = CAST(strftime('%Y', dt_orig) AS INTEGER);")


def populate_sites(con: Union[sqlite.Connection, psycopg.Connection], site_csv: str):
    sites = pd.read_csv(site_csv, sep=',')
    allowed_cols = ['site_name', 'state_code', 'desc']
    # restricts columns to just valid cols that exist in the csv
    site_ins = sites.loc[:, sites.columns.isin(allowed_cols)]
    cols = tuple(site_ins.columns)
    if 'site_name' not in cols:
        print(site_csv, "must have at least the 'site_name' field. Aborting site imports...")
        return
    update_cols = [x for x in cols if x != 'site_name']
    u = con.cursor()

    if isinstance(con, sqlite.Connection):
        ph = '?'
        ignore = 'OR REPLACE'
        conflict = ''
        ph_str = ', '.join([':{}'.format(x) for x in cols])
    elif isinstance(con, psycopg.Connection):
        ph = '%s'
        ignore = ''
        excluded = ', '.join(['{} = EXCLUDED.{}'.format(x, x) for x in update_cols])
        ph_str = ', '.join(['%({})s'.format(x) for x in cols])
        if excluded:
            conflict = f'ON CONFLICT (site_name) DO UPDATE SET {excluded}'
        else:
            conflict = 'ON CONFLICT DO NOTHING'
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")
    col_str = ', '.join(cols)

    ins_sql = f"INSERT {ignore} INTO site ({col_str}) VALUES ({ph_str}) {conflict};"

    u.executemany(ins_sql, site_ins.to_dict('records'))
    con.commit()
    # site_ins.to_sql('site', con=con, if_exists='append', index=False)

    # populate site_names in photo table
    photo = pd.read_sql_query("SELECT * from photo", con)
    for row in sites.to_dict('records'):
        r = row['regex']
        if r is None:
            print("Site regex field cannot be found for", row['site_name'])
            continue
        site_name = row['site_name']
        subset = photo[photo.path.str.contains(r, regex=True, na=False)]
        for row2 in subset.to_dict('records'):
            u.execute(f"UPDATE photo SET site_name = {ph} WHERE md5hash = {ph};", (site_name, row2['md5hash']))
    con.commit()


def populate_cameras(con: Union[sqlite.Connection, psycopg.Connection], camera_csv: str):
    spatial, spatver = db_is_spatial(con=con)
    cameras = pd.read_csv(camera_csv, sep=',')
    allowed_cols = ['site_name', 'camera_id', 'fov_length_m', 'fov_area_sqm', 'lat', 'long', 'elev_m', 'desc']
    # restricts columns to just valid cols that exist in the csv
    camera_ins = cameras.loc[:, cameras.columns.isin(allowed_cols)]
    cols = tuple(camera_ins.columns)
    if not all(x in cols for x in ['site_name', 'camera_id']):
        print(camera_csv, "must have at least the 'site_name' and 'camera_id' fields. Aborting camera imports...")
        return
    update_cols = [x for x in cols if x not in ['site_name', 'camera_id']]
    u = con.cursor()
    if isinstance(con, sqlite.Connection):
        mp = 'MakePointZ(long, lat, elev_m, 4326)'
        ph = '?'
        ignore = 'OR REPLACE'
        conflict = ''
        ph_str = ', '.join([':{}'.format(x) for x in cols])
    elif isinstance(con, psycopg.Connection):
        mp = 'ST_SetSRID(ST_MakePoint(long, lat, elev_m), 4326)'
        ph = '%s'
        ignore = ''
        excluded = ', '.join(['{} = EXCLUDED.{}'.format(x, x) for x in update_cols])
        ph_str = ', '.join(['%({})s'.format(x) for x in cols])
        if excluded:
            conflict = f'ON CONFLICT (site_name) DO UPDATE SET {excluded}'
        else:
            conflict = 'ON CONFLICT DO NOTHING'
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")

    col_str = ', '.join(cols)
    ins_sql = f"INSERT {ignore} INTO camera ({col_str}) VALUES ({ph_str}) {conflict};"
    u.executemany(ins_sql, camera_ins.to_dict('records'))
    # camera_ins.to_sql('camera', con=con, if_exists='append', index=False)

    # populate geometry from columns

    if spatial:
        sql_cam_geo = '\n'.join((
            'WITH geo_cams AS (',
            'SELECT site_name, camera_id, lat, long, coalesce(elev_m, 0) elev_m',
            '  FROM camera WHERE lat IS NOT NULL AND long IS NOT NULL',
            '), geo AS (',
            f'SELECT site_name, camera_id, lat, long, {mp} AS geometry',
            '  FROM geo_cams)',
            '',
            'UPDATE camera SET geometry = (SELECT geometry FROM geo WHERE site_name = camera.site_name',
            '                                                         AND camera_id = camera.camera_id);'
        ))
        u.execute(sql_cam_geo)
    con.commit()

    # populate camera_id in photo table
    if 'regex' not in cameras.columns:
        print("No regex field found in camera csv. Setting all camera_id values to '1' in photo table.")
        u.execute("UPDATE photo SET camera_id = '1' WHERE camera_id IS NULL;")
    else:
        for index, row in cameras.iterrows():
            r = row['regex']
            camera_id = str(row['camera_id'])
            site_name = row['site_name']
            photo = pd.read_sql_query(f"SELECT * from photo WHERE site_name = {ph};", con=con, params=(site_name,))
            if r is not np.nan:
                subset = photo[photo.path.str.contains(r, regex=True, na=False)]
            else:
                subset = photo
            for index2, row2 in subset.iterrows():
                u.execute(f"UPDATE photo SET camera_id = {ph} WHERE md5hash = {ph};", (camera_id, row2['md5hash']))
    con.commit()


def populate_seasons(con: Union[sqlite.Connection, psycopg.Connection], season_break: int):
    if isinstance(con, sqlite.Connection):
        julian_func = "julianday({})"
    elif isinstance(con, psycopg.Connection):
        julian_func = "extract(julian from {})"
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")
    c = con.cursor()
    season_sql = '\n'.join((
        "CREATE TABLE season AS",
        f"WITH lims (max_days) AS (VALUES ({season_break})",
        "",
        "-- Creates a table comparing a photo date and the previous/next photo date",
        "-- Uses dt_orig (EXIF metadata) primarily but also dt_mod (file modified date) as a fallback",
        "), lag_dts AS (",
        "SELECT coalesce(site_name, 'none') site_name, coalesce(camera_id,'none') camera_id,",
        "       year_orig, coalesce(dt_orig, dt_mod) dt_orig,",
        "       lag(coalesce(dt_orig, dt_mod), 1)",
        "           over(partition by site_name, camera_id order by dt_orig, dt_mod) as prev_dt,",
        "       lead(coalesce(dt_orig, dt_mod), 1)",
        "           over(partition by site_name, camera_id order by dt_orig, dt_mod) as next_dt",
        "  FROM photo",
        "",
        "-- Counts the number of days between prev/next photo dates",
        "), day_cnt AS (",
        "SELECT site_name, camera_id, year_orig, prev_dt, dt_orig, next_dt,",
        f"   {julian_func} - {julian_func} AS prev_days,".format('dt_orig', 'prev_dt'),
        f"   {julian_func} - {julian_func} AS next_days".format('next_dt', 'dt_orig'),
        "  FROM lag_dts",
        "",
        "-- Filters down records to just those that have a prev or next photo > break limit in days",
        "), day_filt AS (",
        "SELECT site_name, camera_id, year_orig, prev_dt, dt_orig, next_dt, prev_days, next_days",
        "  FROM day_cnt",
        " WHERE prev_days IS NULL OR prev_days > (SELECT max_days FROM lims)",
        "    OR next_days IS NULL OR next_days > (SELECT max_days FROM lims)",
        "",
        "-- assigns a type to the record depending on if the previous data or next date > break limit",
        "), starts_ends AS (",
        "SELECT site_name, camera_id, year_orig, dt_orig,",
        "       CASE WHEN prev_days IS NULL OR prev_days > (SELECT max_days FROM lims) THEN 'start'",
        "                 WHEN next_days IS NULL OR next_days > (SELECT max_days FROM lims) THEN 'end'",
        "                 ELSE NULL END AS date_type",
        "  FROM day_filt",
        "",
        "-- creates a wide version of the long data given in start_ends with a start and end date for",
        "-- each row",
        "), breaks AS (",
        "SELECT site_name, camera_id, year_orig, date_type, dt_orig AS start_dt,",
        "       lead(dt_orig, 1) over(partition by site_name, camera_id order by dt_orig) as end_dt",
        "  FROM starts_ends",
        "",
        "-- selects just start date_types as those are the ones that contain photos and also assigns",
        "-- a season number",
        "), season AS (",
        "SELECT site_name, camera_id, year_orig, start_dt,",
        "	   CASE WHEN end_dt IS NULL THEN start_dt ELSE end_dt END end_dt,",
        "       row_number() over(partition by site_name, camera_id order by start_dt) AS season_no",
        "  FROM breaks",
        " WHERE date_type = 'start'",
        ")",
        "",
        "SELECT * FROM season ORDER BY site_name, camera_id, season_no;"
    ))
    update_sql = '\n'.join((
        "-- joins seasons back to original photos",
        "WITH joined AS (",
        "SELECT a.md5hash, a.path, a.fname, a.ftype,",
        "       coalesce(a.site_name, 'none') site_name, coalesce(a.camera_id, 'none') camera_id,",
        "	   a.dt_orig, a.year_orig, b.season_no",
        "  FROM photo AS a",
        "  LEFT JOIN season AS b ON coalesce(a.site_name, 'none') = b.site_name",
        "                                AND coalesce(a.camera_id, 'none') = b.camera_id",
        "                                AND coalesce(a.dt_orig, a.dt_mod) BETWEEN b.start_dt AND b.end_dt",
        "",
        "-- assigns a season order based on dt_orig or dt_mod (fallback)",
        "), season_ord AS (",
        "SELECT md5hash, path, fname, ftype, site_name, camera_id, dt_orig, year_orig, season_no,",
        "       row_number() over(partition by site_name, camera_id, season_no order by dt_orig) AS season_order",
        "  FROM joined",
        ")",
        "",
        "UPDATE photo a",
        "   SET season_no = b.season_no,",
        "       season_order = b.season_order",
        "  FROM season_ord b",
        " WHERE a.md5hash = b.md5hash;"
    ))
    c.execute("DROP TABLE IF EXISTS season;")
    c.execute(season_sql)
    c.execute(update_sql)
    con.commit()


def populate_animals(con: Union[sqlite.Connection, psycopg.Connection], animal_csv: str):
    if not isinstance(con, sqlite.Connection) and not isinstance(con, psycopg.Connection):
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")
    animals = pd.read_csv(animal_csv, sep=',')
    allowed_cols = ['path', 'id', 'cnt', 'classifier', 'coords']
    allowed_new_cols = ['md5hash', 'id', 'cnt', 'classifier']
    # restricts columns to just valid cols that exist in the csv
    animal_ins = animals.loc[:, animals.columns.isin(allowed_cols)]
    animal_ins.assign(path=lambda x: x.path.str.replace('\\', '/', regex=False))
    cols = tuple(animal_ins.columns)
    if not all(x in cols for x in ['path', 'id']):
        print(animal_csv, "must have at least the 'path' and 'id' fields. Aborting animal imports...")
        return
    photo = pd.read_sql_query("SELECT path, md5hash from photo", con)
    animal_joined = animal_ins.merge(photo, how='inner', on='path')
    animal_hash = animal_joined.loc[:, animal_joined.columns.isin(allowed_new_cols)]
    new_cols = tuple(animal_hash.columns)
    update_cols = [x for x in new_cols if x not in ['md5hash', 'id']]
    u = con.cursor()
    if isinstance(con, sqlite.Connection):
        ph = '?'
        ignore = 'OR REPLACE'
        conflict = ''
        ph_str = ', '.join([':{}'.format(x) for x in new_cols])
    elif isinstance(con, psycopg.Connection):
        ph = '%s'
        ignore = ''
        excluded = ', '.join(['{} = EXCLUDED.{}'.format(x, x) for x in update_cols])
        ph_str = ', '.join(['%({})s'.format(x) for x in new_cols])
        if excluded:
            conflict = f'ON CONFLICT (site_name) DO UPDATE SET {excluded}'
        else:
            conflict = 'ON CONFLICT DO NOTHING'
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")

    col_str = ', '.join(new_cols)
    ins_sql = f"INSERT {ignore} INTO animal ({col_str}) VALUES ({ph_str}) {conflict};"
    u.executemany(ins_sql, animal_hash.to_dict('records'))
    # animal_hash.to_sql('animal', con=con, if_exists='append', index=False)
    con.commit()

    # coordinates
    # using reindex here to force the split to take 4 columns (2d) for even though the split may only produce 2 in the
    # case of 1d
    animal_long = animal_joined\
        .assign(coord_list=lambda x: x.coords.str.split(pat=r'\s*\|\s*', expand=False, regex=True))\
        .explode('coord_list')
    animal_long[['x1', 'y1', 'x2', 'y2']] = animal_long['coord_list']\
        .str.split(pat=r'\s*,\s*', expand=True, regex=True, n=2).reindex(labels=range(4), axis='columns')
    animal_filt = animal_long.query('~(x1.isnull() & y1.isnull() & x2.isnull() & y2.isnull())', engine='python')

    allowed_coord_cols = ['md5hash', 'id', 'classifier', 'x1', 'y1', 'x2', 'y2']
    animal_coord = animal_filt.loc[:, animal_filt.columns.isin(allowed_coord_cols)]\
        .drop_duplicates(keep='last')
    coord_cols = tuple(animal_coord.columns)
    coord_str = ', '.join(coord_cols)
    if isinstance(con, sqlite.Connection):
        coord_conflict = ''
        coord_ph_str = ', '.join([':{}'.format(x) for x in coord_cols])
    elif isinstance(con, psycopg.Connection):
        coord_excluded = ', '.join(['{} = EXCLUDED.{}'.format(x, x) for x in coord_cols])
        coord_ph_str = ', '.join(['%({})s'.format(x) for x in coord_cols])
        coord_conflict = f'ON CONFLICT ON CONSTRAINT loc_unique DO UPDATE SET {coord_excluded}'
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")

    ins_sql_loc = f"INSERT {ignore} INTO animal_loc ({coord_str}) VALUES ({coord_ph_str}) {coord_conflict};"
    u.executemany(ins_sql_loc, animal_coord.to_dict('records'))
    con.commit()


def populate_sequences(con: Union[sqlite.Connection, psycopg.Connection], sequence_break: int = 60,
                       max_photo: int = 30, overwrite: bool = False):
    if isinstance(con, sqlite.Connection):
        julian_func = "julianday({})"
        hex_func = "lower(hex(randomblob(8)))"
    elif isinstance(con, psycopg.Connection):
        julian_func = "extract(julian from {})"
        hex_func = "encode(gen_random_bytes(8), 'hex')"
    else:
        raise ValueError("con must be either class psycopg.Connection or sqlite3.Connection.")

    # creates new unique sequences for animals based off of sequence_break and max_photo inputs
    seq_sql = '\n'.join((
        "CREATE TEMPORARY TABLE seqs_temp AS",
        "-- sets our sequence time break limit in minutes",
        f"WITH break_limit (minutes) AS (VALUES ({sequence_break})",
        "-- sets maximum number of photos allowed in a sequence",
        f"), max_photo (max_n) AS (VALUES ({max_photo})",
        "",
        "), prev_date AS (",
        "-- attaches previous photo date in same site/camera/animal group",
        "SELECT a.md5hash, a.id, a.cnt, b.site_name, b.camera_id, coalesce(b.dt_orig, b.dt_mod) dt_orig,",
        "       lag(coalesce(b.dt_orig, b.dt_mod))",
        "	       over(PARTITION BY site_name, camera_id, id ORDER BY dt_orig, dt_mod) AS prev_dt",
        "  FROM animal AS a",
        " INNER JOIN photo AS b ON a.md5hash = b.md5hash",
        "",
        "), time_dif AS (",
        "-- calculates the difference between current time and previous photo time",
        "SELECT *,",
        f"       round(({julian_func} - {julian_func}) * 24 * 60, 1) AS minutedif".format('dt_orig', 'prev_dt'),
        "  FROM prev_date",
        "",
        "), ranking AS (",
        "-- attaches a rank to each new break within site, camera, animal",
        "SELECT md5hash, id, cnt, site_name, camera_id, dt_orig, prev_dt, minutedif,",
        "       CASE WHEN minutedif > (SELECT minutes FROM break_limit) OR minutedif IS NULL THEN",
        "       dense_rank() over(PARTITION BY site_name, camera_id, id,",
        "				       CASE WHEN minutedif > (SELECT minutes FROM break_limit) OR minutedif IS NULL",
        "				            THEN 1",
        "				            ELSE 0 END",
        "		          ORDER BY dt_orig) END AS rk",
        "  FROM time_dif",
        "",
        "), partitioning AS (",
        "-- creates a unique partition id such that we can apply the rank from 'ranking'",
        "-- to all other values in that rank block",
        "SELECT md5hash, id, cnt, site_name, camera_id, dt_orig, rk,",
        "       count(rk) OVER (ORDER BY site_name, camera_id, id, dt_orig) AS part_id",
        "  FROM ranking",
        "",
        "), final AS (",
        "-- attaches our rank id to the null values produced from 'ranking'",
        "SELECT md5hash, id, cnt, site_name, camera_id, dt_orig,",
        "       coalesce(first_value(rk) ",
        "           over(PARTITION BY part_id ORDER BY site_name, camera_id, id, dt_orig, rk DESC),0) AS seq",
        "  FROM partitioning",
        "",
        "), seq_rn AS (",
        "-- assigns a row number for calculating sub-divisisions for sequences that are above our max photo limit",
        "SELECT *, row_number() over(partition by site_name, camera_id, id, seq order by dt_orig) rn",
        "  FROM final",
        "",
        "), part_calc AS (",
        "-- calculates sub-parts for sequences that are above our max photo limit",
        "SELECT *, ceiling(cast(rn AS float)/(SELECT max_n FROM max_photo)) seq_part",
        "  FROM seq_rn",
        "",
        "), minmax AS (",
        "-- compiles time ranges for each sequence",
        "SELECT site_name, camera_id, id, seq, seq_part, min(dt_orig) AS min_dt, max(dt_orig) AS max_dt,",
        "       count(md5hash) n",
        "  FROM part_calc",
        " GROUP BY site_name, camera_id, id, seq, seq_part",
        "",
        "), sequence_id AS (",
        "-- adds a random uuid for each sequence",
        "SELECT site_name, camera_id, id, seq, seq_part, min_dt, max_dt,",
        f"       {hex_func} AS seq_id",
        "  FROM minmax",
        ")",
        "",
        "SELECT site_name, camera_id, id, seq, seq_part, min_dt, max_dt, seq_id",
        "  FROM sequence_id",
        " ORDER BY site_name, camera_id, id, seq;"
    ))

    # inserts new records into sequence for animals that are missing a seq_id
    insert_sql = '\n'.join((
        'INSERT INTO "sequence" (seq_id, site_name, camera_id, id, seq, seq_part, min_dt, max_dt)',
        "WITH animal_join AS (",
        "-- joins photo to animal so we can access camera and site info",
        "SELECT a.*, b.id",
        "  FROM photo AS a",
        " INNER JOIN animal AS b ON a.md5hash = b.md5hash",
        " WHERE b.seq_id IS NULL",
        "",
        "-- selects just seq_id in the temp seq table which can be joined to null seq_id values in the animal table",
        "-- and creates a unique list",
        "), join_seq_grp AS (",
        "SELECT b.seq_id",
        "  FROM animal_join AS a",
        " INNER JOIN seqs_temp AS b ON (coalesce(a.dt_orig, a.dt_mod) BETWEEN b.min_dt AND b.max_dt)",
        "                            AND a.id = b.id",
        "                            AND a.site_name = b.site_name",
        "                            AND a.camera_id = b.camera_id",
        " GROUP BY b.seq_id",
        ")",
        "",
        "-- returns records in the temp sequence table identified by join_seq_grp in a format",
        "-- ready for inserting",
        "SELECT a.seq_id, a.site_name, a.camera_id, a.id, a.seq, a.seq_part, a.min_dt, a.max_dt",
        "  FROM seqs_temp a",
        " INNER JOIN join_seq_grp b on a.seq_id = b.seq_id",
        " ORDER BY site_name, camera_id, id, min_dt;"
    ))

    # updates animal table with newly generated sequences. Will also populate older sequences that are missing for
    # some reason but already exist in the sequence table.
    update_sql = '\n'.join((
        "WITH animal_join AS (",
        "-- joins photo to animal so we can access camera and site info",
        "SELECT a.*, b.id",
        "  FROM photo AS a",
        " INNER JOIN animal AS b ON a.md5hash = b.md5hash",
        " WHERE b.seq_id IS NULL",
        "-- joins animals back to sequence table",
        "), seq_join AS (",
        "SELECT a.*, b.seq, b.min_dt, b.max_dt, b.seq_id",
        "  FROM animal_join AS a",
        " INNER JOIN sequence AS b ON (coalesce(a.dt_orig, a.dt_mod) BETWEEN b.min_dt AND b.max_dt)",
        "                         AND a.id = b.id",
        "                         AND a.site_name = b.site_name",
        "                         AND a.camera_id = b.camera_id)",
        "",
        "UPDATE animal a",
        "SET seq_id = b.seq_id",
        "FROM seq_join b",
        "WHERE a.md5hash = b.md5hash AND a.id = b.id",
        "AND a.seq_id IS NULL;"
    ))

    c = con.cursor()
    c.execute("DROP TABLE IF EXISTS seqs_temp;")
    c.execute(seq_sql)
    c.execute(insert_sql)
    c.execute(update_sql)
    con.commit()


def create_indices(con: Union[sqlite.Connection, psycopg.Connection]):
    c = con.cursor()
    stmts = [
        "CREATE INDEX IF NOT EXISTS animal_seq_id ON animal (seq_id);",
        "CREATE INDEX IF NOT EXISTS animal_id ON animal (id);",
        "CREATE INDEX IF NOT EXISTS camera_site_name ON camera (site_name);",
        "CREATE INDEX IF NOT EXISTS condition_scorer_name ON condition (scorer_name);",
        "CREATE INDEX IF NOT EXISTS condition_rating ON condition (rating);",
        "CREATE INDEX IF NOT EXISTS condition_md5hash ON condition (md5hash);",
        "CREATE INDEX IF NOT EXISTS condition_seqs_seq_id ON condition_seqs (seq_id);",
        "CREATE INDEX IF NOT EXISTS photo_md5hash ON photo (md5hash);",
        "CREATE INDEX IF NOT EXISTS photo_dt_orig ON photo (dt_orig);",
        "CREATE INDEX IF NOT EXISTS photo_site_name_camera_id ON photo (site_name, camera_id);",
        "CREATE INDEX IF NOT EXISTS photo_site_name ON photo (site_name);",
        "CREATE INDEX IF NOT EXISTS photo_camera_id ON photo (camera_id);",
        "CREATE INDEX IF NOT EXISTS photo_site_name_camera_id ON photo (site_name, camera_id);",
        "CREATE INDEX IF NOT EXISTS photo_md5hash ON photo (md5hash);",
        "CREATE INDEX IF NOT EXISTS sequence_id ON sequence (id);",
        "CREATE INDEX IF NOT EXISTS sequence_site_name_camera_id ON sequence (site_name, camera_id);",
        "CREATE INDEX IF NOT EXISTS sequence_site_name ON sequence (site_name);",
        "CREATE INDEX IF NOT EXISTS sequence_camera_id ON sequence (camera_id);",
        "CREATE INDEX IF NOT EXISTS sequence_gen_id ON sequence_gen (gen_id);"
    ]
    for stmt in stmts:
        c.execute(stmt)


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='This script will create an empty camera trap database and populate it with given inputs.')
    # positional arguments
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--dbpath',
                       help='the path of the spatialite database to be created.')
    group.add_argument('--db', help='the PostgreSQL database to which to connect.')
    args_pg = parser.add_argument_group('PostgreSQL')
    args_pg.add_argument('--host', default='localhost')
    args_pg.add_argument('--user', default='postgres')
    args_pg.add_argument('--port', default=5432, type=int)
    args_pg.add_argument('--passwd', help="Password for user.")
    args_pg.add_argument('--noask', action='store_true',
                         help="User will not be prompted for password if none given.")
    parser.add_argument('-o', '--overwrite', action='store_true',
                        help='overwrite an existing database given with --dbpath')
    args_camera = parser.add_argument_group('camera')
    args_camera.add_argument('-s', '--site', help='path to a csv file containing site data. See README for required '
                                                  'table specifications.')
    args_camera.add_argument('-c', '--camera', help='path to a csv file containing camera data. See README for '
                                                    'required table specifications.')
    args_camera.add_argument('-b', '--season', type=int, default=30,
                             help='the number of days without photos to use as a defining break point for a camera '
                                  'season.')
    args_animal = parser.add_argument_group('animal')
    args_animal.add_argument('-a', '--animal', help='path to a csv file containing animal detection data. See README '
                                                    'for required table specifications.')
    args_animal.add_argument('-B', '--sequence', type=int, default=60,
                             help='the number of minutes without an animal id to use as a defining break point for a '
                             'sequence.')
    args_animal.add_argument('--overwrite_sequence', action='store_true',
                             help='Overwrite existing sequence info in the sequences and animal table if already '
                                  'present.')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help="Print misc. info for use in debugging.")

    args = parser.parse_args()

    # argument checking
    if args.site:
        if not os.path.isfile(args.site):
            print(args.site, 'does not exist. quitting...')
            quit()
        else:
            sites = pd.read_csv(args.site, sep=',')
            if 'regex' not in sites.columns:
                print("no 'regex' column found in site csv to use in matching to photo table. quitting...")
                quit()
    if args.camera:
        if not os.path.isfile(args.camera):
            print(args.camera_path, 'does not exist. quitting...')
            quit()
    if args.animal:
        if not os.path.isfile(args.animal):
            print(args.animal, 'does not exist. quitting...')
            quit()

    if args.dbpath:
        cdb.init_db_sqlite(dbpath=args.dbpath, overwrite=args.overwrite)
        conn = cdb.get_sqlite_con(dbpath=args.dbpath, geo=True)
    else:
        if args.passwd is None and not args.noask:
            args.passwd = getpass()
        cdb.init_db_pg(user=args.user, database=args.db, password=args.passwd, host=args.host, port=args.port, geo=True)
        conn = cdb.get_pg_con(user=args.user, database=args.db, password=args.passwd, host=args.host, port=args.port)
    print("Creating tables...")
    create_animal_tables(con=conn, verbose=args.verbose)
    create_animal_views(con=conn, verbose=args.verbose)
    if args.site:
        print("Populating site table...")
        populate_sites(con=conn, site_csv=args.site)
    if args.camera:
        print("Populating camera table...")
        populate_cameras(con=conn, camera_csv=args.camera)
    if args.season:
        print("Updating season info in photo table...")
        populate_seasons(con=conn, season_break=args.season)
    if args.animal:
        print("Populating animal table...")
        populate_animals(con=conn, animal_csv=args.animal)
        print("Populating sequence table and updating animal table with sequence info...")
        populate_sequences(con=conn, sequence_break=args.sequence, overwrite=args.overwrite_sequence)
    print("Creating indices...")
    create_indices(con=conn)
    if conn is not None:
        conn.close()
    print('Script finished.')
