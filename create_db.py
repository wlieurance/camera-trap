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
import csv
import pandas as pd
import re
import pytz
from datetime import datetime


def create_db(dbpath, srid=4326):
    print("creating empty spatialite database...")
    sql_list = [
        """
        CREATE TABLE site(site_name TEXT, state_code TEXT,
        PRIMARY KEY(site_name));
        """, """
        
        CREATE TABLE camera (
            ogc_fid INTEGER,
            site_name VARCHAR,
            camera_id TEXT,
            fov_length_m FLOAT,
            fov_area_sqm FLOAT,
            lat FLOAT,
            long FLOAT,
            elev_m FLOAT,
            PRIMARY KEY(ogc_fid AUTOINCREMENT),
            FOREIGN KEY(site_name) REFERENCES site(site_name) ON DELETE CASCADE,
            UNIQUE(site_name, camera_id));
        """, """
        
        SELECT AddGeometryColumn('camera', 'geometry', {srid}, 'POINTZ');
        """.format(srid=srid), """
        
        CREATE TABLE photo (
            md5hash TEXT,
            path TEXT UNIQUE,
            fname TEXT,
            ftype TEXT,
            site_name TEXT,
            camera_id TEXT,
            taken_dt DATETIME,
            taken_yr INTEGER,
            season_no INTEGER,
            season_order INTEGER,
            PRIMARY KEY(md5hash),
            FOREIGN KEY(site_name, camera_id) REFERENCES camera(site_name, camera_id) ON DELETE CASCADE);
        """, """
        
        CREATE TABLE tag (
            md5hash TEXT,
            tag TEXT,
            value TEXT,
            PRIMARY KEY(md5hash, tag));
        """, """
        
        CREATE TABLE sequence (
            seq_id TEXT,
            site_name TEXT,
            camera_id TEXT,
            id TEXT,
            seq INTEGER,
            min_dt DATETIME,
            max_dt DATETIME,
            FOREIGN KEY(site_name, camera_id) REFERENCES camera(site_name, camera_id) ON DELETE CASCADE,
            PRIMARY KEY(seq_id));
        """, """
                
        CREATE TABLE generation (
            gen_id TEXT,
            gen_dt DATETIME,
            dbpath TEXT,
            seq_file TEXT,
            classifier TEXT,
            animal TEXT,
            date_range TEXT,
            site_name TEXT,
            camera TEXT,
            overwrite INTEGER,
            seq_no INTEGER,
            filter_condition INTEGER,
            filter_generated INTEGER,
            subsample REAL,
            PRIMARY KEY(gen_id));
        """, """
        
        CREATE TABLE sequence_gen (
            seq_id TEXT,
            gen_id TEXT,
            FOREIGN KEY(seq_id) REFERENCES sequence(seq_id) ON DELETE CASCADE,
            FOREIGN KEY(gen_id) REFERENCES generation(gen_id) ON DELETE CASCADE,
            PRIMARY KEY(seq_id, gen_id));
        """, """
        CREATE TABLE animal (
            md5hash TEXT,
            id TEXT,
            cnt INTEGER,
            classifier TEXT,
            seq_id TEXT,
            coords TEXT,
            PRIMARY KEY(md5hash ,id),
            FOREIGN KEY(md5hash) REFERENCES photo(md5hash) ON DELETE CASCADE,
            FOREIGN KEY(seq_id) REFERENCES sequence(seq_id) ON DELETE SET NULL);
        """, """
        
        CREATE INDEX tag_value_idx ON tag (value);
        """
    ]

    # connect to db
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    con.enable_load_extension(True)
    con.execute("SELECT load_extension('mod_spatialite')")
    con.execute("SELECT InitSpatialMetaData(1)")
    c = con.cursor()
    for sql in sql_list:
        c.execute(sql)
    con.commit()
    con.close()


def copy_data(dbpath, photo_db, remove_thumbnail, tags):
    print("copying records from", photo_db, "to", dbpath)
    con = sqlite.connect(dbpath)
    c = con.cursor()
    c.execute("ATTACH DATABASE ? AS photo;", (photo_db,))
    c.execute("""INSERT INTO photo (path, fname, ftype, md5hash)
                 SELECT path, fname, ftype, md5hash
                   FROM photo.photo;""")
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
    con.close()


def populate_datetime(dbpath, tz_string):
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    c = con.cursor()
    c.execute("""
    WITH dt AS (
    SELECT md5hash, value AS dt FROM tag WHERE tag = 'EXIF DateTimeOriginal'

    ), f AS (
    SELECT a.md5hash, b.dt, 
           replace(substr(b.dt, 1, instr(b.dt, ' ') -1), ':', '-') || ' ' || substr(b.dt, instr(b.dt, ' ') + 1, 
           length(b.dt)) AS taken_dt, substr(b.dt, 1, instr(b.dt, ':') -1) AS taken_yr
      FROM photo AS a
      LEFT JOIN dt As b ON a.md5hash = b.md5hash)

    UPDATE photo SET taken_dt = (SELECT taken_dt FROM f WHERE md5hash = photo.md5hash),
                     taken_yr = (SELECT taken_yr FROM f WHERE md5hash = photo.md5hash);""")
    con.commit()
    if tz_string:
        tz = pytz.timezone(tz_string)
        u = con.cursor()
        rows = c.execute("SELECT md5hash, taken_dt FROM photo;")
        for row in rows:
            dt_orig = datetime.strptime(row['taken_dt'], '%Y-%m-%d %H:%M:%S')
            dt_new = tz.localize(dt_orig)
            u.execute("UPDATE photo SET taken_dt = ? WHERE md5hash = ?;", (dt_new, row['md5hash']))
        con.commit()
    con.close()


def populate_sites(dbpath, site_csv):
    print("populating site table and site_name in photo table...")
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    u = con.cursor()
    sites = pd.read_csv(site_csv, sep=',')
    # restricts columns to just valid cols that exist in the csv
    site_ins = sites.loc[:, sites.columns.isin(['site_name', 'state_code'])]

    site_ins.to_sql('site', con=con, if_exists='append', index=False)

    # populate site_names in photo table
    photo = pd.read_sql_query("SELECT * from photo", con)
    for index, row in sites.iterrows():
        r = row['regex']
        site_name = row['site_name']
        subset = photo[photo.path.str.contains(r, regex=True, na=False)]
        for index2, row2 in subset.iterrows():
            u.execute("UPDATE photo SET site_name = ? WHERE md5hash = ?;", (site_name, row2['md5hash']))
    con.commit()
    con.close()


def populate_cameras(dbpath, camera_csv, srid):
    print("populating camera table and camera_id in photo table...")
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    con.enable_load_extension(True)
    con.execute("SELECT load_extension('mod_spatialite')")
    u = con.cursor()
    cameras = pd.read_csv(camera_csv, sep=',')
    # restricts columns to just valid cols that exist in the csv
    camera_ins = cameras.loc[:, cameras.columns.isin(['site_name', 'camera_id', 'fov_length_m', 'fov_area_sqm', 'lat',
                                                      'long', 'elev_m'])]

    camera_ins.to_sql('camera', con=con, if_exists='append', index=False)

    # populate geometry from columns
    u.execute("""
    WITH geo_cams AS (
    SELECT site_name, camera_id, lat, long, CASE WHEN elev_m IS NULL THEN 0 ELSE NULL END AS elev_m
      FROM camera WHERE lat IS NOT NULL AND long IS NOT NULL

    ), geo AS (
    SELECT site_name, camera_id, lat, long, MakePointZ(long, lat, elev_m, {srid}) AS geometry
      FROM geo_cams)
  
    UPDATE camera SET geometry = (SELECT geometry FROM geo WHERE site_name = camera.site_name 
                                                             AND camera_id = camera.camera_id);
    """.format(srid=srid))
    con.commit()

    # populate camera_id in photo table
    if 'regex' not in cameras.columns:
        print("No regex field found in camera csv. Setting all camera_id values to '1' in photo table.")
        u.execute("UPDATE photo SET camera_id = '1' WHERE camera_id IS NULL;")
    else:
        print("Using regex field in camera csv to populate camera_id in photo table.")
        for index, row in cameras.iterrows():
            r = row['regex']
            camera_id = str(row['camera_id'])
            site_name = row['site_name']
            photo = pd.read_sql_query("SELECT * from photo WHERE site_name = ?", con=con, params=(site_name,))
            subset = photo[photo.path.str.contains(r, regex=True, na=False)]
            for index2, row2 in subset.iterrows():
                u.execute("UPDATE photo SET camera_id = ? WHERE md5hash = ?;", (camera_id, row2['md5hash']))
    con.commit()
    con.close()


def populate_seasons(dbpath, season_break):
    con = sqlite.connect(dbpath)
    con.row_factory = sqlite.Row
    c = con.cursor()
    sql = """
    WITH lims (max_days) AS (VALUES ({season_break})
 
    ), lag_dts AS (
    SELECT site_name, camera_id, cast(strftime('%Y', taken_dt) AS INT) AS year, taken_dt,
           lag(taken_dt, 1) over(partition by site_name, camera_id order by taken_dt) as prev_dt,
           lead(taken_dt, 1) over(partition by site_name, camera_id order by taken_dt) as next_dt
      FROM photo
      
    ), day_cnt AS (
    SELECT site_name, camera_id, year, prev_dt, taken_dt, next_dt,
           julianday(taken_dt) - julianday(prev_dt) AS prev_days,
           julianday(next_dt) - julianday(taken_dt) AS next_days
      FROM lag_dts WHERE prev_days IS NULL OR prev_days > (SELECT max_days FROM lims) OR
                         next_days IS NULL OR next_days > ((SELECT max_days FROM lims))
    ), starts_ends AS (
    SELECT site_name, camera_id, year, taken_dt,
           CASE WHEN prev_days IS NULL OR prev_days > (SELECT max_days FROM lims) THEN 'start'
                WHEN next_days IS NULL OR next_days > (SELECT max_days FROM lims) THEN 'end'
                ELSE NULL END AS date_type
      FROM day_cnt
      
    ), breaks AS (
    SELECT site_name, camera_id, year, date_type, taken_dt AS start_dt,
           lead(taken_dt, 1) over(partition by site_name, camera_id order by taken_dt) as end_dt
      FROM starts_ends
      
    ), season AS (
    SELECT site_name, camera_id, year, start_dt, end_dt, 
           row_number() over(partition by site_name, camera_id order by start_dt) AS season_no
      FROM breaks 
     WHERE date_type = 'start'
    
    ), joined AS (
    SELECT a.md5hash, a.path, a.fname, a.ftype, a.site_name, a.camera_id, a.taken_dt, a.taken_yr, b.season_no
      FROM photo AS a
      LEFT JOIN season AS b ON a.site_name = b.site_name AND 
                               a.camera_id = b.camera_id AND 
                               a.taken_yr = b.year AND
                               a.taken_dt BETWEEN b.start_dt AND b.end_dt
    
    ), season_ord AS (
    SELECT md5hash, path, fname, ftype, site_name, camera_id, taken_dt, taken_yr, season_no,
           row_number() over(partition by site_name, camera_id, season_no order by taken_dt) AS season_order
      FROM joined
    )
    
    
    UPDATE photo SET season_no = (SELECT season_no FROM season_ord WHERE md5hash = photo.md5hash),
                     season_order = (SELECT season_order FROM season_ord WHERE md5hash = photo.md5hash);
    """.format(season_break=season_break)
    c.execute(sql)
    con.commit()
    con.close()


def populate_animals(dbpath, animal_csv):
    print("populating animal table...")
    con = sqlite.connect(dbpath)
    animals = pd.read_csv(animal_csv, sep=',')
    # restricts columns to just valid cols that exist in the csv
    animal_ins = animals.loc[:, animals.columns.isin(['path', 'id', 'cnt', 'classifier', 'coords'])]
    # animal_ins['old_path'] = animal_ins['path']
    animal_ins.path = animal_ins.path.str.replace('\\', '/', regex=False)
    photo = pd.read_sql_query("SELECT path, md5hash from photo", con)
    animal_joined = animal_ins.merge(photo, how='inner', on='path')
    animal_hash = animal_joined.loc[:, animal_joined.columns.isin(['md5hash', 'id', 'cnt', 'classifier', 'coords'])]
    animal_hash.to_sql('animal', con=con, if_exists='append', index=False)
    con.commit()
    con.close()


def populate_sequences(dbpath, sequence_break):
    print("populating sequence table and updating seq_id in animal table...")
    seq_sql = """
    WITH
    -- sets our sequence time break limit in minutes 
    break_limit (minutes) AS (VALUES ({sequence_break})
    
    ), prev_date AS (
    -- combines previous data in order to do gap calculations
    SELECT a.md5hash, a.id, a.cnt, b.site_name, b.camera_id, b.taken_dt, 
           lag(taken_dt) over(PARTITION BY site_name, camera_id, id ORDER BY taken_dt) AS prev_dt
      FROM animal AS a
     INNER JOIN photo AS b ON a.md5hash = b.md5hash
    
    ), time_dif AS (
    -- calculates the difference between current time and previous photo time
    SELECT *, 
           round((julianday(taken_dt) - julianday(prev_dt)) * 24 * 60, 1) AS minutedif
      FROM prev_date
    
    ), ranking AS (
    -- attaches a rank to each new break within site, camera, animal
    SELECT md5hash, id, cnt, site_name, camera_id, taken_dt, prev_dt, minutedif,
           CASE WHEN minutedif > (SELECT minutes FROM break_limit) OR minutedif IS NULL THEN
           dense_rank() over(PARTITION BY site_name, camera_id, id, 
                                          CASE WHEN minutedif > (SELECT minutes FROM break_limit) OR minutedif IS NULL 
                                               THEN 1 
                                               ELSE 0 END 
                             ORDER BY taken_dt) END AS rk 
      FROM time_dif
    
    ), partitioning AS (
    -- creates a unique partition id such that we can apply the rank from 'ranking'
    -- to all other values in that rank block
    SELECT md5hash, id, cnt, site_name, camera_id, taken_dt, rk,
           count(rk) OVER (ORDER BY site_name, camera_id, id, taken_dt) AS part_id
      FROM ranking
      
    ), final AS (
    -- attaches our rank id to the null values produced from 'ranking'
    SELECT md5hash, id, cnt, site_name, camera_id, taken_dt,
           first_value(rk) over(PARTITION BY part_id ORDER BY site_name, camera_id, id, taken_dt) AS seq
      FROM partitioning
      
    ), minmax AS (
    -- compiles time ranges for each sequence
    SELECT site_name, camera_id, id, seq, min(taken_dt) AS min_dt, max(taken_dt) AS max_dt
      FROM final
     GROUP BY  site_name, camera_id, id, seq
     
    ), sequence_id AS (
    -- adds a random uuid for each sequence
    SELECT site_name, camera_id, id, seq, min_dt, max_dt,
           lower(hex(randomblob(8))) AS seq_id
      FROM minmax)
    
    INSERT INTO sequence (site_name, camera_id, id, seq, min_dt, max_dt, seq_id)
    SELECT site_name, camera_id, id, seq, min_dt, max_dt, seq_id
      FROM sequence_id
     ORDER BY site_name, camera_id, id, seq;
    """.format(sequence_break=sequence_break)

    update_sql = """
    WITH animal_join AS (
    SELECT a.*, b.id
      FROM photo AS a 
     INNER JOIN animal AS b ON a.md5hash = b.md5hash
     
    ), seq_join AS (
    SELECT a.*, b.seq, b.min_dt, b.max_dt, b.seq_id
      FROM animal_join AS a 
    INNER JOIN sequence AS b ON (a.taken_dt BETWEEN b.min_dt AND b.max_dt)  
                             AND a.id = b.id 
                             AND a.site_name = b.site_name 
                             AND a.camera_id = b.camera_id)
    
    UPDATE animal
    SET seq_id = (SELECT seq_id
                      FROM seq_join
                      WHERE md5hash = animal.md5hash AND id = animal.id) 
    where EXISTS (SELECT seq_id
                      FROM seq_join
                      WHERE md5hash = animal.md5hash AND id = animal.id);
    """
    con = sqlite.connect(dbpath)
    c = con.cursor()
    c.execute(seq_sql)
    c.execute(update_sql)
    con.commit()
    con.close()


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='This script will create an empty camera trap database and populate it with given inputs.')
    # positional arguments
    parser.add_argument('outpath', help='path create sqlite database with (e.g. my/path/db.sqlite).')
    parser.add_argument('inpath', help='path to a database that was created via PhotoMetadata.py')
    parser.add_argument('site_path', help='path to a csv file containing site data. See README for required table '
                                          'specifications.')
    parser.add_argument('camera_path', help='path to a csv file containing camera data. See README for required table '
                                            'specifications.')
    parser.add_argument('-a', '--animal_path', help='path to a csv file containing camera data. See README for '
                                                    'required table specifications.')
    parser.add_argument('-s', '--srid', type=int, default=4326,
                        help='the spatial reference identifier code to use in the database.')
    parser.add_argument('-t', '--tags', nargs='*',
                        help="EXIF tags to keep in the new db (tags not listed with this option will be discarded). "
                             "Included for optional db size reduction. EXIF DateTimeOriginal is kept regardless.")
    parser.add_argument('-T', '--remove_thumbnail', action='store_true',
                        help="remove any image thumbnails or related records in the tag table (for size reduction).")
    parser.add_argument('-b', '--season_break', type=int, default=30,
                        help='the number of days without photos to use as a defining break point for a camera season.')
    parser.add_argument('-B', '--sequence_break', type=int, default=60,
                        help='the number of minutes without an animal id to use as a defining break point for a '
                             'sequence.')
    parser.add_argument('-z', '--timezone',
                        help='a timezone available from pytz.all_timezones used to localize EXIF DateTimeOriginal.'
                             'Most camera traps do not automatically adjust to Daylight Savings, in which case a '
                             'choice from the Etc/GMT group would be appropriate. A list of available timezones can '
                             'also be found at https://en.wikipedia.org/wiki/List_of_tz_database_time_zones')

    args = parser.parse_args()

    # argument checking
    if os.path.isfile(args.outpath):
        print('file already exists at dbpath. quitting...')
        quit()
    try:
        tz = pytz.timezone(args.timezone)
    except pytz.exceptions.UnknownTimeZoneError:
        print('invalid timezone. quitting...')
        quit()
    if not os.path.isfile(args.site_path):
        print(args.site_path, 'does not exist. quitting...')
        quit()
    else:
        sites = pd.read_csv(args.site_path, sep=',')
        if 'regex' not in sites.columns:
            print("no 'regex' column found in site csv to use in matching to photo table. quitting...")
            quit()
    if not os.path.isfile(args.camera_path):
        print(args.camera_path, 'does not exist. quitting...')
        quit()
    if args.animal_path is not None:
        if not os.path.isfile(args.animal_path):
            print(args.animal_path, 'does not exist. quitting...')
            quit()

    create_db(dbpath=args.outpath, srid=args.srid)
    copy_data(dbpath=args.outpath, photo_db=args.inpath, remove_thumbnail=args.remove_thumbnail, tags=args.tags)
    populate_datetime(dbpath=args.outpath, tz_string=args.timezone)
    populate_sites(dbpath=args.outpath, site_csv=args.site_path)
    populate_cameras(dbpath=args.outpath, camera_csv=args.camera_path, srid=args.srid)
    populate_seasons(dbpath=args.outpath, season_break=args.season_break)
    if args.animal_path is not None:
        populate_animals(dbpath=args.outpath, animal_csv=args.animal_path)
        populate_sequences(dbpath=args.outpath, sequence_break=args.sequence_break)
    print('Script finished.')
