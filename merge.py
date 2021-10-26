#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@created: 2020-03-11
@author: Wade Lieurance

This script will generate a sample of animal sequences given script filtering parameters and store them in a csv file.
"""

import sqlite3 as sqlite
import argparse


def merge_db(source, dest):
    con = sqlite.connect(dest)
    con.enable_load_extension(True)
    c = con.cursor()
    c.execute("PRAGMA foreign_keys = on;")
    c.execute("SELECT load_extension('mod_spatialite');")
    c.execute("ATTACH DATABASE '{source}' AS src;".format(source=source))
    print("Inserting into site... ", end="", flush=True)
    c.execute("INSERT OR IGNORE INTO main.site (site_name, state_code) "
              "SELECT site_name, state_code FROM src.site;")
    print(c.rowcount, "rows affected.")
    print("Inserting into camera... ", end="", flush=True)
    c.execute("INSERT OR IGNORE INTO main.camera "
              "  (site_name, camera_id, fov_length_m, fov_area_sqm, lat, long, elev_m, geometry) "
              "SELECT site_name, camera_id, fov_length_m, fov_area_sqm, lat, long, elev_m, geometry "
              "  FROM src.camera;")
    print(c.rowcount, "rows affected.")
    print("Inserting into import... ", end="", flush=True)
    c.execute("INSERT OR IGNORE INTO main.import (import_date, base_path, local, type) "
              "SELECT import_date, base_path, local, type "
              "  FROM src.import;")
    print(c.rowcount, "rows affected.")
    print("Inserting into hash... ", end="", flush=True)
    c.execute("INSERT OR IGNORE INTO main.hash (md5hash, import_date) "
              "SELECT md5hash, import_date "
              "  FROM src.hash;")
    print(c.rowcount, "rows affected.")
    print("Inserting into photo... ", end="", flush=True)
    c.execute("INSERT OR IGNORE INTO main.photo "
              "(path, fname, ftype, site_name, camera_id, dt_orig, year_orig, dt_mod, season_no, season_order, "
              "       md5hash) "
              "SELECT path, fname, ftype, site_name, camera_id, dt_orig, year_orig, dt_mod, season_no, season_order, "
              "       md5hash "
              "  FROM src.photo;")
    print(c.rowcount, "rows affected.")
    print("Inserting into tag... ", end="", flush=True)
    c.execute("INSERT OR IGNORE INTO main.tag (md5hash, tag, value) "
              "SELECT md5hash, tag, value "
              "  FROM src.tag;")
    print(c.rowcount, "rows affected.")
    print("Inserting into generation... ", end="", flush=True)
    c.execute("INSERT OR IGNORE INTO main.generation "
              "(gen_id, gen_dt, dbpath, seq_file, classifier, animal, date_range, site_name, camera, overwrite, "
              "seq_no, filter_condition, filter_generated, subsample, label) "
              "SELECT gen_id, gen_dt, dbpath, seq_file, classifier, animal, date_range, site_name, camera, overwrite, "
              "seq_no, filter_condition, filter_generated, subsample, label "
              "  FROM src.generation;")
    print(c.rowcount, "rows affected.")
    print("Inserting into sequence... ", end="", flush=True)
    c.execute("INSERT OR IGNORE INTO main.sequence (seq_id, site_name, camera_id, id, seq, min_dt, max_dt) "
              "SELECT seq_id, site_name, camera_id, id, seq, min_dt, max_dt "
              "  FROM src.sequence;")
    print(c.rowcount, "rows affected.")
    print("Inserting into sequence_gen... ", end="", flush=True)
    c.execute("INSERT OR IGNORE INTO main.sequence_gen (seq_id, gen_id) "
              "SELECT seq_id, gen_id "
              "  FROM src.sequence_gen;")
    print(c.rowcount, "rows affected.")
    print("Inserting into animal... ", end="", flush=True)
    c.execute("INSERT OR IGNORE INTO main.animal (md5hash, id, cnt, classifier, seq_id, coords) "
              "SELECT md5hash, id, cnt, classifier, seq_id, coords "
              "  FROM src.animal;")
    print(c.rowcount, "rows affected.")
    print("Inserting into condition... ", end="", flush=True)
    c.execute("INSERT OR IGNORE INTO main.condition (md5hash, seq_id, rating, scorer_name, score_dt, "
              "       bbox_x1, bbox_y1, bbox_x2, bbox_y2) "
              "SELECT md5hash, seq_id, rating, scorer_name, score_dt, bbox_x1, bbox_y1, bbox_x2, bbox_y2 "
              "  FROM src.condition;")
    print(c.rowcount, "rows affected.")
    print("Inserting into condition_seqs... ", end="", flush=True)
    c.execute("INSERT OR IGNORE INTO main.condition_seqs (seq_id, scorer_name, scores) "
              "SELECT seq_id, scorer_name, scores "
              "  FROM src.condition_seqs;")
    print(c.rowcount, "rows affected.")
    con.commit()
    c.execute("DETACH DATABASE src;")
    con.close()


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description='This script will insert data from a camera trap database into another camera trap database.')
    # positional arguments
    parser.add_argument('source', help='A camera trap db from which to source records.')
    parser.add_argument('destination', help='A camera trap db in which to insert the source records.')
    args = parser.parse_args()

    merge_db(source=args.source, dest=args.destination)
    print("Script complete.")