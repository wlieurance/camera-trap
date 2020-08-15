#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Feb 20 16:32:42 2020

@author: Wade Lieurance
"""
import os
import argparse
import sqlite3 as sqlite
import json

if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     description='This script will read a json file produced from Microsoft''s '
                                     'CameraTraps api and filter the result based on already identified photos. Photos '
                                     'will be matched if either there is either a partial or exact match for paths in '
                                     'the db and paths in the json')
    # positional arguments
    parser.add_argument('json_file', help='path to a CameraTraps api produced json file.')
    parser.add_argument('-d', '--dbpath',
                        help='The path to the sqlite database to which contains the identified data.')
    parser.add_argument('-f', '--filt_type', default='dif',
                        help='In the case that a database is provided for filtering, perform either an intersect '
                             '(''int'') or a set difference (''dif'') of json and db.')
    parser.add_argument('-o', '--out',
                        help='The path for the filtered json file.')
    parser.add_argument('-r', '--reduce', type=int,
                        help='a number of directories to keep out of the output json file paths. 0 will return no '
                             'subdirectories, 1 the base directory, etc.')
    parser.add_argument('-s', '--strip_db', type=int, default=0,
                        help='a number of subdirectories to strip out of the beginning of the db paths (to assist with '
                             'matching)')
    parser.add_argument('-S', '--strip_json', type=int, default=0,
                        help='a number of subdirectories to strip out of the beginning of the json paths (to assist '
                             'with matching)')
    args = parser.parse_args()

    with open(args.json_file, 'r') as f:
        detect = json.load(f)
    images_all = detect['images']
    if args.dbpath is not None:
        conn = sqlite.connect(args.dbpath)
        c = conn.cursor()
        print("getting paths from database...")
        rows = c.execute("SELECT path FROM photo WHERE EXISTS (SELECT md5hash FROM animal WHERE md5hash = "
                         "photo.md5hash);").fetchall()
        print("filtering json file...")
        db_paths = [x[0].replace("\\", "/")  for x in rows]
        db_paths_reduce = []
        for path in db_paths:
            split_path = path.split("/")
            new_path = '/'.join(split_path[args.strip_db:])
            db_paths_reduce.append(new_path)
        json_paths = [x['file'].replace("\\", "/") for x in images_all]
        json_paths_reduce = []
        for path in json_paths:
            split_path = path.split("/")
            new_path = '/'.join(split_path[args.strip_json:])
            json_paths_reduce.append(new_path)
        paths_filt = []
        for i in range(0, len(json_paths)):
            if json_paths_reduce[i] in db_paths_reduce:
                paths_filt.append(json_paths[i])
            else:
                for p in db_paths_reduce:
                    if p in json_paths_reduce[i]:
                        paths_filt.append(json_paths[i])
                        break
        if args.filt_type == 'dif':
            images_filt = [x for x in images_all if os.path.basename(x['file']) not in paths_filt]
        elif args.filt_type == 'int':
            images_filt = [x for x in images_all if os.path.basename(x['file']) in paths_filt]
    else:
        images_filt = images_all

    if args.reduce is not None:
        print("reducing filenames...")
        images_reduce = []
        for i in images_filt:
            new_i = i
            old_path = i['file']
            path_split = old_path.split(os.path.sep)
            new_path = os.path.sep.join(path_split[-1 * (args.reduce + 1):])
            new_i['file'] = new_path
            images_reduce.append(new_i)
    else:
        images_reduce = images_filt

    detect_filt = detect
    detect_filt['images'] = images_reduce
    
    print("writing results to json...")
    if not args.out:
        out_f = ''.join((os.path.splitext(args.json_file)[0], '_nodetects', os.path.splitext(args.json_file)[1]))
    else:
        out_f = args.out
    with open(out_f, 'w') as f:
        json.dump(detect_filt, f, indent=4)
    
    print("Script finished.")