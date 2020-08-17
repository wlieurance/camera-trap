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
    args = parser.parse_args()

    my_sql, my_params, my_photos = get_photos(args.dbpath, args.animal, args.date_range, args.site_name, args.camera,
                                              args.seq_id, args.classifier)
    if len(my_photos) == 0:
        print("No photos match script criteria. Quitting...")
        quit()

    full_paths, hashes, seq_id = get_sample(my_photos, self.scored_seqs, self.basepath,
                                                           self.random, self.set_start)