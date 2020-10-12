#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@created: 2020-03-11
@author: Wade Lieurance

This script will allow for the sampling and rating of photos provided the proper photo database path and base path for
the photos in the database. Bounding boxes are drawn around an object to be rated and associated ratings and bounding
boxes are saved in the database in the 'condition' and 'condition_seqs' tables.
"""

import sqlite3 as sqlite
import argparse
import pandas
import cv2
import os
import csv
import copy
from datetime import datetime
from tzlocal import get_localzone


def decomment(csvfile):
    for row in csvfile:
        raw = row.split('#')[0].strip()
        if raw: yield raw


def is_number(s):
    """determines whether an input is a number or not"""
    try:
        float(s)
        return True
    except ValueError:
        return False


def get_sample(phtos, scored, base_path, do_random, start=0):
    """returns the paths, md5hashes and seq_id for 1 randomly sampled row of the input database"""
    unscored_photos = phtos[~phtos['seq_id'].isin(scored)]
    if len(unscored_photos) == 0:
        return None, None, None
    if do_random:
        sample = unscored_photos.sample(n=1)
    else:
        unscored_seqs = unscored_photos.groupby('seq_id', sort=False, as_index=False)['md5hash'].count()
        n = start % len(unscored_seqs)  # allows us to cycle back to start even if start > length of df
        sample = unscored_seqs.iloc[[n]]
    sid = list(sample['seq_id'])[0]
    samples = unscored_photos[(unscored_photos['seq_id'] == sample['seq_id'].iloc[0])]
    samples = samples.sort_values(by=['site_name', 'camera_id', 'taken_dt'])
    local_paths = list(samples['path'])
    fp = [os.path.join(base_path, x).replace('\\', '/') for x in local_paths]
    h = list(samples['md5hash'])
    return fp, h, sid


def get_photos(dbpath, animal=None, date_range=None, site_name=None, camera=None, seq_id=None, classifier=None,
               verbose=False, df=True):
    """pulls photo data from the database given the given script arguments and stores in pandas df.
    animal, site_name, camera and seq_id can be single items or lists. date_range needs to be a list of 2 items."""

    sql = '\n'.join((
        "SELECT a.md5hash, a.id, a.cnt, a.classifier, a.seq_id, b.path, b.fname, b.site_name, b.taken_dt, ",
        "       b.camera_id",
        "  FROM animal AS a",
        " INNER JOIN photo AS b ON a.md5hash = b.md5hash"))
    param_list = []
    where = []
    if animal is not None:
        where.append("a.id IN ({})".format(', '.join('?' * len(animal))))
        param_list.extend(animal)
    if date_range is not None:
        assert len(date_range) == 2, "Date range does not have 2 values."
        assert len(date_range[0].split('-')) == len(date_range[1].split('-')), \
            'Date ranges are of different format.'
        assert 2 <= len(date_range[0].split('-')) <= 3, "Date ranges given in incorrect format."
        if len(date_range[0].split('-')) == 3:
            where.append("date(substr(b.taken_dt, 1, 19)) BETWEEN ? AND ?")
        elif len(date_range[0].split('-')) == 2:
            where.append("strftime('%m-%d', date(substr(b.taken_dt, 1, 19))) BETWEEN ? AND ?")
        param_list.extend(date_range)
    if site_name is not None:
        where.append("b.site_name IN ({})".format(', '.join('?' * len(site_name))))
        param_list.extend(site_name)
    if camera is not None:
        where.append("b.camera_id IN ({})".format(', '.join('?' * len(camera))))
        param_list.extend(camera)
    if seq_id is not None:
        where.append("a.seq_id IN ({})".format(', '.join('?' * len(seq_id))))
        param_list.extend(seq_id)
    if classifier is not None:
        where.append("a.classifier IN ({})".format(', '.join('?' * len(classifier))))
        param_list.extend(classifier)
    if where:
        sql += "\n WHERE " + " AND \n       ".join(where)
    sql += "\n ORDER BY b.site_name, b.camera_id, b.taken_dt;"

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


def construct_tables(dbpath):
    """constructs tables in the given database if they do not exist"""
    conn = sqlite.connect(dbpath)
    c = conn.cursor()
    c.execute("CREATE TABLE IF NOT EXISTS condition (md5hash TEXT, seq_id TEXT, bbox TEXT, rating NUMERIC, "
              "scorer_name TEXT, score_dt DATETIME, PRIMARY KEY(md5hash,seq_id,bbox, scorer_name), "
              "FOREIGN KEY(md5hash) REFERENCES photo(md5hash) ON DELETE CASCADE);")
    c.execute("CREATE TABLE IF NOT EXISTS condition_seqs (seq_id TEXT, scorer_name TEXT scores BOOLEAN, "
              "PRIMARY KEY(seq_id, scorer_name), FOREIGN KEY(seq_id) REFERENCES sequence(seq_id) ON DELETE CASCADE);")
    conn.commit()
    conn.close()


def construct_seq_list(csv_file, seqs):
    """append any seqs found in the seq_file to the args.seq_id list"""
    if csv_file is not None:
        if os.path.isfile(csv_file):
            if seqs is None:
                seqs = []
            with open(csv_file, newline='') as f:
                reader = csv.reader(decomment(f), delimiter=',', quotechar='"')
                for row in reader:
                    frow = [x.strip() for x in row]
                    seqs.extend(frow)
                    # print(seqs)
        else:
            print(csv_file, "is not an existing file.")

    # create a unique list of seq_ids in case of duplicates
    if seqs is not None:
        seqs = list(set(seqs))
    return seqs


class RatePhotos:
    """constructs a photo viewer and rating system with mouse event derived bounding boxes for photos and stores the
    rating in the database."""
    def __init__(self, photos, dbpath, basepath, name, win_name, random=True):
        # passed parameters
        self.photos = photos
        self.dbpath = dbpath
        self.basepath = basepath
        self.name = name
        self.win_name = win_name
        self.random = random

        # image constructor
        self.img = None
        self.clone = None

        # bounding box constructor
        self.refPt = []
        self.bbox = []
        self.iboxes = []
        self.cropping = False
        self.colors = [{'name': 'blue', 'value': (255, 0, 0)}, {'name': 'lime', 'value': (0, 255, 0)},
                       {'name': 'red', 'value': (0, 0, 255)}, {'name': 'cyan', 'value': (255, 255, 0)},
                       {'name': 'yellow', 'value': (0, 255, 255)}, {'name': 'magenta', 'value': (255, 0, 255)},
                       {'name': 'orange', 'value': (0, 165, 255)}, {'name': 'teal', 'value': (128, 128, 0)},
                       {'name': 'plum', 'value': (221, 160, 221)}]
        self.col_i = 0
        self.col = self.colors[self.col_i]

        # key press constructor
        self.key = ord('n')
        self.raw_key = None

        # misc
        self.i = 0  # keeps track of image number in a set being currently viewed
        self.quit_script = False
        self.set_start = 0  # keeps track of stored sets without ratings

        # sequences constructor
        self.scored_seqs = None
        self.photo_seqs = None
        self.scored_filt = None
        self.full_paths = None
        self.hashes = None
        self.seq_id = None
        self.animal_id = None

    def reset_vars(self):
        """resets image number, color, and bounding box variables"""
        self.refPt = []
        self.bbox = []
        self.iboxes = []
        self.cropping = False
        self.col_i = 0
        self.col = self.colors[self.col_i]
        self.key = None
        self.raw_key = None
        self.i = 0

    def get_scored(self):
        """returns a list of scored sequences for a particular scorer"""
        cnx = sqlite.connect(self.dbpath)
        seq_sql = "SELECT * FROM condition_seqs WHERE scorer_name = ?;"
        scored = pandas.read_sql_query(seq_sql, cnx, params=[self.name])
        scr_seqs = list(scored['seq_id'])
        cnx.close()
        return scr_seqs

    def get_animalid(self):
        """gets the animal id associated with a particular sequence"""
        cnx = sqlite.connect(self.dbpath)
        seq_sql = "SELECT * FROM sequence WHERE seq_id = ?;"
        animals_df = pandas.read_sql_query(seq_sql, cnx, params=[self.seq_id])
        animals = list(animals_df['id'])
        cnx.close()
        return animals[0]

    def click_and_crop(self, event, x, y, flags, param):
        """a callback function used to trap and process events in the image window"""
        # if the left mouse button was clicked, record the starting
        # (x, y) coordinates and indicate that cropping is being
        # performed
        if event == cv2.EVENT_LBUTTONDOWN:
            # print("LBUTTONDOWN")
            self.refPt = [(x, y)]
            self.cropping = True
            # add these lines to restrict to single bounding box
            # img = clone.copy()
        # check to see if the left mouse button was released
        elif event == cv2.EVENT_LBUTTONUP:
            # print("LBUTTONUP")
            # record the ending (x, y) coordinates and indicate that
            # the cropping operation is finished
            self.refPt.append((x, y))
            self.cropping = False
            # draw a rectangle around the region of interest
            self.bbox.append({'coords': self.refPt, 'col': self.col['value']})
            cv2.rectangle(self.img, self.refPt[0], self.refPt[1], self.col['value'], 2)
            # add these lines to restrict to single bounding box
            # self.refPt = []
        elif event == cv2.EVENT_RBUTTONDOWN:
            # print("RBUTTONDOWN")
            bbox_saved = [x for x in self.bbox if x['col'] != self.col['value']]
            bbox_unsaved = [x for x in self.bbox if x['col'] == self.col['value']]
            self.bbox = bbox_saved + bbox_unsaved[:-1]
            self.img = self.clone.copy()
            for box in self.bbox:
                cv2.rectangle(self.img, box['coords'][0], box['coords'][1], box['col'], 2)

    def store_bbox(self):
        """stores a bounding box in the list of bounding boxes"""
        path = self.full_paths[self.i]
        hash = self.hashes[self.i]
        my_paths = [x.get('path') for x in self.iboxes]
        if path not in my_paths:
            self.iboxes.append({'path': path, 'hash': hash, 'bbox': self.bbox})
        else:
            self.iboxes[:] = [{'path': path, 'hash': hash, 'bbox': self.bbox}
                              if x.get('path') == path else x for x in self.iboxes]

    def get_bbox(self):
        """gets stored bounding boxes for current photo"""
        path = self.full_paths[self.i]
        newbox = []
        for x in self.iboxes:
            if x.get('path') == path:
                newbox = x.get('bbox')
                break
        self.bbox = newbox

    def store_score(self, scr):
        """stores scores data in the bounding box list"""
        for box in self.iboxes:
            for b in box['bbox']:
                if b['col'] == self.col['value'] and b.get('score') is None:
                    b['score'] = scr

    def store_sequence(self, na=False):
        """stores bounding boxes and scores from a scored sequence into the database"""
        dt_now = datetime.now(get_localzone())
        cnt = 0
        cnx = sqlite.connect(self.dbpath)
        r = cnx.cursor()
        isql = "INSERT OR IGNORE INTO condition (md5hash, seq_id, bbox, rating, scorer_name, score_dt) " \
               "VALUES (?, ?, ?, ?, ?, ?);"
        ssql = "INSERT OR IGNORE INTO condition_seqs (seq_id, scorer_name, scores) VALUES (?, ?, ?);"
        if na:
            r.execute(ssql, (self.seq_id, self.name, False))
        else:
            for ibox in self.iboxes:
                if ibox['bbox']:
                    for b in ibox['bbox']:
                        if b.get('score') is not None:
                            str_coords = ', '.join((str(b['coords'][0][0]), str(b['coords'][0][1]),
                                                    str(b['coords'][1][0]), str(b['coords'][1][1])))
                            params = (ibox['hash'], self.seq_id, str_coords, b['score'], self.name, dt_now)
                            # print(params)
                            r.execute(isql, params)
                            cnt += 1
            if cnt > 0:
                r.execute(ssql, (self.seq_id, self.name, True))
        cnx.commit()
        cnx.close()
        return cnt

    def get_next(self):
        """moves the images sequence onto the next available set"""
        self.scored_seqs = self.get_scored()
        self.photo_seqs = list(self.photos['seq_id'])
        self.scored_filt = set([x for x in self.photo_seqs if x in self.scored_seqs])
        self.reset_vars()
        print(len(self.scored_filt), "sequences already scored within provided parameters.")
        self.full_paths, self.hashes, self.seq_id = get_sample(self.photos, self.scored_seqs, self.basepath,
                                                               self.random, self.set_start)
        if self.full_paths is None:
            print("No more unscored images with given parameters. Quitting...")
            self.quit_script = True
            return
        self.animal_id = self.get_animalid()
        print(self.animal_id, " is current scoring target for ", len(self.full_paths), " photos (seq_id: ",
              self.seq_id, ")", sep='')
        self.img = cv2.imread(self.full_paths[self.i])
        self.clone = self.img.copy()

    def start(self):
        """starts the image display and scoring window process"""
        self.get_next()
        cv2.namedWindow(self.win_name, cv2.WINDOW_NORMAL)
        # keep looping until the 'q' key is pressed
        while not self.quit_script:
            # display the image and wait for a keypress
            cv2.setMouseCallback(self.win_name, self.click_and_crop)
            cv2.imshow(self.win_name, self.img)
            # the & 0xFF chops off the last 8 bits of the binary value of the key, negating things like numlock changing
            # the key value
            self.raw_key = cv2.waitKeyEx(1)
            self.key = self.raw_key & 0xFF

            # if the 'r' key is pressed, reset the cropping region
            if self.key == ord("r"):
                self.img = self.clone.copy()
                self.bbox = []
                self.iboxes = []
                print("Resetting all stored ratings for this sequence.")

            # scroll using brackets or arrow keys
            elif self.key == ord(",") or self.raw_key == 2424832 or self.key == ord(".") or self.raw_key == 2555904:
                self.store_bbox()
                if self.key == ord(",") or self.raw_key == 2424832:
                    self.i = max(0, self.i-1)
                elif self.key == ord(".") or self.raw_key == 2555904:
                    self.i = min(len(self.full_paths) - 1, self.i + 1)
                self.img = cv2.imread(self.full_paths[self.i])
                self.clone = self.img.copy()
                self.get_bbox()
                for box in self.bbox:
                    cv2.rectangle(self.img, box['coords'][0], box['coords'][1], box['col'], 2)

            # zoom in on last box
            elif self.key == ord("z"):
                last_box = self.bbox[-1]
                x = sorted([x[0] for x in last_box['coords']])
                y = sorted([y[1] for y in last_box['coords']])
                cropped = self.clone.copy()[y[0]:y[1], x[0]:x[1]]
                cv2.namedWindow("cropped", cv2.WINDOW_NORMAL)
                while(True):
                    cv2.imshow("cropped", cropped)
                    raw_key = cv2.waitKeyEx(1)
                    key = raw_key & 0xFF
                    if key == ord("q") or key == 27 or cv2.getWindowProperty("cropped", cv2.WND_PROP_VISIBLE) < 1:
                        break
                cv2.destroyWindow("cropped")

            # save the bbox info and ratings
            elif self.key == ord("s"):
                while True:
                    try:
                        score = input(' '.join(('Input score for', self.col['name'], 'bounded animals: ')))
                        if not is_number(score) and score != 'c':
                            raise ValueError
                        break
                    except ValueError:
                        print("Invalid entry. Entry must be numeric or 'c' to cancel input.")
                if score != 'c':
                    self.store_bbox()
                    self.store_score(score)
                    self.col_i += 1
                    if self.col_i > (len(self.colors) - 1):
                        self.col_i = 0
                    self.col = self.colors[self.col_i]

            # quit or move to next sequence
            elif self.key == ord("n") or self.key == ord("q") or self.key == ord("x") or \
                    cv2.getWindowProperty(self.win_name, cv2.WND_PROP_VISIBLE) < 1:
                if self.key == ord("x"):
                    recs = self.store_sequence(na=True)
                    print("Sequence marked as NA.")
                elif self.key != ord("x"):
                    recs = self.store_sequence()
                    if recs > 0:
                        print(recs, "ratings stored.")
                    else:
                        self.set_start += 1
                if self.key == ord("q") or cv2.getWindowProperty(self.win_name, cv2.WND_PROP_VISIBLE) < 1:
                    print('Quitting...')
                    self.quit_script = True
                    break

                self.get_next()
                if self.quit_script:
                    break
                self.img = cv2.imread(self.full_paths[self.i])
                self.clone = self.img.copy()
        cv2.destroyWindow(self.win_name)


if __name__ == "__main__":
    # parses script arguments
    parser = argparse.ArgumentParser(
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=os.linesep.join(("This script will sample photos fitting certain "
                                     "descriptions and display them for reviewing. ",
                                     "Navigation keys on images (L = left, R = right, M = mouse, B = button): \n",
                                     "'LR' arrow keys or '<>' to navigate through photo sequence. ",
                                     "'LMB', drag, and release to draw bounding box. ",
                                     "'RMB' to undo unsaved bounding box on current photo. ",
                                     "'r' to reset all bounding boxes on all photos in sequence. ",
                                     "'s' to save current colors of bounding boxes in all sequences and "
                                     "submit a score. ",
                                     "'n' to move on to next random sequence and store scoring. ",
                                     "'x' to indicate no scoring is possible for a sequence. ",
                                     "'q' to store scoring and quit. ",
                                     "'z' to create a separate resizable window with contents of the last drawn "
                                     "bounding box for zooming ('esc' or 'q' to quit).",
                                     "'c' to cancel current scoring (within shell prompt).")))
    # positional arguments
    parser.add_argument('dbpath', help='path to sqlite database.')
    parser.add_argument('base_path', help='base folder for photos.')
    parser.add_argument('scorer_name', help='the full name of the person doing the scoring (e.g. "Firstname Lastname")')
    parser.add_argument('-a', '--animal', nargs='*', help='The id of the animal(s) to restrict photos to '
                                                          '(e.g. "Equus ferus caballus").')
    parser.add_argument('-d', '--date_range', nargs=2, help='date ranges to filter by (2 arguments in YYYY-MM-DD)'
                        ' format or MM-DD format.')
    parser.add_argument('-s', '--site_name', nargs='*', help='site name(s) to filter by (e.g. "Austin" '
                        '"Becky Springs").')
    parser.add_argument('-c', '--camera', nargs='*', help='camera identifier(s) for those sites with multiple cameras.')
    parser.add_argument('-C', '--classifier', nargs='*', help='the name of the person who classified the photo to '
                                                              'filter by.')
    parser.add_argument('-v', '--verbose', action='store_true', help='include more verbose output for debugging.')
    parser.add_argument('-q', '--seq_id', nargs='+',
                        help='Specific sequence id(s) to retrieve instead of a random sample. At least one seq_id must'
                             'be provided.')
    parser.add_argument('-Q', '--seq_file',
                        help='The local path to a delimited file containing seq_ids to sample (1 per row, no header).')
    args = parser.parse_args()

    my_seqs = copy.deepcopy(args.seq_id)
    args.seq_id = construct_seq_list(args.seq_file, my_seqs)
    if args.seq_id is None:
        rnd = True
    else:
        rnd = False
    construct_tables(args.dbpath)

    # dbpath, animal = None, date_range = None, site_name = None, camera = None, seq_id = None, classifier = None,
    # verbose = False, df = True
    my_sql, my_params, my_photos = get_photos(dbpath=args.dbpath, animal=args.animal, date_range=args.date_range,
                                              site_name=args.site_name, camera=args.camera, seq_id=args.seq_id,
                                              classifier=args.classifier)
    if len(my_photos) == 0:
        print("No photos match script criteria. Quitting...")
        quit()
    scenes = RatePhotos(photos=my_photos, dbpath=args.dbpath, basepath=args.base_path, name=args.scorer_name,
                        win_name='image', random=rnd)
    scenes.start()
    cv2.destroyAllWindows()  # just in case
    print("script finished.")


