from photo_mgmt.create_db import get_sqlite_con
import cv2
from tqdm import tqdm
import multiprocessing as mp


def get_size(full_path):
    im = cv2.imread(full_path)
    if im is not None:
        height, width = im.shape[:2]
    else:
        height, width = None, None
    return {'path': full_path, 'width': width, 'height': height}


db_path = r'/home/wlieurance/network/gis/Photos/tools/animal.sqlite'
con = get_sqlite_con(db_path)
c = con.cursor()
c.execute("SELECT md5hash, path FROM photo WHERE width IS NULL OR height IS NULL;")
rows = c.fetchall()
results = []
paths = [("/home/wlieurance/network/gis/Photos/cameras/" + row['path'],) for row in rows]

with mp.Pool(processes=100) as pool:
    results = pool.starmap(get_size, paths)

processed = [{'path': x.get('path').replace('/home/wlieurance/network/gis/Photos/cameras/', ''),
              'width': x.get('width'), 'height': x.get('height')} for x in results]
isql = "UPDATE photo SET width = :width, height = :height WHERE path = :path;"
c.executemany(isql, processed)
con.commit()
