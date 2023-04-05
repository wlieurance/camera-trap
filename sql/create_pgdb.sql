CREATE TABLE site(site_name VARCHAR, state_code VARCHAR,
PRIMARY KEY(site_name));

CREATE TABLE camera (
    site_name VARCHAR,
    camera_id VARCHAR,
    fov_length_m REAL,
    fov_area_sqm REAL,
    lat REAL,
    long REAL,
    elev_m REAL,
    geom geometry(POINTZ, 4326),
    PRIMARY KEY(site_name, camera_id),
    FOREIGN KEY(site_name) REFERENCES site(site_name) ON DELETE CASCADE);

CREATE TABLE import (
    import_date TIMESTAMP PRIMARY KEY, 
    base_path VARCHAR, 
    local BOOLEAN, 
    type VARCHAR);

CREATE TABLE hash (
    md5hash VARCHAR PRIMARY KEY, 
    import_date TIMESTAMP,
    FOREIGN KEY (import_date) REFERENCES import(import_date) ON DELETE CASCADE);

CREATE TABLE photo (
    path VARCHAR PRIMARY KEY,
    fname VARCHAR,
    ftype VARCHAR,
    site_name VARCHAR,
    camera_id VARCHAR,
    dt_orig TIMESTAMP,
    year_orig INTEGER,
    dt_mod TIMESTAMP,
    season_no INTEGER,
    season_order INTEGER,
    md5hash VARCHAR,
    FOREIGN KEY (md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE ON UPDATE CASCADE,
    FOREIGN KEY (site_name, camera_id) REFERENCES camera(site_name, camera_id) 
    ON DELETE CASCADE ON UPDATE CASCADE);

CREATE TABLE tag (
    md5hash VARCHAR,
    tag VARCHAR,
    value VARCHAR,
    PRIMARY KEY(md5hash, tag),
    FOREIGN KEY (md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE ON UPDATE CASCADE);

CREATE TABLE sequence (
    seq_id VARCHAR,
    site_name VARCHAR,
    camera_id VARCHAR,
    id VARCHAR,
    seq INTEGER,
    min_dt TIMESTAMP,
    max_dt TIMESTAMP,
    FOREIGN KEY(site_name, camera_id) REFERENCES camera(site_name, camera_id) ON DELETE CASCADE,
    PRIMARY KEY(seq_id));
        
CREATE TABLE generation (
    gen_id VARCHAR,
    gen_dt TIMESTAMP,
    dbpath VARCHAR,
    seq_file VARCHAR,
    classifier VARCHAR,
    animal VARCHAR,
    date_range VARCHAR,
    site_name VARCHAR,
    camera VARCHAR,
    overwrite INTEGER,
    seq_no INTEGER,
    filter_condition INTEGER,
    filter_generated INTEGER,
    partition VARCHAR,
    subsample REAL,
    label VARCHAR,
    PRIMARY KEY(gen_id));

CREATE TABLE sequence_gen (
    seq_id VARCHAR,
    gen_id VARCHAR,
    FOREIGN KEY(seq_id) REFERENCES sequence(seq_id) ON DELETE CASCADE,
    FOREIGN KEY(gen_id) REFERENCES generation(gen_id) ON DELETE CASCADE,
    PRIMARY KEY(seq_id, gen_id));

CREATE TABLE animal (
    md5hash VARCHAR,
    id VARCHAR,
    cnt INTEGER,
    classifier VARCHAR,
    seq_id VARCHAR,
    coords VARCHAR,
    PRIMARY KEY(md5hash, id),
    FOREIGN KEY(md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE,
    FOREIGN KEY(seq_id) REFERENCES sequence(seq_id) ON DELETE SET NULL);

CREATE TABLE condition (
    md5hash VARCHAR, 
    seq_id VARCHAR, 
    rating NUMERIC, 
    scorer_name VARCHAR, 
    score_dt TIMESTAMP, 
    bbox_x1 INTEGER,
    bbox_y1 INTEGER,
    bbox_x2 INTEGER,
    bbox_y2 INTEGER,
    PRIMARY KEY(md5hash, seq_id, scorer_name, bbox_x1, bbox_y1, bbox_x2, bbox_y2), 
    FOREIGN KEY(md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE);

CREATE TABLE condition_seqs (
    seq_id VARCHAR, 
    scorer_name VARCHAR,
    scores BOOLEAN, 
    PRIMARY KEY(seq_id, scorer_name), 
    FOREIGN KEY(seq_id) REFERENCES sequence(seq_id) ON DELETE CASCADE);

CREATE VIEW gen_seq_count AS 
WITH gen_count AS (
SELECT gen_id, count(seq_id) AS n
  FROM sequence_gen
 GROUP BY gen_id
)

SELECT a.*, CASE WHEN b.n IS NULL THEN 0 ELSE b.n END AS n
  FROM generation a
  LEFT JOIN gen_count b ON a.gen_id = b.gen_id;

