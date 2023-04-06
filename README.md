# Camera Trap Management Tools
A set of CLI tools which:

1. Utilizes [Photo Management Tools](https://github.com/wlieurance/photo_mgmt)
   for relational DB storage.
2. Generate sequences IDs for time and space associated images of an object.
3. Randomly sample sequences based on given criteria.
4. Draw and store bounding box info and rating value for objects.

# Installation
Users will need [python 3](https://www.python.org) on their computer and will
need to install required dependencies via *pip install -r requirements.txt*.
SQLite back end users will also need to install the
[spatialite](https://www.gaia-gis.it/fossil/libspatialite/index) library
*(mod_spatialite.dll/so)* and add it to their system path (if it is not already
present). PostgreSQL back end users will, similarly, need the
[PostGIS](https://postgis.net/) extension.

Users are also encouraged to download (and install the requirements for) the
author's [photo management tools](https://github.com/wlieurance/photo_mgmt.git).
This tool creates a database of photo paths, md5 hashes and EXIF tags. A user
could use another tool they prefer more and export the results into a SQLite
database with proper table and column names (see create\_db function within the
create\_db.py script for reference).

# Usage 
The user must first create the database with the
[create_db.py](create_db.py) script. Required script inputs are: 
## Arguments
1. **outpath**: The file path to which to save the newly created database (e.g.
   *my/path/traps.sqlite* or *"C:\My Path\Traps\camera_traps.db"*).
2. **inpath**: The path to an existing spatialite/sqlite database produced via
   the [PhotoMetadata.py](https://github.com/wlieurance/photo_mgmt.git) script,
   or a user crated database with at least two tables, **photo** and **tag**,
   with columns/types matching ones produced via the PhotoMetadata.py script.
   Please note that this script will always store file paths with the '/'
   separator, regardless of the native OS file path separator.
3. **site_path**: This is the path to a comma delimited csv file containing a
   user's site definitions. This csv file's text fields must be quoted if they
   contain commas and have field names as the first line of the csv. The csv
   needs to contain the following fields of specific type:
	1. **site_name** of type *TEXT*. This field contains a list of unique
	   site names that the photos belong to, such as specific camera
	   locations.
	2. **regex** of type *TEXT*. This field needs to be populated with a
	   search string (regex type) that will identify which photos belong to
	   which site_name based on their file path. Examples:
	   1.*/my/path/site1/001.jpg*: if all of site1 photos are contained in
	   this type of subdirectory then **site1** should be used as the regex
	   field.
		2. */my/path/Site A (2018)/*: if a user wants all Site A 2018
		   photos in one site and all Site A 2019 photos in another then
		   **Site A \(2018\)** should be the regex string. Remember to
		   escape the following regex metacharacters with a backslash
		   '\': [\^$.|?*+(){}.
	3. **state_code** of type *TEXT* (optional). This field contains the
	   site's state identifier (e.g. NV, IA, OR, etc.).
4. **camera_path**: This is the path to a comma delimited csv file containing a
   user's camera definitions. This csv file's text fields must be quoted if they
   contain commas and have field names as the first line of the csv. Csv’s with
   valid lat and long fields populated for each camera will be given a
   SpatiaLite geometry which can be used in a spatial context (GIS, the R sf
   library, etc.). The csv needs to contain the following fields of specific
   type:
	1. **site_name** of type *TEXT*. This field points back to the site_name
	   field uploaded via the site csv.
	2. **camera_id** of type *TEXT*. This field identifies a camera uniquely
	   for each site (i.e. the combination of site_name and camera_id must
	   be unique).
	3. **regex** of type *TEXT* (optional but suggested). This field acts
	   exactly the same as the site regex field except that it is used to
	   identify which photos contain which camera labels. This field is
	   optional, but beware that not including it will default all of your
	   photos to having a camera_id = '1', which will result in foreign key
	   errors in the database ex post facto if not all of your camera_id
	   fields in the camera_path csv are '1'.
	4. **fov_length_m** of type *REAL* (optional). A field of numeric values
	   documenting the field of view length in meters.
	5. **fov_area_sqm** of type *REAL* (optional). A field of numeric values
	   documenting the field of view size in square meters.
	6. **lat** of type *REAL* (optional). A numeric field containing the
	   Latitude/Y (srid provided with the --srid option) position of the
	   camera.
	7. **long** of type *REAL* (optional). A numeric field containing the
	   Longitude/X (srid provided with the --srid option) position of the
	   camera.
	8. **elev_m** of type *REAL* (optional). A numeric field containing the
	   Altitude position of the camera in meters.

## Options
1. **animal_path**: This is the path to a comma delimited csv file containing a
   user's animal detections for each photo. This csv file's text fields must be
   quoted if they contain commas and have field names as the first line of the
   csv. The combination of the path and id fields must be unique. The csv needs
   to contain the following fields of specific type:
	1. **path** of type *TEXT*. This is the file path to the photo
	   containing the detection. This needs to be exactly the same as the
	   file path produced via the PhotoMetadata.py database.
	2. **id** of type *TEXT*. This is the identifier of the object in the
	   photo (e.g. horse, vehicle, human, Canis lupus). Users are encouraged
	   for their own data management purposes to have a unifying scheme for
	   object naming, such as using all scientific names where possible or
	   using all common names, etc.
	3. **cnt** of type *INTEGER*. This field contains the number of each
	   object seen in the photo.
	4. **classifier** of type TEXT (optional). This field contains the name
	   of the individual responsible for identifying the objects for this
	   record.
	5. **coords** of type TEXT (optional). This is a field containing comma
	   delimited coordinates of point locators for each object
	   identification in terms of their X and Y values as a percentage of
	   total photo width and height. There can be multiple %X,%Y coordinate
	   sets delimited by pipes (|). Examples: 0.436,0.712 and
	   0.355,0.448|0.607,0.415. All values for this field within the csv
	   would need to be enclosed in double quotes due to the field
	   containing commas.
2. **srid**: This option allows the user to provide the Spatial Reference
   Identifier used to set the coordinate system of the database. Defaults to
   4326 (WGS 84).
3. **tags**: This option allows a user give a number of EXIF tags names used to
   pare down which EXIF tags are to be stored in the new database's tag table.
   The 'EXIF DateTimeOriginal' tag will be imported in the tag table regardless.
   EXIF tags can dramatically increase the size of the database.
4. **remove_thumbnail**: This option will remove all thumbnail BLOBs stored in
   the tag table as well as any EXIF tag containing the word Thumbnail. Provided
   as a way to decrease database size.
5. **season_break**: This option allows a user to set 'seasons' of camera use by
   giving the maximum number of days that can occur without photos before a new
   season is established. For example, say a camera is set up for three months
   in the summer and three months in the winter. If season_break is set to less
   than 90, then photos will be grouped into two seasons for that year, season 1
   and season 2. Defaults to 30 days.
6. **sequence_break**: This field sets the maximum amount of time in minutes
   that is required to break out a photo sequence in the animal table. Thus if
   there are five records with the identifier of 'Canis lupus' in the animal
   table, and there is no time gap longer than sequence\_break between them, they
   will be classified as the same sequence in the animal table.
7. **timezone**: This option is a string identifying the timezone within which
   the photo was taken. It is used to localize the timestamp within the
   database. If no timezone is given, the timestamp remains unlocalized.
   Importing pytz within python and running `pytz.all_timezones` will provide a
   full list of usable time zones. The *TZ database name* field
   [here](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones) also
   provides a good list.

## Other Usage 
Additional functionality is provided by the following scripts
once a database is populated:
1. [filter_detections.py](filter_detections.py): This allows a user to filter
   out a json produced from Microsoft’s CameraTraps API to either only photos
   with entries in the *animal* table or only photos not in the *animal* table.
2. [pull_random.py](pull_random.py): This script will take a random selection of
   photos matching certain criteria and copy them to a new directory. Useful for
   looking at a random subset of animal identifications.
3. [sample.py](sample.py): This script will pull up either a list of sequences
   given to it or a random subset of sequences matching certain criteria for a
   user to see. It allows the user to draw bounding boxes around animals and
   provide a numerical rating for them. Ratings and boxes are stored in the
   database in the *condition* and *condition\_seqs* tables.
4. [subset.py](subset.py): This script will subset the original database to only
   photos matching certain criteria. It is useful for making a subdet database
   that only has certain object detections or date ranges in it.
5. [generate_seqs.py](generate_seqs.py): This script will sample animal
   sequences fitting certain criteria (much like **sample.py**) and export the
   sampled sequences to a delimited file, to be used with **sample.py**.  This
   allows multiple people to view/rate the exact same random sub-sample of
   available sequences in the database.

# Contributing 
If you want to add error checking or other features to anything
here, please feel free to contact the author.

Suggested projects:
1. Expand the sample.py script to be not just a rating tool but a fully-fledged
   animal id tool which can store records in the *animal* table. Most of the
   necessary code to do this already exists in this script.

# Credits 
Author: Wade Lieurance

# License 
See [LICENSE](LICENSE).
