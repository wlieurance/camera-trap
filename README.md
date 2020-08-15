# camera_trap
A set of tools for storing and managing camera trap data.

# Installation
Users will need [python 3](https://www.python.org) on their computer and will need to install required dependencies via *pip -r requirements.txt*.
Users will also need to install the [spatialite](https://www.gaia-gis.it/fossil/libspatialite/index) library *(mod_spatialite)* and add it to your system path if it is not already present.
Users are also encouraged to download and install the requirements for the author's [photo management tools](https://github.com/wlieurance/photo_mgmt.git), though a user could certainly write their own tool or use another one they prefer more.

# Usage
Currently this tool set is in early development and not plug-and-play directly in that running a script will not directly produce an end product (i.e. a spatialite database populated with relevant records.) It is instead a set of python and SQL scripts that can be run to populate a SQLite database with some user intervention for things that cannot or are not automated (yet). Some bare minimum knowledge of SQL is currently required and a SQLite/spatialite db browser is recommended. (The author personally recommends [DB Browser for SQLite](https://sqlitebrowser.org/)). 

The following list is a suggested order of operations:
1. Use a script to scan the directory containing your photo files and write results to a spatialite database. [This](https://github.com/wlieurance/photo_mgmt/blob/master/PhotoMetadata.py) is one the author uses. This will populate a database with tables (photo and tag) that will be used to populate the cameratrap db.  
2. Run *create_db.py* in order to create an empty database.
3. Import records from the photo scan database to the camera trap database (tables *photo* and *tag*)
4. Populate **site_name** (optional helper: *regex_sites.py*), **camera_id** (manual), **taken_dt**, and **taken_yr** (*sql/update_photo_takendt.sql*) in the cameratrap db *photo* table.
	1. Note that the '-06:00' in the update_photo_takendt.sql query appends a time zone to localize the taken time contained in the exif tags. Users will want to customize this. 
5. Populate the *site* table (manual).
6. Populate the *camera* table (manual).  Note that the camera table optionally contains a 'POINTZ' geometry field. It can be populated with SQL using the [MakePointZ()](http://www.gaia-gis.it/gaia-sins/spatialite-sql-4.3.0.html) function.
7. Populate **season_no** and **season_order** in *photo* (*sql/update_photo_seasons.sql*). **season_no** establishes breaks for each **site_name**/**camera_no** if there have been more than 30 days between photos. **season_order** is an ascending integer for each photo within each season ordered by date.
8. Populate the *animal* table with identifications (manual). Each users methodology for doing animal id will be different (The author recommends using [Timelapse](http://saul.cpsc.ucalgary.ca/timelapse/) and Microsoft's [CameraTraps](https://github.com/microsoft/CameraTraps) repository).  The user should import a csv into the database containing their identifications and delete the imported table after populating.
9. Populate the *sequence* (*sql/sequences.sql*) table and use that to populate the **seq_id** field in the *animal* table (*sql/update_animal_sequences.sql*). These fields allow a set of photos containing the same animal id within a certain time restriction to be classified with a unique sequence (e.g. all photos containing animal X with less than time Y between them are the same sequence).

In the future, some or all of the tasks in here should be automated, ideally creating the database with one or two script calls. 

Additional functionality is provided by the following scripts once a database is populated:
1. *filter_detections.py*: This allows a user to filter out a json produced from Microsoft’s CameraTraps API to either only photos with entries in the *animal* table or only photos not in the *animal* table.
2. *pull_random.py*: This script will take a random selection of photos matching certain criteria and copy them to a new directory.  Useful for looking at a random subset of animal identifications.
3. *sample.py*: This script will pull up either a list of sequences given to it or a random subset of sequences matching certain criteria for a user to see.  It allows the user to draw bounding boxes and around animals and provide a numerical rating to them.  Ratings and boxes are stored in the database in the *condition* and *condition_seqs* tables.
4. *subset.py*: This script will subset the original database to only photos matching certain criteria.  It is useful for making a database that only has certain animal detections or date ranges in it.

# Contributing
If you want to add automation, error checking or other features to anything here, please feel free to contact the author.

Suggested projects:
1. Automate the db creation process.
2. Expand the sample.py script to be not just a rating tool but a fully-fledged animal id tool which can store records in the *animal* table.  Most of the necessary code to do this already exists in this script.

# Credits 
Author: Wade Lieurance

# License 
Gnu Public License v3
