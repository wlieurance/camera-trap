# This utility script is still in development and testing and will probably 
# break in edge cases. It is meant to extract count and coordinate data from a 
# timelapse sqlite database (.ddb). Users will likely have to heavily modify the
# script to deal with their own timelapse db designs, as timelapse databases are
# created by the user for each project.

library(DBI)
library(tibble)
library(dplyr)
library(tidyr)
library(stringr)
library(foreach)
library(jsonlite)
library(doParallel)
scan_path <- ("~/network/gis/Photos/cameras")

re <- function(pattern){
  return(stringr::regex(pattern, ignore_case = TRUE))
}

# Users will need to modify this table for their own database designs
defaults <- tibble::tribble(
  ~field, ~id,
  'vehicle_cnt','vehicle',
  'equipment_cnt','equipment',
  'human_cnt','Homo sapiens',
  'horse_cnt','Equus ferus caballus',
  'cattle_cnt','Bos taurus',
  'sheep_cnt','Ovis aries',
  'muledeer_cnt','Odocoileus hemionus',
  'elk_cnt','Cervus canadensis nelsoni',
  'antelope_cnt','Antilocapra americana',
  'ungulate_cnt','Ungulata order',
  'cervid_cnt','Cervidae genus',
  'crow_cnt','Corvus brachyrhynchos',
  'raven_cnt','Corvus corax',
  'magpie_cnt','Pica hudsonia',
  'corvid_cnt','Corvidae genus',
  'sagegrouse_cnt','Centrocercus urophasianus',
  'raptor_cnt','Telluraves order',
  'bird_cnt','Aves order',
  'coyote_cnt','Canis latrans',
  'dog_cnt','Canis lupus familiaris',
  'jackrabbit_cnt','Lepus californicus',
  'animal_cnt','Animalia phylum'
)


dbs = list.files(path = scan_path, pattern = ".ddb$", recursive = TRUE)
db = dbs[!grepl("Backups", dbs)]

count_sql = "SELECT * FROM DataTable;"

coords_sql = paste(
  "SELECT a.*, b.other1_id, b.other2_id, b.RelativePath, b.File, b.class_name",
  "  FROM MarkersTable a",
  " INNER JOIN DataTable b ON a.Id = b.Id;",
  sep = '\n')

# extract all data for each .ddb file found
all <- foreach(i=1:length(db), .combine = dplyr::bind_rows) %do% {
  db_path = file.path(scan_path, db[i])
  con <- DBI::dbConnect(RSQLite::SQLite(), db_path)
  coords_tbl <- tibble::as_tibble(dbGetQuery(con, coords_sql))
  count_tbl <- tibble::as_tibble(dbGetQuery(con, count_sql))
  DBI::dbDisconnect(con)
  rm(con)
  
  regular_count <- dplyr::select(count_tbl, -starts_with('other'), -Id, -DateTime, -DeleteFlag, -note, -empty)
  other_count <- dplyr::select(count_tbl, starts_with('other'), RelativePath, File, class_name)
  regular_coords <- dplyr::select(coords_tbl, -starts_with('other'), -Id)
  other_coords <- dplyr::select(coords_tbl, starts_with('other'), RelativePath, File, class_name)
  
  reg_coords_long <- regular_coords |> 
    tidyr::pivot_longer(cols = !matches(c('RelativePath', 'File', 'class_name')),
                        names_to = "field", values_to = "coords") |>
    dplyr::filter(coords != "[]") |>
    dplyr::left_join(defaults, by=c("field"="field")) |>
    dplyr::mutate(field_type = "regular") |>
    dplyr::mutate(dbpath = db_path)
  
  reg_count_long <- regular_count |> 
    tidyr::pivot_longer(cols = !matches(c('RelativePath', 'File', 'class_name')),
                        names_to = "field", values_to = "count") |>
    dplyr::mutate(count = as.integer(count)) |>
    # dplyr::filter(count != 0) |>
    dplyr::left_join(defaults, by=c("field"="field")) |>
    dplyr::mutate(field_type = "regular") |>
    dplyr::mutate(dbpath = db_path)
  
  reg_long <- reg_count_long |>
    dplyr::left_join(select(reg_coords_long, File, RelativePath, field, coords),
                     by = c("File" = "File", "RelativePath" = "RelativePath", 
                            "field" = "field")) |>
    filter(count > 0 | !is.na(coords))
  
  other1_count <- other_count |>
    dplyr::select(RelativePath, File, class_name, other1_id, other1_cnt) |>
    dplyr::rename(field = other1_id, count = other1_cnt) |>
    dplyr::filter(field != "") |>
    mutate(count = as.integer(count))
  other2_count <- other_count |>
    dplyr::select(RelativePath, File, class_name, other2_id, other2_cnt) |>
    dplyr::rename(field = other2_id, count = other2_cnt) |>
    dplyr::filter(field != "") |>
    mutate(count = as.integer(count))
  other_count_bind <- dplyr::bind_rows(other1_count, other2_count)
  
  other1_coords <- other_coords |>
    dplyr::select(RelativePath, File, class_name, other1_id, other1_cnt) |>
    dplyr::rename(field = other1_id, coords = other1_cnt) |>
    dplyr::filter(field != "" & coords != "[]")
  other2_coords <- other_coords |>
    dplyr::select(RelativePath, File, class_name, other2_id, other2_cnt) |>
    dplyr::rename(field = other2_id, coords = other2_cnt) |>
    dplyr::filter(field != "" & coords != "[]")
  other_coords_bind <- dplyr::bind_rows(other1_coords, other2_coords)
  
  other_bind <- other_count_bind |>
    dplyr::left_join(select(other_coords_bind, File, RelativePath, field, coords),
                     by = c("File" = "File", "RelativePath" = "RelativePath", 
                            "field" = "field")) |>
    dplyr::filter(count > 0 | !is.na(coords))
  
  # this dplyr rename chain needs to be modified on a User to User basis and is quite specific to each project.
  # it takes counts stored in two text fields (other1_id and other2_id) and their respective count field
  # (other1_cnt, other2_cnt) and replaces common names, sloppy names, etc.  Users will have to modify this for their own
  # db design and use cases.
  other_rename <- other_bind |>
    dplyr::mutate(field = trimws(field)) |>
    dplyr::mutate(id = field) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^chu[ck]?kar$}"), "Alectoris chukar")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^golden eagle$}"), "Aquila chrysaetos")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^american robin$}"), "Turdus migratorius")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^brewer'?s sparrow$}"), "Spizella breweri")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^mou?rning dove$}"), "Zenaida macroura")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^western meadow ?lark$}"), "Sturnella neglecta")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^badger$}"), "Taxidea taxus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^black ?bird$}"), "Icteridae genus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^bobcat$}"), "Lynx rufus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^chipmunc?k$}"), "Neotamias sp.")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^common poorwill.*$}"), "Phalaenoptilus nuttallii")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^dog$}"), "Canis lupus familiaris")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^dove$}"), "Columbidae genus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^crane$}"), "Gruidae genus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^dragonfly$}"), "Odonata family")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^butterfly$}"), "Lepidoptera family")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^duck$}"), "Anatidae genus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^fox.*$}"), "Vulpes vulpes")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^great blue heron$}"), "Ardea herodias")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^hare$}"), "Lepus sp.")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^heron$}"), "Ardeidae genus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^killdeer$}"), "Charadrius vociferus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^k(?:angaroo )*rat$}"), "Dipodomys ordii")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^lodgepole chipmunk$}"), "Neotamias speciosus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^mallard.*$}"), "Anas platyrhynchos")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^marmot$}"), "Marmota flaviventris")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^(?:mountain lion|cougar)$}"), "Puma concolor")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^(?:mouse|rodent).*$}"), "Rodentia order")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^[a-z]*\s*o?possum.*$}"), "Didelphis virginiana")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^owl$}"), "Strigiformes family")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^say'?s\s*(?:phoebe)*$}"), "Sayornis saya")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^(?:skunk|mephitidae)$}"), "Mephitidae genus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^snake$}"), "Serpentes family")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^(?:squirrel|scuridae)$}"), "Sciuridae genus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^turkey v[ou]lture$}"), "Cathartes aura")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^ungulat[ea]$}"), "Ungulata order")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^cervida?e?$}"), "Cervidae genus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^corvida?e?$}"), "Corvidae genus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^raptor$}"), "Telluraves order")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^bird$}"), "Aves order")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^animali?a?$}"), "Animalia phylum")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^blue ?jay$}"), "Corvidae genus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^burrowing owl.*$}"), "Athene cunicularia hypugaea")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^(?:bug|insect).*$}"), "Insecta order")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^egret$}"), "Ardeinae genus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^(?:greater )*sage[ \-]grouse$}"), "Centrocercus urophasianus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^tamias$}"), "Neotamias sp.")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^white[ \-]faced ibis$}"), "Plegadis chihi")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^(?:black)*\s*bear$}"), "Ursus americanus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^lizard$}"), "Squamata suborder")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^blue ?bird$}"), "Sialia sp.")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^goose.*$}"), "Anatidae genus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{^goose.*$}"), "Anatidae genus")) |>
    dplyr::mutate(id = stringr::str_replace(id, re(r"{\bspecies\b}"), "sp."))
  
  other_long <- other_rename |>
    dplyr::mutate(field_type = "other") |>
    dplyr::mutate(dbpath = db_path)
  
  all_sub <- dplyr::bind_rows(reg_long, other_long) |>
    arrange(RelativePath, File, field_type, field) |>
    rename(rel_path = RelativePath, file = File)
}

# shows output in order to visually inspect for bad values that still remain.
all |> dplyr::group_by(id) |> 
  dplyr::summarize(n=dplyr::n(), .groups = "drop") |>
  print(n=500)

final <- all |>
  mutate(path = stringr::str_replace_all(file.path(dirname(dbpath), rel_path, file), r"(\\)", "/")) |>
  mutate(path = stringr::str_replace_all(path, "/+", "/")) |>
  dplyr::mutate(coords = ifelse(is.na(coords), "[]", coords)) 

# this next section is only for getting the md5hashes and coordinates for the files in case the path is not longer
# up-to-date.  Hashing and getting dims for the photos is already done by the scan_image.py script from the photo_mgmt
# library, which is imported by this project
  
#paths  <- (final |> dplyr::group_by(path) |> summarize(.groups="drop"))$path

#clust <- parallel::makeCluster(24, type = "FORK") 
#doParallel::registerDoParallel(clust)
#file_data  <- foreach (i=1:length(paths), .combine=bind_rows) %dopar% {
#  path = paths[i]
#  hash  <- tools::md5sum(path)
#  im <- imager::load.image(path)
#  width <- imager::width(im)
#  height  <- imager::height(im)
#  as_tibble_row(list(path=path, hash=hash, width=width, height=height)) 
#}
#parallel::stopCluster(clust)

saveRDS(final, 'animal.rds')
j <- jsonlite::toJSON(final)
write(j, 'animal.json')
