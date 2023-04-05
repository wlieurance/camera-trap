-- DROP
ALTER TABLE photo DROP CONSTRAINT photo_md5hash_fkey;
ALTER TABLE photo DROP CONSTRAINT photo_dt_import_fkey;
ALTER TABLE photo DROP CONSTRAINT camera_site_fk;
ALTER TABLE tag DROP CONSTRAINT tag_md5hash_fkey;
ALTER TABLE camera DROP CONSTRAINT camera_site_name_fkey;
ALTER TABLE "sequence" DROP CONSTRAINT sequence_site_name_camera_id_fkey;
ALTER TABLE sequence_gen DROP CONSTRAINT sequence_gen_seq_id_fkey;
ALTER TABLE sequence_gen DROP CONSTRAINT sequence_gen_gen_id_fkey;
ALTER TABLE animal DROP CONSTRAINT animal_seq_id_fkey;
ALTER TABLE animal DROP CONSTRAINT animal_md5hash_fkey;
ALTER TABLE animal_loc DROP CONSTRAINT animal_loc_md5hash_id_fkey;
ALTER TABLE "condition" DROP CONSTRAINT condition_md5hash_fkey;
ALTER TABLE "condition" DROP CONSTRAINT condition_seq_id_scorer_name_fkey;
ALTER TABLE condition_seqs DROP CONSTRAINT condition_seqs_seq_id_fkey;


-- ADD
ALTER TABLE photo ADD CONSTRAINT photo_md5hash_fkey FOREIGN KEY (md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE photo ADD CONSTRAINT photo_dt_import_fkey FOREIGN KEY (dt_import) REFERENCES import(import_date) ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE photo ADD CONSTRAINT camera_site_fk FOREIGN KEY (site_name, camera_id) REFERENCES camera(site_name, camera_id) ON DELETE SET NULL ON UPDATE CASCADE;
ALTER TABLE tag ADD CONSTRAINT tag_md5hash_fkey FOREIGN KEY (md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE camera ADD CONSTRAINT camera_site_name_fkey FOREIGN KEY(site_name) REFERENCES site(site_name) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE sequence ADD CONSTRAINT sequence_site_name_camera_id_fkey FOREIGN KEY(site_name, camera_id) REFERENCES camera(site_name, camera_id) ON DELETE CASCADE ON UPDATE CASCADE;
ALTER TABLE sequence_gen ADD CONSTRAINT sequence_gen_seq_id_fkey FOREIGN KEY(seq_id) REFERENCES sequence(seq_id) ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE sequence_gen ADD CONSTRAINT sequence_gen_gen_id_fkey FOREIGN KEY(gen_id) REFERENCES generation(gen_id) ON DELETE CASCADE;
ALTER TABLE animal ADD CONSTRAINT animal_seq_id_fkey FOREIGN KEY(seq_id) REFERENCES sequence(seq_id) ON DELETE SET NULL ON UPDATE CASCADE;
ALTER TABLE animal ADD CONSTRAINT animal_md5hash_fkey FOREIGN KEY(md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE;
ALTER TABLE animal_loc ADD CONSTRAINT animal_loc_md5hash_id_fkey FOREIGN KEY (md5hash, id) REFERENCES animal(md5hash, id) ON UPDATE CASCADE ON DELETE CASCADE;
ALTER TABLE condition ADD CONSTRAINT condition_md5hash_fkey FOREIGN KEY(md5hash) REFERENCES hash(md5hash) ON DELETE CASCADE;
ALTER TABLE condition ADD CONSTRAINT condition_seq_id_scorer_name_fkey FOREIGN KEY(seq_id, scorer_name) REFERENCES condition_seqs(seq_id, scorer_name) ON DELETE RESTRICT ON UPDATE CASCADE;
ALTER TABLE condition_seqs ADD CONSTRAINT condition_seqs_seq_id_fkey FOREIGN KEY(seq_id) REFERENCES sequence(seq_id) ON DELETE RESTRICT ON UPDATE CASCADE;

