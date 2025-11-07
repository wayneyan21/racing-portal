-- Minimal schema + demo data
CREATE DATABASE IF NOT EXISTS racing_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
USE racing_db;

CREATE TABLE IF NOT EXISTS jockeys (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  name_zh VARCHAR(128), country VARCHAR(64),
  starts INT DEFAULT 0, wins INT DEFAULT 0, place_pct DECIMAL(5,2) DEFAULT 0.00
);

CREATE TABLE IF NOT EXISTS trainers (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  name_zh VARCHAR(128), country VARCHAR(64),
  stable VARCHAR(128) NULL
);

CREATE TABLE IF NOT EXISTS horses (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  reg_no VARCHAR(32) UNIQUE, name_zh VARCHAR(128),
  sex VARCHAR(8), foaling_year SMALLINT NULL,
  trainer VARCHAR(128), current_rating INT NULL
);

CREATE TABLE IF NOT EXISTS venues (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  code VARCHAR(8) UNIQUE, name_zh VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS races (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  race_day DATE, venue_code VARCHAR(8), race_no TINYINT, distance_m INT, going VARCHAR(32),
  UNIQUE KEY uk_race (race_day, venue_code, race_no)
);

CREATE TABLE IF NOT EXISTS race_runners (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  race_id BIGINT, saddle_no INT, horse_name_zh VARCHAR(128),
  jockey_zh VARCHAR(128), weight_lbs INT, draw INT, sp DECIMAL(6,2) NULL,
  FOREIGN KEY (race_id) REFERENCES races(id)
);

INSERT INTO venues(code,name_zh) VALUES ('ST','沙田') ON DUPLICATE KEY UPDATE name_zh=VALUES(name_zh);
INSERT INTO races(race_day,venue_code,race_no,distance_m,going)
VALUES (CURDATE(),'ST',1,1200,'GOOD')
ON DUPLICATE KEY UPDATE distance_m=VALUES(distance_m), going=VALUES(going);

INSERT INTO race_runners(race_id,saddle_no,horse_name_zh,jockey_zh,weight_lbs,draw,sp)
SELECT r.id, 1, '飛躍天際','莫雷拉',126,3,4.5 FROM races r WHERE r.race_no=1 AND r.venue_code='ST' AND r.race_day=CURDATE()
UNION ALL
SELECT r.id, 2, '極速旋風','潘頓',123,8,3.2 FROM races r WHERE r.race_no=1 AND r.venue_code='ST' AND r.race_day=CURDATE();
