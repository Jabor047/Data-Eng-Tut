DROP TABLE IF EXISTS dimAllStations;

CREATE TABLE IF NOT EXISTS `dimAllStations` 
(
    `id` INT NOT NULL AUTO_INCREMENT,
    `timestamp_id` DATETIME DEFAULT NULL,
    `station_id` INT NOT NULL,
    `flow1` FLOAT DEFAULT NULL,
    `occupancy1` FLOAT DEFAULT NULL,
    `flow2` FLOAT DEFAULT NULL,
    `occupancy2` FLOAT DEFAULT NULL,
    `flow3` FLOAT DEFAULT NULL,
    `occupancy3` FLOAT DEFAULT NULL,
    `flow4` FLOAT DEFAULT NULL,
    `occupancy4` FLOAT DEFAULT NULL,
    `flow5` FLOAT DEFAULT NULL,
    `occupancy5` FLOAT DEFAULT NULL,
    `flow6` FLOAT DEFAULT NULL,
    `occupancy6` FLOAT DEFAULT NULL,
    `flow7` FLOAT DEFAULT NULL,
    `occupancy7` FLOAT DEFAULT NULL,
    `flow8` FLOAT DEFAULT NULL,
    `occupancy8` FLOAT DEFAULT NULL,
    `flow9` FLOAT DEFAULT NULL,
    `occupancy9` FLOAT DEFAULT NULL,
    `flow10` FLOAT DEFAULT NULL,
    `occupancy10` FLOAT DEFAULT NULL,
    `flow11` FLOAT DEFAULT NULL,
    `occupancy11` FLOAT DEFAULT NULL,
    `flow12` FLOAT DEFAULT NULL,
    `occupancy12` FLOAT DEFAULT NULL,
    PRIMARY KEY (`id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;
