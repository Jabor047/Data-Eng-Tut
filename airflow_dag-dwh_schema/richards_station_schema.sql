DROP TABLE dimRichardStation IF EXISTS;

CREATE TABLE IF NOT EXISTS dimRichardStation 
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
    `totalflow` FLOAT DEFAULT NULL,
    `weekday` INT DEFAULT NULL,
    `hour` INT DEFAULT NULL,
    `minute` INT DEFAULT NULL,
    `second` INT DEFAULT NULL,
    PRIMARY KEY (`id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;
