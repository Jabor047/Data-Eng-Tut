DROP TABLE `dimStation` IF EXISTS;

CREATE TABLE IF NOT EXISTS `dimStation` 
(
    `id` INT NOT NULL AUTO_INCREMENT,
    `station_id` INT NOT NULL,
    `fwy` INT DEFAULT NULL,
    `dir` TEXT DEFAULT NULL,
    `district` INT DEFAULT NULL,
    `country` INT DEFAULT NULL,
    `city` TEXT DEFAULT NULL,
    `statePm` FLOAT DEFAULT NULL,
    `absPm` FLOAT DEFAULT NULL,
    `latitude` FLOAT DEFAULT NULL,
    `longitude` FLOAT DEFAULT NULL,
    `length` FLOAT DEFAULT NULL,
    `type` TEXT DEFAULT NULL,
    `lanes` INT DEFAULT NULL,
    `name` TEXT DEFAULT NULL,
    `userId1` TEXT DEFAULT NULL,
    `userId2` TEXT DEFAULT NULL,
    `userId3` INT DEFAULT NULL, 
    `userId4` INT DEFAULT NULL,
    PRIMARY KEY (`id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;
