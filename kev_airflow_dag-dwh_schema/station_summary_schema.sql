CREATE TABLE IF NOT EXISTS `dimStationSummaryAirflow` 
(
    `id` INT NOT NULL AUTO_INCREMENT,
    `station_id` INT NOT NULL,
    `flow_99` FLOAT DEFAULT NULL,
    `flow_max` FLOAT DEFAULT NULL,
    `flow_median` FLOAT DEFAULT NULL,
    `flow_total` FLOAT DEFAULT NULL,
    `n_obs` FLOAT DEFAULT NULL,
    PRIMARY KEY (`id`)
)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;
