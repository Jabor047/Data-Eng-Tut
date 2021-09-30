-- create dimStattionSummary Table
CREATE TABLE IF NOT EXISTS dimStationSummary 
(
    id SERIAL PRIMARY KEY, 
    station_id INT NOT NULL,
    flow_99 FLOAT DEFAULT NULL,
    flow_max FLOAT DEFAULT NULL,
    flow_median FLOAT DEFAULT NULL,
    flow_total FLOAT DEFAULT NULL,
    n_obs FLOAT DEFAULT NULL
);

-- create dimRichardStation Table
CREATE TABLE IF NOT EXISTS dimRichardStation 
(
    id SERIAL PRIMARY KEY,
    timestamp_id TIMESTAMP DEFAULT NULL,
    flow1 FLOAT DEFAULT NULL,
    occupancy1 FLOAT DEFAULT NULL,
    flow2 FLOAT DEFAULT NULL,
    occupancy2 FLOAT DEFAULT NULL,
    flow3 FLOAT DEFAULT NULL,
    occupancy3 FLOAT DEFAULT NULL,
    totalflow FLOAT DEFAULT NULL,
    weekday INT DEFAULT NULL,
    hour INT DEFAULT NULL,
    minute INT DEFAULT NULL,
    second INT DEFAULT NULL
);

-- create dimStation Table
CREATE TABLE IF NOT EXISTS dimStation
(
    id SERIAL PRIMARY KEY,
    station_id INT NOT NULL,
    fwy INT DEFAULT NULL,
    dir TEXT DEFAULT NULL,
    district INT DEFAULT NULL,
    country INT DEFAULT NULL,
    city TEXT DEFAULT NULL,
    statePm FLOAT DEFAULT NULL,
    absPm FLOAT DEFAULT NULL,
    latitude FLOAT DEFAULT NULL,
    longitude FLOAT DEFAULT NULL,
    length FLOAT DEFAULT NULL,
    type TEXT DEFAULT NULL,
    lanes INT DEFAULT NULL,
    name TEXT DEFAULT NULL,
    userId1 TEXT DEFAULT NULL,
    userId2 TEXT DEFAULT NULL,
    userId3 INT DEFAULT NULL, 
    userId4 INT DEFAULT NULL
);

-- create dimAllStations Table
CREATE TABLE IF NOT EXISTS dimAllStations
(
    id SERIAL PRIMARY KEY,
    timestamp_id TIMESTAMP DEFAULT NULL,
    station_id INT NOT NULL,
    flow1 FLOAT DEFAULT NULL,
    occupancy1 FLOAT DEFAULT NULL,
    flow2 FLOAT DEFAULT NULL,
    occupancy2 FLOAT DEFAULT NULL,
    flow3 FLOAT DEFAULT NULL,
    occupancy3 FLOAT DEFAULT NULL,
    flow4 FLOAT DEFAULT NULL,
    occupancy4 FLOAT DEFAULT NULL,
    flow5 FLOAT DEFAULT NULL,
    occupancy5 FLOAT DEFAULT NULL,
    flow6 FLOAT DEFAULT NULL,
    occupancy6 FLOAT DEFAULT NULL,
    flow7 FLOAT DEFAULT NULL,
    occupancy7 FLOAT DEFAULT NULL,
    flow8 FLOAT DEFAULT NULL,
    occupancy8 FLOAT DEFAULT NULL,
    flow9 FLOAT DEFAULT NULL,
    occupancy9 FLOAT DEFAULT NULL,
    flow10 FLOAT DEFAULT NULL,
    occupancy10 FLOAT DEFAULT NULL,
    flow11 FLOAT DEFAULT NULL,
    occupancy11 FLOAT DEFAULT NULL,
    flow12 FLOAT DEFAULT NULL,
    occupancy12 FLOAT DEFAULT NULL
);