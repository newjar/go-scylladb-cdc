CREATE KEYSPACE IF NOT EXISTS vehicle_monitoring
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

USE vehicle_monitoring;

CREATE TABLE IF NOT EXISTS battery (
    id text,
    entry_time timestamp,
    voltage float,
    current float,
    capacity float,
    power int,
    temperature float,
    soc int,
    internal_resistance float,
    PRIMARY KEY (id)
) WITH CDC = {'enabled': true};
