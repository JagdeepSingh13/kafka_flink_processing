CREATE DATABASE smartgrid;

\c smartgrid;

CREATE TABLE halfhourlyAverages (
    window_start TIMESTAMP PRIMARY KEY,
    avg_wind DOUBLE PRECISION,
    avg_solar DOUBLE PRECISION
);

-- INSERT INTO halfhourlyAverages (window_start, avg_wind, avg_solar) VALUES
--     ('2025-12-21 05:30:00', 91.7, 102.4);