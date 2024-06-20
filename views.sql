USE FDB
GO

CREATE VIEW DriverWinsView AS
SELECT
    YEAR(race.r_date) as Season,
    driver.d_id AS DriverID,
    driver.d_firstname AS FirstName,
    driver.d_surname AS LastName,
    constructor.con_id AS ConstructorID,
    constructor.con_name AS ConstructorName,
    COUNT(result.res_position) as Wins
FROM result
         JOIN race ON result.res_fk_r_id = race.r_id
         JOIN driver ON result.res_fk_d_id = driver.d_id
         JOIN constructor ON result.res_fk_con_id = constructor.con_id
WHERE result.res_position = 1
GROUP BY YEAR(race.r_date), driver.d_id, driver.d_firstname, driver.d_surname, constructor.con_id, constructor.con_name;
GO

SELECT * FROM DriverWinsView ORDER BY Season, Wins DESC;
GO

CREATE VIEW ConstructorWinsView AS
SELECT
    YEAR(race.r_date) as Season,
    constructor.con_id AS ConstructorID,
    constructor.con_name AS ConstructorName,
    COUNT(result.res_position) as Wins
FROM result
         JOIN race ON result.res_fk_r_id = race.r_id
         JOIN constructor ON result.res_fk_con_id = constructor.con_id
WHERE result.res_position = 1
GROUP BY YEAR(race.r_date), constructor.con_id, constructor.con_name;
GO

SELECT * FROM ConstructorWinsView ORDER BY Season, Wins DESC;
GO

CREATE VIEW PodiumFinishesView AS
SELECT
    race.r_id AS RaceID,
    YEAR(race.r_date) AS Season,
    driver.d_id AS DriverID,
    driver.d_firstname AS FirstName,
    driver.d_surname AS LastName,
    constructor.con_id AS ConstructorID,
    constructor.con_name AS ConstructorName,
    result.res_position AS Position
FROM result
JOIN race ON result.res_fk_r_id = race.r_id
JOIN driver ON result.res_fk_d_id = driver.d_id
JOIN constructor ON result.res_fk_con_id = constructor.con_id
WHERE result.res_position IN (1, 2, 3)
GO


SELECT * FROM PodiumFinishesView ORDER BY Season, RaceID, Position;