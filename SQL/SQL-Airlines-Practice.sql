-- ## airlines questions

-- Select all airlines from the table.(1)
-- Select airline names and their countries.(2)
-- Select airlines where the country is 'India'.(3)
-- Select airlines with fleet size greater than 200.(4)
-- Select airlines founded after 1950.(5)
-- Select airlines that belong to 'Star Alliance'.(6)
-- Select airline names where destinations are more than 150.(7)
-- Select airlines where the hub airport is 'Delhi'.(8)
-- Count the total number of airlines.(9)
-- Count airlines grouped by country.(10)
-- Find the number of airlines in each alliance.(11)
-- Find average fleet size grouped by country.(12)
-- Find the maximum fleet size among airlines.(13)
-- Find the minimum number of destinations grouped by alliance.(14)
-- Select countries having more than one airline (GROUP BY + HAVING).(15)
-- Select alliances with more than 2 airlines (GROUP BY + HAVING).(16)
-- Find countries where average fleet size is greater than 200 (GROUP BY + HAVING).(16)
-- Find alliances where total destinations exceed 300 (GROUP BY + HAVING).(17)
-- Count airlines founded before 2000 grouped by country.(18)
-- Select alliances having average destinations greater than 150 (GROUP BY + HAVING).(19)





-- ## airlines answers




-- -- -- select * from air_indias;         (1)

-- -- select airline_name,country from air_indias;      (2)

-- select * from air_indias where country = 'India';      (3)

-- select * from air_indias where fleet_size >200;    (4)

-- select * from air_indias where founded_year >1950;      (5)

-- select * from air_indias where alliance ='star_Alliance';       (6)
-- select * from air_indias where alliance ='Star_Alliance';         (6)

-- select * from air_indias where destination >150;        (7)

-- select * from air_indias where hub_airport='Delhi';        (8)

-- select count(*) from air_indias;                 (9)

-- select country, count(*) from air_indias                (10)
-- group by country;

-- select alliance,count(*) from air_indias                (11)
-- group by alliance;

-- select country,avg(fleet_size) as avg_fleet                (12)
-- from air_indias 
-- group by country;

-- select airline_name, max(fleet_size) from air_indias           (13)
-- group by airline_name;

-- select alliance,min(destination) from AIR_INDIAS               (14)
-- group by alliance;

-- Select country,count(*) from AIR_INDIAS                         (15)
-- group by country having count(*)>1;

-- select alliance,count(*) from AIR_INDIAS
-- group by alliance having count(*)>2;                              (16)



-- select country,avg(fleet_size) as avg_fleet
-- from air_indias
-- group by country                                             (17)
-- having avg_fleet>200;

-- select country,avg(fleet_size) as avg_fleet
-- from air_indias                                             
-- group by country
-- having avg(fleet_size)>200;



-- select alliance ,sum(destination) as total_destiny
-- from air_indias
-- group by alliance                                                 (18)
-- having total_destiny > 300;



-- select country,count(*) from air_indias
-- where founded_year<2000                                              (19)
-- group by country;


-- select alliance,avg(destination)as avg_destiny
-- from air_indias
-- group by alliance
-- having avg_destiny > 150;