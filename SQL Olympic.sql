select Country, COUNT(*) As TotalAthletes
from tb_athletes
GROUP BY Country
ORDER BY TotalAthletes DESC;

--Country gold,silver,bronze

select 
TeamCountry,
SUM(Gold) Total_gold,
SUM(Bronze) Total_bronze,
SUM(Silver)  Total_silver
from medals
Group BY TeamCountry
ORDER BY Total_gold DESC;

