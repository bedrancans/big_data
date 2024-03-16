SELECT 
    br.name, 
    SUM(bsi.capacity) AS ToplamKapasite
FROM 
     bigquery-public-data.san_francisco_bikeshare.bikeshare_regions AS br
LEFT JOIN 
    bigquery-public-data.san_francisco_bikeshare.bikeshare_station_info AS bsi ON br.region_id = bsi.region_id
GROUP BY 
    br.name,bsi.region_id
HAVING 
    SUM(bsi.capacity) < 5000;
