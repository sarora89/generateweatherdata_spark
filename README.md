generate_weatherdata.py is Pyspark code which generates the test weather dataset in the following format:

    SYD|-33.86,151.21,39|2015-12-23T05:02:12Z|Rain|+12.5|1004.3|97
    MEL|-37.83,144.98,7|2015-12-24T15:30:55Z|Snow|-5.3|998.4|55
    ADL|-34.92,138.62,48|2016-01-03T12:35:37Z|Sunny|+39.4|1114.1|12

It requires spark-2.x version to be installed along with the following python packages with python 2.7.x +:

osgeo
requests
 
Assumptions:

1) Generated IATA code with latitude longitude data for 15 Airports around every continent.
2) To cover circumference of the IATA, code takes +- 2 (Lat,Long).
3) For the locations where the IATA information is not present, a random IATA code is assigned. 
4) GeoTiff images are divided and passed contining 1 or 2 continents as projected on https://visibleearth.nasa.gov/grid.php
5) Image used for testing is Oceana continent with name : gebco_08_rev_elev_D2_grey_geo.tif
6) Job is running on a Spark Cluster or Local mode where Spark libraries are present. 

Below is the command to trigger the job:

spark-submit generate_weatherdata.py -i Input GeoTiff File -l LatitudeLongitude Mapping File -o Output Location -c Continent(OC,EU...)
# generateweatherdata_spark
