'''
General Description
Weather Test Generator tool implemented with PySpark
Input: 
GeoTiff File argument -i (required to get latitude and longitude information)
Latlong information file argument -l containing information about latitude and longutide with mapping in the following json format:
{<Continent Code>:{<Longitude>:[<Latitude>,<IATA code>]...}}
Output Location -o Where the output needs to be dumped. 
Continent Name -c Continent corresponding to the image.

'''


from osgeo import gdal
import requests
from pyspark.context import SparkContext
#import sys, getopt
from datetime import datetime
import random
import time
import json
from argparse import ArgumentParser

global sc


# Random date time generator
def randomDate():
    year = random.randint(2000, 2018)
    month = random.randint(1, 12)
    day = random.randint(1, 28)
    hour = random.randint(0,23)
    min = random.randint(0,59)
    sec = random.randint(0,59)
    randomDateTime = datetime(year, month, day,hour,min,sec)
    formatedTime = '{:%Y-%m-%dT%H:%M:%SZ}'.format(randomDateTime)
    return formatedTime

#To provide Random Iata Codes
def randomIata():
    iataCodes = ['SYD','PER','ADL','BNE','MEL','OOL','HBA','NTL','GIS','DUD','CNS','CHC']
    return random.choice(iataCodes)


#To Provide Random Weather Conditions
def randomWeather():
    weather_conditions = {"Sunny": {"temperature": (40, 10), "pressure": (1200, 700), "humidity": (70, 55)},
                      "Rain": {"temperature": (25, 15), "pressure": (1200, 700), "humidity": (70, 55)},
                      "Snow": {"temperature": (-1, -7), "pressure": (1200, 700), "humidity": (70, 55)}}
    weather = random.choice(weather_conditions.keys())
    condition = weather_conditions[weather]
    (tMax, tMin) = condition["temperature"]
    (pMax, pMin) = condition["pressure"]
    (hMax, hMin) = condition["humidity"]
    outWeather = "{}|{}|{}|{}".format(weather,str(random.randint(tMin, tMax)),str(round(random.uniform(pMin,pMax), 1)),str(random.randint(hMin, hMax)))
    return outWeather

def transformData(xval,gtb,contDatab):
   try:
       #print xval
       gtBrod = gtb.value
       longitude = gtBrod[1] * xval[0] + gtBrod[2] * xval[1] + gtBrod[0]
       latitude = gtBrod[4] * xval[0] + gtBrod[5] * xval[1] + gtBrod[3]
       iata = str(randomIata()) #can be set to None if we want real prediction
      
       #Below code is written to cover near by locations to the airport, with an assumption of +-2 points.  
       for elem in contDatab.value.keys():
           if (float(elem) -2) <= long <= (float(elem)+2):
		latSource = float(contDatab.value[elem][0])
		if (float(latSource) -2) <= lat <= (float(latSource)+2):
		   iata = contDatab.value[elem][1] 		
       
       output = None       
       if iata : #if iata is not selected randomly.
           output = "{}|{},{}|{}|{}".format(iata,str(latitude),str(longitude),str(randomDate()),str(randomWeather()))
       return output
   except Exception,e:
       print 'Exception while transforming the data: ',str(e)

#Service Method Heart of the code, all the flow starts from here.

def generateData(inpArgs):
   inputfile=inpArgs.inputfile
   latlongfile=inpArgs.latlongfile
   outputloc=inpArgs.outputloc
   cont=inpArgs.cont
   ds = gdal.Open(inputfile)
   width = ds.RasterXSize #Xpixels
   height = ds.RasterYSize #Ypixels
   gt = ds.GetGeoTransform() # To Fetch Raster's georeference info
   gtb = sc.broadcast(gt)
   listCoordinates = []
   #converting 2D matrix to 1D Array of Pixel locations to parallelize with spark context, can be done using any python library like numpy.
   for row in range(0,width):
      for col in range(0,height):
         listCoordinates.append((row,col))
   
   #Below code is fetching latlong data on the basis of continent name passed.  
   contData = json.loads(open(latlongfile).read())[cont] 
   contDatab = sc.broadcast(contData) #Broadcasted the variable to be used in the spark executors.
   
   print 'Starting Job at : ',str(datetime.now()) 
   '''
    Below statement will parallelize the 1D Array which we generated and apply the transformations , finally after the transformations are completed, 
    it will filter the empty lines
   ''' 
   rdd = sc.parallelize(listCoordinates).map(lambda xval: transformData(xval,gtb,contDatab)).filter(lambda x: x != None)
   
   #Saves the generated output at the specified location.
   rdd.saveAsTextFile(outputloc)

#This is the arguments parser it raises alert incase of any arguments issue.

def argsParser():
  parser = ArgumentParser(usage='spark-submit %(prog)s [options]')
  try:
      parser.add_argument("-i", "--inputfile", help="Path of input GeoTiff file",action="store",  dest="inputfile", required=True) 
      parser.add_argument("-l", "--latlongfile", help="Path of latlong file",action="store",  dest="latlongfile", required=True)
      parser.add_argument("-o", "--outputloc", help="Output Location",action="store",  dest="outputloc", required=True)
      parser.add_argument("-c", "--cont", help="Continent Name",action="store",  dest="cont", required=True)  
      inpArgs = parser.parse_args()
  except Exception,e:
      parser.print_help()
  
  print 'GEO file is "', inpArgs.inputfile
  print 'Latlong file with IATA codes is : ',inpArgs.latlongfile
  print 'Output file location is: ',inpArgs.outputloc
  print 'Continent Name is: ',inpArgs.cont 
   
  return inpArgs

#Main method, this is the triggering point.
if __name__ == '__main__':
   sc = SparkContext(appName='Generate Weather Data').getOrCreate()
   inpArgs = argsParser()
   generateData(inpArgs)
