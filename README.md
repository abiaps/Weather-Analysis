# Weather-Analysis

  This is a Hadoop MapReduce project to compute average temperature, windspeed and dew point of Texas state

  Input consists of station_id, yearmonthday, temperature, windspeed, dewpoint and so on

  Two MapReduce jobs are needed

  Every hour is monitored in the input file, therefore the average is computed for each day of each station of each year and each month

  Similarly the average can be computed for month, year based on the needs

 # Compile the code using the following cmd

  javac -classpath ${HADOOP_CLASSPATH} -d '/home/abiaps/Desktop/weatherclass' '/home/abiaps/Desktop/WeatherAnalysis.java' 
  
  where weatherclass is the folder created for the compiled classes
  
 # Create a jar file using the class folder

  jar -cvf weather.jar -C weatherclass/ . 
  
  where weather.jar is the jar file to be created
  
 # Run the java application using the jar file created

  hadoop jar '/home/abiaps/Desktop/weather.jar' WeatherAnalysis /weather/input/hourly_2006.g /weather/temp /weather/output
  
  where WeatherAnalysis is my java file, path of input file in hdfs, output of job1 and output of job2 path


  

  


