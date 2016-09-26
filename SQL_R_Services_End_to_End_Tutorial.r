
## SQL Server R Services Self-Paced Tutorial

#Welcome to the the SQL R-Services self-paced tutorial. In this tutorial, you will use a jupyter notebook in order examine a few of the in-database analytics capabilities of SQL Server 2016.

#This tutorial is similar to the walk through found on MSDN [here](https://msdn.microsoft.com/en-us/library/mt612857.aspx). The primary differences are that this tutorial are two-fold. First, this tutorial includes some additional exercises not available in the on-line walk-through. Second, this tutorial assumes that step 1 (["Prepare the Data"](https://msdn.microsoft.com/en-us/library/mt612858.aspx)) has already been completed. This step can be completed by simply running this tutorial on the windows version of the data science virtual machine (DSVM), or by running the relevant steps on a different SQL Server 2016 machine.


### Data, Audience, and Goals

#### Data

#As stated on [MSDN](https://msdn.microsoft.com/en-us/library/mt612857.aspx), this tutorial is based on a well-known 
#public data set, the New York City taxi dataset. 
#You will use a combination of R code, SQL Server data, and custom SQL functions to build a 
#classification model that indicates the probability that the driver will get a tip on a 
#particular taxi trip. You'll also deploy your R model to SQL Server and use server data to 
#generate scores based on the model.

#This example is chosen because it can easily be extended to all kinds of real-life problems, 
#such as predicting customer responses to sales campaigns, or predicting spending by visitors to events.
#Because the model can be invoked from a stored procedure, you can also easily embed it in an application.

#### Intended Audience

#This walkthrough is intended for R developers. You should also be somewhat familiar with 
#fundamental database operations, such as creating databases, creating tables, importing data 
#into tables, and querying the tables using Transact-SQL. 

#If you are new to R but know databases well, this tutorial provides an introduction into how R 
#can be integrated into enterprise workflows using R Services (In-database).

#The first few cells check for required R packages, and install them if necessary.


## Start by installing relevant R packages.
# These are the packages that possibly need to be installed:
# importantly - ggmap requires the jpeg library to be installed!
required_packages <- c('ggmap','mapproj','ROCR','RODBC')
# check which ones need to be installed:
packages_to_install <- required_packages[!(required_packages %in% rownames(installed.packages()))]
# install in a specific directory where we know we have write access:
libdir <- "./libsforjupyter"
# create the lib directory if it does not already exist.
if(!file.exists(libdir)) dir.create(libdir)
# install the relevant packages.
lapply(packages_to_install, install.packages, lib=libdir)
# Add local lib directory to libPaths - important on Windows due to permissions of the Jupyter user
# The Jupyter windows user (LOCAL SERVICE) does not have write permission to global lib
.libPaths( "./libsforjupyter")

# In order to make this more effective in a multi-user environment, we will create 
# a stem that will be useful for identifying which database objects you create in this
# tutorial. We will simply concatenate the user with the nodename:
username <- gsub("-","_",paste(Sys.info()['user'],Sys.info()['nodename'], sep = "_"))
username

### Specifying the Database Connection

#Now that we have set up a few logistics, we can jump right in. In order to connect to a SQL Server 2016 instance,
#we need to start with a connection to the database. We need to start by creating an RODBC-style connection string:



## local connection string - this will work if you are on the windows data science virtual machine!
# connStr <- "Driver=SQL Server;Server=localhost;Database=nyctaxi;Trusted_Connection=true"

## Remote connection string:
driverStr <- "Driver=SQL Server"
serverStr <- "Server=dss-sql-vm0.southcentralus.cloudapp.azure.com"
dbStr <- "Database=NYCTaxiData"
uidStr <- "Uid=sqluser"
pwdStr <- "Pwd=SQLServer2016#"
connStr <- paste(driverStr,serverStr,dbStr,uidStr,pwdStr, sep = ";")


### Specifying the Compute Context

#Once we have specified the database connection, we also need to define a "compute context." A compute context simply refers to the computational back-end. For this particular example, the two most relevant compute contexts are:

#- `local`: Commands get run locally on the client. Because the data for this tutorial are sitting in SQL Server, which is potentially a different machine, this means that the data must be pulled out of SQL Server to the client, and hten processed on the client machine.
#- `InSqlServer`: Commands will be run on the SQL Server machine, which means that the data can stay!

#For this set of examples, we will work with the `InSqlServer` compute context, which can be defined in the next few steps.


# Define a temp directory path to serialize R objects back and forth (on the client)
if(Sys.info()['sysname'] == 'Windows'){
    sqlShareDir <- paste("C:\\AllShare\\",Sys.info()['user'],sep="")
}else{
    sqlShareDir <- tempdir()
}
# wait for the job to complete before providing a prompt? Yes!
sqlWait <- TRUE
# Print stdout from the SQL Server console? No!
sqlConsoleOutput <- FALSE
# Actually *Define* the compute context
cc <- RxInSqlServer(connectionString = connStr, shareDir = sqlShareDir, 
                    wait = sqlWait, consoleOutput = sqlConsoleOutput)
# Set it so that it is the engine used.
rxSetComputeContext(cc)


### Setting up R environment to query the database

#Once we have set up the appropriate connection string, we can now start processing the data. Let's start by
#creating a simple query and executing that query against the database. The resulting dataset can then be used as
#the data source for future data processing and modeling steps.

#As we move forward through the tutorial, we will use `inDataSource` as the input data source for a number of
#data processing steps. As we do so, keep in mind that `inDataSource` is just a reference to the result dataset from the SQL query.


sampleDataQuery <- "select tipped, tip_amount, fare_amount, passenger_count,trip_time_in_secs,trip_distance, 
pickup_datetime, dropoff_datetime, 
cast(pickup_longitude as float) as pickup_longitude, 
cast(pickup_latitude as float) as pickup_latitude, 
cast(dropoff_longitude as float) as dropoff_longitude, 
cast(dropoff_latitude as float)  as dropoff_latitude,
cast(pickup_longitude as float) * 100 as pickup_longitude2, 
cast(pickup_latitude as float) * 100 as pickup_latitude2, 
cast(dropoff_longitude as float) * 100 as dropoff_longitude2, 
cast(dropoff_latitude as float) * 100 as dropoff_latitude2,
payment_type from nyctaxi_sample
tablesample (5 percent) repeatable (98052)
"

ptypeColInfo <- list(
  payment_type = list(
    type = "factor",
    levels = c("CSH", "CRD", "DIS", "NOC", "UNK")
#      ,
#    newLevels= c("CSH", "CRD", "DIS", "NOC", "UNK")
  )
)

inDataSource <- RxSqlServerData(sqlQuery = sampleDataQuery, 
                                connectionString = connStr, 
                                colInfo = ptypeColInfo,
                                colClasses = c(pickup_longitude = "numeric", pickup_latitude = "numeric", 
                                               dropoff_longitude = "numeric", dropoff_latitude = "numeric"),
                                rowsPerRead=500)
inDataSource

### Understand the data

#These functions give you basic information about your data source, such as its size and the names of columns in the set:

#- `rxGetInfo()`: Provides information about the data source
#- `rxGetVarInfo()`: Provides information about the *variables* (can also be accessed by setting the `getVarinfo` argument in  `rxGetInfo` to `TRUE`)
#- `rxGetVarNames()`: Provides a vector of variable names.

  
####################
## Exercise 1.
####################

# What information does rxGetInfo() provide for `inDataSource`?
# How many variables are there, and how did you get that information?

  
### Numerical Data Summaries

#Once we know we can start inspecting the data, we may want to start creating univariate descriptive summaries of the datasets. The `rxSummary()` function provides output on the number of observations contained in a data set and for a particular column, the function provides output on the following by default:

 #- Name
 #- Mean value
 #- Standard Deviation
 #- Minimum and Maximum value
 #- Number of valid observations
 #- Number of missing observations

#The `rxSummary()` function takes a formula as its first argument, and the name of the data set as the second. See ?rxSummary for more details.

#In addition, column subsets and transformations may also be computed as a sub-call to the function using the transforms (and so forth) commands.

#If compute context is SQL server, it is executed closer to the data. 

################################
#        Data exploration      #
################################
# Summarize a fare amount independent of other variables:
rxSummary( ~ fare_amount, data = inDataSource)

# With rxSummary(), you can also examine one variable as a function of others, by including 
# interactions in the formula - this will summarize fare_amount at each different number of passengers
# between 1 and 6.
rxSummary(~fare_amount:F(passenger_count,1,6), data = inDataSource)

################################
## Exercise 2
################################

# Summarize *tip_amount* as a function of passenger_count.
# Summarize *tip_amount* as a function of payment_type.
# Summarize *tipped* as a function of passenger_count as well.
# Summarize *tipped* as a function of payment_type as well.
# Do you notice anything interesting?

################################
#       Data Visualization     #
################################

# In addition to getting summaries grouped by other variables, we can also 
# use rxCube() to compute grouped means as well.
cube1<-rxCube(tip_amount~F(passenger_count,1,7):F(trip_distance,0, 25), 
        data = inDataSource)
# rxResultsDF() can be used to create a data.frame() from a variety of objects
# including the results of a call to rxCube():
cubePlot <- rxResultsDF(cube1)
head(cubePlot)

# Once we have a tabular format, we can actually use the levelplot()
# to create a heatmap that relates one value to couple of others.
# In this plot, color maps on to tip amount, while the x-axis in the 
# graph corresponds to trip distance, and the y-axis corresponds to
# passenger count.
levelplot(tip_amount ~ trip_distance * passenger_count, data = cubePlot)

# We can get more sophisticated, as we can start making the x- and y-
# axes actually reference longitude and latitude, respectively.
# We can start by doing this for the pickup location:
# More Cubeplots
cube2<-rxCube(tip_amount~F(pickup_longitude2,-7410, -7385, exclude=TRUE):F(pickup_latitude2,4068,4084, exclude = TRUE), data = inDataSource)
cubePlot2 <- rxResultsDF(cube2)
colnames(cubePlot2) <- c("pickup_longitude", "pickup_latitude", "tip_amount", "Counts")
cubePlot2 <- transform(cubePlot2, pickup_longitude = as.numeric(pickup_longitude)/100.0, pickup_latitude = as.numeric(pickup_latitude)/100.0)
levelplot(tip_amount~pickup_longitude*pickup_latitude, cubePlot2)

# We can create the same graph for hte dropoff location.
cube3<-rxCube(tip_amount~F(dropoff_longitude2,-7410, -7385, exclude=TRUE):F(dropoff_latitude2,4068,4084, exclude = TRUE), data = inDataSource)
cubePlot3 <- rxResultsDF(cube3)
colnames(cubePlot3) <- c("dropoff_longitude", "dropoff_latitude", "tip_amount", "Counts")
cubePlot3 <- transform(cubePlot3, dropoff_longitude = as.numeric(dropoff_longitude)/100.0, dropoff_latitude = as.numeric(dropoff_latitude)/100.0)
levelplot(tip_amount~dropoff_longitude*dropoff_latitude, cubePlot3)

# It is important to keep in mind that in this context, the data are actually being computed in 
# the database. We are only getting the results back. Where this may become a little less obvious
# is an example where the computations that are necessary for a couple of visualizations plotting
#  is being executed on by SQL Server:
# Let's examine the histogram of tip amounts as a function of the different payment types
rxHistogram( ~ tip_amount | payment_type, data = inDataSource, title = "Tip Amount Histogram")
rxHistogram(~fare_amount, data = inDataSource, title = "Fare Amount Histogram")

# Let's do something slightly more sophisticated
# In this example, we will set up with a slightly different query on the NYC taxi data.
# However, what we will do is use rxExec() to execute a command remotely inside the SQL
# database.

# Start with the query that will form our table:
sampleDataQuery2 <- "select top 1000 tipped, tip_amount, fare_amount, passenger_count,trip_time_in_secs,trip_distance, 
    pickup_datetime, dropoff_datetime, 
pickup_longitude, 
pickup_latitude, 
dropoff_longitude, 
dropoff_latitude,
payment_type from nyctaxi_sample"

# Create the data source:
inDataSource2 <- RxSqlServerData(sqlQuery = sampleDataQuery2, connectionString = connStr, colInfo = ptypeColInfo,
                                 colClasses = c(pickup_longitude = "numeric", pickup_latitude = "numeric", 
                                                dropoff_longitude = "numeric", dropoff_latitude = "numeric"),
                                 rowsPerRead=500)


################################
#      Feature engineering     #
################################
# Define a function in open source R to calculate the direct distance between pickup and dropoff as a new feature 
# Use Haversine Formula: https://en.wikipedia.org/wiki/Haversine_formula
env <- new.env()

env$ComputeDist <- function(pickup_long, pickup_lat, dropoff_long, dropoff_lat){
  R <- 6371/1.609344 #radius in mile
  delta_lat <- dropoff_lat - pickup_lat
  delta_long <- dropoff_long - pickup_long
  degrees_to_radians = pi/180.0
  a1 <- sin(delta_lat/2*degrees_to_radians)
  a2 <- as.numeric(a1)^2
  a3 <- cos(pickup_lat*degrees_to_radians)
  a4 <- cos(dropoff_lat*degrees_to_radians)
  a5 <- sin(delta_long/2*degrees_to_radians)
  a6 <- as.numeric(a5)^2
  a <- a2+a3*a4*a6
  c <- 2*atan2(sqrt(a),sqrt(1-a))
  d <- R*c
  return (d)
}

featuretable = paste0(username , "_features")
#Define the featureDataSource to be used to store the features, specify types of some variables as numeric
featureDataSource = RxSqlServerData(table = featuretable, 
                                    colClasses = c(pickup_longitude = "numeric", pickup_latitude = "numeric", 
                                                   dropoff_longitude = "numeric", dropoff_latitude = "numeric",
                                                   passenger_count  = "numeric", trip_distance  = "numeric",
                                                   trip_time_in_secs  = "numeric", direct_distance  = "numeric"),
                                    connectionString = connStr)

# Create feature (direct distance) by calling rxDataStep() function, which calls the env$ComputeDist function to process records
# And output it along with other variables as features to the featureDataSource
# This will be the feature set for training machine learning models
start.time <- proc.time()
rxDataStep(inData =   inDataSource, outFile = featureDataSource,  overwrite = TRUE, 
           varsToKeep=c("tipped", "fare_amount", "passenger_count","trip_time_in_secs", 
                        "trip_distance", "pickup_datetime", "dropoff_datetime", "pickup_longitude",
                        "pickup_latitude","dropoff_longitude", "dropoff_latitude"),
           transforms = list(direct_distance=ComputeDist(pickup_longitude, pickup_latitude, dropoff_longitude, 
                                                         dropoff_latitude)),
           transformEnvir = env, rowsPerRead=10000, reportProgress = 3)
used.time <- proc.time() - start.time
print(paste("It takes CPU Time=", round(used.time[1]+used.time[2],2), 
            " seconds, Elapsed Time=", round(used.time[3],2), " seconds to generate features.", sep=""))

###############################
## Examine your new variable
###############################

## Exercise 3

# Can you example the new variable and whether it seems related to the tip amount?

# Do feature engineering through a SQL Query

ptypeColInfo <- list(
  payment_type = list(
    type = "factor",
    levels = c("CSH", "CRD", "DIS", "NOC", "UNK"),
    newLevels= c("CSH", "CRD", "DIS", "NOC", "UNK")
  )
)
# Alternatively, use a user defined function in SQL to create features
# Sometimes, feature engineering in SQL might be faster than R
# You need to choose the most efficient way based on real situation
# Here, featureEngineeringQuery is just a reference to the result from a SQL query. 
featureEngineeringQuery = "SELECT tipped, fare_amount, passenger_count,trip_time_in_secs,trip_distance, 
pickup_datetime, dropoff_datetime, 
dbo.fnCalculateDistance(pickup_latitude, pickup_longitude,  dropoff_latitude, dropoff_longitude) as direct_distance,
pickup_latitude, pickup_longitude,  dropoff_latitude, dropoff_longitude, payment_type
FROM nyctaxi_sample
tablesample (70 percent) repeatable (98052) where payment_type = 'CRD'
"
featureDataSource = RxSqlServerData(sqlQuery = featureEngineeringQuery, colInfo = ptypeColInfo,
                                    colClasses = c(pickup_longitude = "numeric", pickup_latitude = "numeric", 
                                                   dropoff_longitude = "numeric", dropoff_latitude = "numeric",
                                                   passenger_count  = "numeric", trip_distance  = "numeric",
                                                   trip_time_in_secs  = "numeric", direct_distance  = "numeric", fare_amount="numeric"),
                                    connectionString = connStr)

# summarize the feature table after the feature set is created
rxGetVarInfo(data = featureDataSource)
################################
## Summarize the new variable
################################

## Exercise 4.

# How do the descriptive statistics of the SQL-produced variable compare 
# to the descriptive statistics for the R-produced?

################################
#        Training models       #
################################

## Let's go ahead and build a model that predicts whether a tip is provided
## based on additive effects of the number of passengers, trip distance,
## trip time, and the direct distance:

# build classification model to predict tipped or not
system.time(logitObj <- rxLogit(tipped ~  passenger_count + trip_distance + trip_time_in_secs + direct_distance, data = featureDataSource))
summary(logitObj)

################################
#        Make predictions      #
################################

## We can use R to generate predictions quite easily.

## Set up the output table in SQL Server
outputtable = paste0(username, "_scoreoutput")
scoredOutput <- RxSqlServerData(
  connectionString = connStr,
  table = outputtable
)

# predict and write the prediction results back to SQL Server table
rxPredict(modelObject = logitObj, data = featureDataSource, outData = scoredOutput, 
          predVarNames = "Score", type = "response", writeModelVars = TRUE, overwrite = TRUE)


################################
#   Model operationalization   #
################################

## In addition to prediction from R, we can also operationalize with SQL.
## However this takes a few steps - the first thing we must do is serialize
## the R object and write it int the database by running a stored procedure:

# First, serialize a model and put it into a database table
modelbin <- serialize(logitObj, NULL)
modelbinstr=paste(modelbin, collapse="")

## connect to the DB
library(RODBC)
conn <- odbcDriverConnect(connStr )

# Persist model by calling a stored procedure from SQL
q<-paste("EXEC PersistModel @m='", modelbinstr,"'", sep="")
sqlQuery (conn, q)

## Once we have persisted it in the database,
## we can also generate predictions from it (i.e. "consume it" using SQL.
## There are two different stored Procs
# There are two stored Procs - One for predicting on single observation and another for predicting a batch of observations

# Single Observation prediction
q = "EXEC PredictTipSingleMode @passenger_count=1, @trip_distance=2.5, @trip_time_in_secs=631, 
     @pickup_latitude=40.763958,@pickup_longitude=-73.973373, @dropoff_latitude=40.782139,@dropoff_longitude=-73.977303"
sqlQuery (conn, q)

q = "EXEC PredictTipSingleMode @passenger_count=2, @trip_distance=35.3, @trip_time_in_secs=2939, 
@pickup_latitude=40.75984,@pickup_longitude=-73.9754, @dropoff_latitude=41.0496,@dropoff_longitude=-73.54097"
sqlQuery (conn, q)

# Batch mode prediction. Score all data that were not part of the training dataset and select top 10 to preedict
input = "N'select top 10 a.passenger_count as passenger_count, 
	a.trip_time_in_secs as trip_time_in_secs,
	a.trip_distance as trip_distance,
	a.dropoff_datetime as dropoff_datetime,  
	dbo.fnCalculateDistance(pickup_latitude, pickup_longitude, dropoff_latitude,dropoff_longitude) as direct_distance , fare_amount, payment_type
from
(
	select medallion, hack_license, pickup_datetime, passenger_count,trip_time_in_secs,trip_distance,  
		dropoff_datetime, pickup_latitude, pickup_longitude, dropoff_latitude, dropoff_longitude, fare_amount, payment_type
	from nyctaxi_sample where payment_type = ''CRD''
)a
left outer join
(
select medallion, hack_license, pickup_datetime
from nyctaxi_sample
tablesample (70 percent) repeatable (98052) where payment_type = ''CRD''
)b
on a.medallion=b.medallion and a.hack_license=b.hack_license and a.pickup_datetime=b.pickup_datetime
where b.medallion is null
'"
q<-paste("EXEC PredictTipBatchMode @inquery = ", input, sep="")
sqlQuery (conn, q)

