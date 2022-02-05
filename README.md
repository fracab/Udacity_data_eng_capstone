# Udacity Data Engineering Capstone

# Introduction 
This repository contains the scripts and a notebook for the final project of Udacity Data Engineering Nanodegree. The project consists in a complete ETL process.

The aim of the present project is to gather information about commercial flight delays in the United States and consolidate them with methereolgical data (precipitations) at the departure and destination airport, with the final objective of building a dataset for analytical purposes. The resulting dataset can be used for research purposes or to feed a dashboard or other types of data visualizations - enabling end users to investigate e.g. correlations between precipitations and delays or flight cancellations, controlling them by date, day of the week, carrier, geographical position of the airports, etc.

Data about commercial flight delays in the U.S. are available through the Bureau of Transport Statistics website (https://www.transtats.bts.gov), however there is no API available to programmatically access the dataset. For the purpose of the present exercise, a simplified version of the data (available on https://www.kaggle.com/yuanyuwendymu/airline-delay-and-cancellation-data-2009-2018) were stored as .csv on a publicly accessible S3 bucket. Each year include approx. 5 millions observations.

Daily precipitations values can be accessed in JSON format through the API of the Applied Climate Information System (ACIS, http://www.rcc-acis.org/docs_webservices.html#title44). The MultiStnData web service gives access to a single measurement for a day for each station in the dataset. As detailed below, the dataset is queried only for the stations which are also airports and are represented in the delays dataset. As the number of airports in the dataset on a given year is about 300, each year will include approx. 3,300,000 observations.

# Datasets

## Delays dataset
The delays dataset gives a granular overview of the timing and delays faced of a commercial flight, with information at flight level.The dataset taken into consideration doesn't contain duplicate entries nor completely missing rows - however in order to prevent issues and since duplicates would clearly be erroneous, we will filter duplicates and entries that are missing date or origin or destination, since missing these values would make the entry in any case not usable for our purposes. A better understanding of the dataset and the manipulation steps required can be obtained from the jupter notebook.

## ACIS precipitations dataset
Contrarily to the delays dataset, the JSON export from the Acis web service has several nested fields which require significant wrangling in order to be stored in tabular format. A detailed walkthrough is in the jupyter notebook.


# Data Model

The objective of the process is the creation of an analytical dataset where information about commercial flight delays can be compared with precipitation information at the departure and destination airport.

As the precipitation dataset contains information at daily level and details about individual flights data are not required, the information will be grouped at route per day level - therefore each row will be univoquely identified as a origin-destination-date combination. Two additional dimensions will be included, one for the airports in the dataset (containing full name, latitude and longitude and elevation) and a second one with the different  routes in the dataset, identified by flight number and day of the week, and including information about the carrier, origin and destination and first and last flight date in the dataset. We finally include a date dimension. A complete data dictionary is in the DataDictionary.txt file.

The tables are created from the datasets described above and loaded to S3 as a staging area, before being loaded to redshift.

# General considerations

## Tooling
The ETL process use S3 as staging storage, Redshift as final database storage and is written in pySpark to leverage on Spark capacity to process  large-scale data. 

Given the size of the datasets involved, distributed computing is highly recomended, as the whole operation would be too computationally expensive for a single device (although it can run, albeit slowly, for a limited subset of the data).

Although the use of S3 as a staging step might look redundant, it eliminates the need of an external connector between Spark and Redshift, and given the low costs of S3, such redundancy might result even desireable should different transformations be required or a data backup be needed. 

Redshift appears to be the best-suited warehouse product given the scale of the data set, although a on-prem traditional database engine could also be used. However, Redshift appears a better choice in terms of scalability, in particular if the number of people needing access to the dataset is supposed to grow. 

## Additional scenarios

The ETL process is designed in the current form only to be run saltuarily, although it can be potentially be adapted to run on a regular basis. Should it be run e.g. on a daily basis, a function can be used to automatically update the start date for data capture. As historical data are not supposed to change, the default option "overwrite" when staging to S3 could also be de-activated. In such a scenario, the ETL could also be adapted to be incorporated into an Airflow DAG. 

As mentioned above, redshift (and S3) offer good scalability options should data increase in size, although it might be necessary to increase the EMR/EC cluster size to ensure adequate performance, due to the geomatrical increas of the number of joins. Dividing the ETL in batches by date would also be adviseable.

The scalability considerations apply also in terms of access needed by different users at the same time. In that case, an adequately powerful instance of redshift would be necessary. Additional considerations pertain to the location of the users (as opposed to that of the redshift instance). 
