The project creates an ETL pipeline using AWS Glue, AWS S3, Python, and Sparkto build a lakehouse solution in AWS that satisfies these requirements from the STEDI data scientists.


**Project Requirements**
To create S3 directories for customer_landing, step_trainer_landing, and accelerometer_landing zones, and copy the data there as a starting point to simulate the data coming from the various sources. 

1. create three Glue tables for the three landing zones - customer_landing.sql, accelerometer_landing.sql, and step_trainer_landing.sql
2. screenshot of each table showing the resulting data using Athena
3. create 2 AWS Glue Jobs that do the following:
    a. Sanitize the Customer data from the Website (Landing Zone) and only store the Customer Records who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called customer_trusted.
    b. Sanitize the Accelerometer data from the Mobile App (Landing Zone) - and only store Accelerometer Readings from customers who agreed to share their data for research purposes (Trusted Zone) - creating a Glue Table called accelerometer_trusted.

Data Scientists have discovered a data quality issue with the Customer Data. The serial number should be a unique identifier for the STEDI Step Trainer they purchased. However, there was a defect in the fulfillment website, and it used the same 30 serial numbers over and over again for millions of customers! 
Most customers have not received their Step Trainers yet, but those who have, are submitting Step Trainer data over the IoT network (Landing Zone). The data from the Step Trainer Records has the correct serial numbers.

The problem is that because of this serial number bug in the fulfillment data (Landing Zone), we don’t know which customer the Step Trainer Records data belongs to.

The Data Science team asked to write a Glue job that sanitizes the Customer data (Trusted Zone) and create a Glue Table (Curated Zone) that only includes customers who have accelerometer data and have agreed to share their data for research called customers_curated. Hence -
4. creates two Glue Studio jobs that do the following tasks:
    a. Read the Step Trainer IoT data stream (S3) and populate a Trusted Zone Glue Table called step_trainer_trusted that contains the Step Trainer Records data for customers who have accelerometer data and have agreed to share their data for research (customers_curated).
    b. Create an aggregated table that has each of the Step Trainer Readings, and the associated accelerometer reading data for the same timestamp, but only for customers who have agreed to share their data, and make a glue table called machine_learning_curated.
