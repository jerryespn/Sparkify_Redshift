# Udacity - Data Engineer Nanodegree - Sparkify - Redshift Data Warehouse

## About this project

Sparkify dataset is a database that contains music and artist records created for educational purposes.
The data extracted for this project is hosted at AWS S3 Bucket and you could find the S3 location at "dwh.cfg" file.

For this project two datasets were used: songs and log data. 

- Song json files, were used to get the information about songs (of course) and artist.
- Log json files, were used to get the information about covers song, artist and the complementary information for each song. 

## Prerequisites to use this repository

1. AWS Account (You will need your own Key and Secret)
1. Python 3.x
1. Jupyter Notebook (Recommended, to install with Anaconda)
1. Windows/Mac/Linux - Compatible
2. Firefox, Chrome, Edge Navigators - Compatible

## Deployment instructions

You will need to create your AWS Account if you don't have it yet. Then please checkout how could you get your KEY and SECRET (**Be Careful!!** this values should never be revealed to other person)

Now that you are ready, please fill the "dwh.cfg" file with your KEY and SECRET.

Feel free to execute the notebook **nb_etl_testing.ipynb** it is intended to help you create the cluster and IAM roles needed before starting with the data processing.

1. Once you have your cluster and IAM role ready, execute as follows:
	1. `$ python ./create_tables.py` - This script is intended to create database and tables
	2. `python ./etl.py` - This scripts is intended to load data from dataset into Resdhift Database