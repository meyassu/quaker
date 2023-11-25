# Quaker

## Table of Contents
- [Overview](#overview)
- [The Data](#the-data)
- [Repository Contents](#repository-contents)
- [Instructions](#instructions)

## Overview
Welcome to Quaker, a repository built to visualize the spatiotemporal distribution of severe earthquakes and their effects on regional macroeconomic variables such as real GDP (rGDP) using geospatial libraries and the Qlik Sense platform. Quaker is based on an extensive dataset on seismic activity provided by the National Earthquake Information Center (NEIC) and made available on [Kaggle](https://www.kaggle.com/datasets/usgs/earthquake-database) and a large time-series economic dataset on rGDP trends collected from the archives of the [Federal Reserve Bank of St. Louis](https://fred.stlouisfed.org/) (FRED). 

This repository is made up of two independent modules: Revgeocoder and Econbot. 

Revgeocoder is an efficient, general-purpose reverse geocoder built from scratch. Creating a visualization of the effects of severe seismic activity on local rGDP trends requires a uniform system for representing geographical data. The issue is earthquake datasets represent geographical location with (latitude, longitude) coordinates while economic datasets represent it in geopolitical terms i.e. (country, province) tuples. Revgeocoder bridges this gap by translating coordinate data into human-readable geolocation information. It can perform this computation on any dataset regardless of its structure as long as it contains columns called 'latitude' and 'longitude'. Its internal algorithm, supporting libraries, and instructions to run it will be described further in (LINK TO REVGEOCODER SECTION).

Econbot is the web scraper responsible for creating the rGDP time-series dataset. It works by navigating to the FRED website, downloading region-specific rGDP data for > 100 countries as CSV files, and then collating all the CSVs into a single coherent dataset. More details regarding Econbot, including instructions to run it locally can be found in (LINK TO ECONBOT SECTION).

Both of these back-end modules will be discussed after the datasets and the visualizations are presented.


## The Data

### NEIC Earthquake Dataset
The NEIC earthquake dataset contains information on over 23k severe earthquakes from 1965 to 2016 with magnitudes exceeding 5.5 on the Richter scale. Each earthquake record is made up many dimensions but the visualizations only make use of the following fields:

| Date | Time | Latitude | Longitude | Magnitude |
| ---- | ---- | -------- | --------- | --------- |


### FRED rGDP Dataset
The FRED rGDP dataset consists of the annual rGDP values at constant national price for over 100 countries from around 1950 to 2019. The precise dates are differnt for each country but the data invariably spans a substantial portion of the 20th/21st centuries. Each record is made up of the following dimensions:

| Country | rGDP | Year | 
| ---- | ---- | ------- |


Both datasets were run through a validation process to ensure that they do not contain missing values, duplicate records, empty records, or out of range values. 




The software in this repository works towards a visualization of severe earthquake data (> 5.5 magnitude Richter scale) vis-a-vis trends in regional macroeneomic variables to better understand the economic impact of large earthquakes. The data is provided by the National Earthquake Information Center, an arm of the US Geological Survey, and made made available on Kaggle (https://www.kaggle.com/datasets/usgs/earthquake-database). This dataset only includes the latitude/longitude coordinates of the epicenters of the various earthquakes and does not annotate the earthquake records with any geopolitical details (this information is needed to do economic analysis since economic data is indexed according to geopolitical boundaries and not geographical coordinates). It is necessary, for this reason, to reverse-geocode the dataset. Most of the code in this repository is concerned with this task.

The visualization of the earthquake data can be viewed by importing quaker.qvf into the Qlik Sense cloud platform. 

## Repository Contents

- data/ contains geopgraphical data describing the boundaries of countries and subregions (sourced from NaturalEarth: https://www.naturalearthdata.com/) and the earthquake data

- src/
  - quaker.py: the main file, currently has functions that initialize the PosGreSQL databases on both Neon and AWS RDS platforms
  - utils.py: various utility functions to establish database connections, manipulate the PosGreSQL database, and load data into RAM
  - preprocess.py: various functions for computing subregion boundaries from country boundaries, building minimum bounding rectangles (MBRs) for geographic regions, spatially indexing geographic data to optimize query performance by encoding the data into an R-tree, performing Point-in-Polygon computations (the final step for reverse geocoding)
  - validate.py: data validation functions
  - exceptions.py: various custom exceptions
 
- .dockerignore: the Docker ignore file

## Instructions
This repository is a work-in-progress so the code cannot be run at the moment. All of the functions execute without any issues but a few meta-structural implementations are needed to bring all the pieces together. This code will be run on an AWS EC2 t2.micro instance connected to an RDS db.t2.micro instance.
