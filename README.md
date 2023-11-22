# quaker

Outline
- 



## Table of Contents
- [Overview](#overview)
- [Repository Contents](#repository-contents)
- [Instructions](#instructions)

## Overview
Welcome to Quaker, a repository built to visualize the spatiotemporal distribution of severe earthquakes and their effects on regional macroeconomic variables such as real GDP (rGDP) using geospatial libraries and the Qlik Sense platform. Quaker is based on a single extensive dataset provided by the National Earthquake Information Center (NEIC) and made available on [Kaggle](https://www.kaggle.com/datasets/usgs/earthquake-database).

### The Data
The NEIC earthquake dataset contains information on over 23k severe earthquakes from 1965 to 2016. Each earthquake record is made up of the following dimensions:

 Date | Time | Latitude | Longitude | Type | Depth | Depth Error | Depth Seismic Stations | Magnitude | Magnitude Type | Magnitude Error | 
  ---   ---      ---         ---      ---     ---       ---                ---                 ---           ---               ---






Magnitude Seismic Stations | Azimuthal Gap | Horizontal Distance
Horizontal Error
Root Mean Square
ID Source
Location Source
Magnitude Source
Status









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
