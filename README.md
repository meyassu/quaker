# Quaker

## Table of Contents
- [Overview](#overview)
- [The Data](#the-data)
- [Modules](#modules)
- [Repository Contents](#repository-contents)
- [Instructions](#instructions)

## Overview
Welcome to Quaker, a repository built to visualize the spatiotemporal distribution of severe earthquakes and their effects on regional macroeconomic variables such as real GDP (rGDP) using geospatial libraries and the Qlik Sense platform. Quaker is based on an extensive dataset on seismic activity provided by the National Earthquake Information Center (NEIC) and made available on [Kaggle](https://www.kaggle.com/datasets/usgs/earthquake-database) and a large time-series economic dataset on rGDP trends collected from the archives of the [Federal Reserve Bank of St. Louis](https://fred.stlouisfed.org/) (FRED). 

This repository is made up of two independent modules: Revgeocoder and Econbot. 

Revgeocoder is an efficient, general-purpose reverse geocoder built from scratch. To create visualizations of the effects of severe seismic activity on local rGDP trends, there needs to be a uniform system for representing geographical data across both the earthquake and economic datasets. The issue is earthquake datasets represent geographical location with (latitude, longitude) coordinates while economic datasets represent location in geopolitical terms i.e. (country, province) tuples. Revgeocoder bridges this gap by translating coordinate data into human-readable geolocation information. It can perform this computation on any dataset regardless of its structure as long as it contains columns called 'latitude' and 'longitude'. Its internal algorithm, supporting libraries, and instructions to run it will be described further in [Revgeocoder](#revgeocoder).

Econbot is the web scraper responsible for creating the rGDP time-series dataset. It works by navigating to the FRED website, downloading region-specific rGDP data for > 100 countries as CSV files, and then collating all the CSVs into a single coherent dataset. More information regarding Econbot, including instructions to run it locally can be found in [Econbot](#econbot).

The modules will be discussed in more detail after the datasets and the visualizations are presented.

## The Data

### NEIC Earthquake Dataset
The NEIC earthquake dataset contains information on over 23k severe earthquakes from 1965 to 2016 with magnitudes exceeding 5.5 on the Richter scale. Each earthquake record is made up many dimensions but the visualizations only make use of the following fields:

| Date | Time | Latitude | Longitude | Magnitude |
| ---- | ---- | -------- | --------- | --------- |


### FRED rGDP Dataset
The FRED rGDP dataset consists of the annual rGDP values at constant national price for over 100 countries from around 1950 to 2019. The precise dates are different for each country but the data invariably spans a substantial portion of the 20th/21st centuries. Each record is made up of the following dimensions:
 
| Country | rGDP | Year | 
| ------- | ---- | ---- |

Both datasets were run through a validation process to ensure that they do not contain missing values, duplicate records, empty records, or out of range values. 

## Visualizations

### Spatiotemporal Distribution of Earthquakes
The spatial distribution of earthquakes is concentrated most around the Ring of Fire, a seismically active area encircling the Pacific Ocean, and Oceania. The data reflects a weak positive variance between earthquake frequency and time. These visualizations are interactive; users can change the year being displayed and the lower bound on magnitude. Below are screenshots from the Qlik Sense Platform. 

To access the visualizations and the associated stories directly on your local machine, see the [Instructions](#instructions) section.

![spatiotemporal-visualization-1987](https://github.com/meyassu/quaker/raw/main/documentation/img/earthquake_st_distribution.png?raw=true)
Global spatial distribution of earthquakes with magnitude > 6 on Richter scale in 1987.<br><br>

![spatiotemporal-visualization-2011](https://github.com/meyassu/quaker/raw/main/documentation/img/earthquake_st_distribution_2011.png?raw=true)
Global spatial distribution of earthquakes with magnitude > 5 on Richter scale in 2011 (note the cluster of activity around Japan).<br><br>

![spatiotemporal-visualization-japan-2011](https://github.com/meyassu/quaker/raw/main/documentation/img/earthquake_japan_2011.png?raw=true)
Closer look at Japan earthquakes in 2011.<br><br>


### Economic Effects of Earthquakes 
The countries included in these datasets showed a surprising economic resilience to severe earthquakes in that annual rGDP generally remained constant or rose even through seismically intense periods. The most striking example of this is the Japanese economy in the early 2010s.  These visualizations are interactive; users can change the year and country being displayed. Below are screenshots from the Qlik Sense Platform. 

To access the visualizations and stories directly on your local machine, see the [Instructions](#instructions) section.

![spatiotemporal-visualization-1987](https://github.com/meyassu/quaker/raw/main/documentation/img/earthquakes_rgdp.png?raw=true)
Time-series rGDP data parallel with time-series earthquake frequency data.<br><br>

![spatiotemporal-visualization-2011](https://github.com/meyassu/quaker/raw/main/documentation/img/earthquakes_rgdp_japan.png?raw=true)
Japanese rGDP resilient to 2011 catastrophes.<br><br>

## Modules

### Revgeocoder
Revgeocoder is a general-purpose reverse geocoder. It takes in as input data any raw CSV file with columns named ```longitude``` and ```latitude``` and returns as output the exact same CSV file with the additional columns ```province``` and ```country```. 

#### Reverse Geocoding Algorithm
Revgeocoder depends on an R-tree populated with an extensive boundary dataset for spatial indexing (more information on this can be found in [Boundary Dataset](#boundary-dataset) and [Spatial Indexing with R-tree](#spatial-indexing-with-r-tree)). The core algorithm, which is implemented in ```revgeocoder/src/core/rgc.py```, consists of doing the following for every coordinate point in the input: query the R-tree to quickly narrow down the set of candidate regions and perform a Point-in-Polygon (PiP) operation on each candidate region until a match is found. If any point is matched with a maritime boundary, Revgeocoder checks to see if there are any coastlines within 370.4 km (the UN specified buffer distance within which nations are authorized to exploit the ocean for economic resources), and if there are, it matches the point with that country. 

It is possible to carry out this process for large inputs with batch processing while using a raw CSV file on SSD as a rudimentary database but this is inferior to using a true relational database engine. Revgeocoder uses a PostGreSQL database on the backend to efficiently perform batch processing on the data. At this time, the central database environment is under development, so the user must provide connection details and credentials to their own PostGreSQL instance in a configuration file. This is perfectly safe since the software is designed to be run locally. The batch size can also be specified in the configuration file by the user. More information on the input configuration file and other execution steps can be found in [Instructions](#instructions). Revgeocoder interfaces with the database via the functions in ```revgeocoder/src/utils/database.py```.

#### Boundary Dataset
The boundary data is sourced from a community-run site known as [NaturalEarth](https://www.naturalearthdata.com/downloads/). All of the land boundaries are at the provicial level (e.g. states, administrative regions) and the maritime boundaries include seas, oceans, and some lakes although most small bodies of water are excluded from the dataset.

NaturalEarth ships all boundary data as a Shapefile directory with several files, each encoding a specific component of the geographical data. It also delivers maritime and land boundary data in separate Shapefile directories. Revgeocoder has a function in ```revgeocoder/src/utils/geodata.py``` called ```_shapefile_to_geojson()``` which can compress any Shapefile directory into a single GeoJSON file. ```_shapefile_to_geojson()``` was used to combine the maritime and land boundary data into a single GeoJSON file called boundaries.geojson located at ```revgeocoder/data/internal/```. This file is the authoritative source of truth for boundary data in this module.

#### Spatial Indexing with R-tree
The naive approach to reverse geocoding a geographical coordinate pair in the context of this project would be to run a PiP operation for the point on every boundary available in boundaries.geojson. Although simple, this algorithm is inefficient with an upper bound complexity of something like ```O(n * m * p)``` where ```n``` is the number of coordinate pairs in the input, ```m``` is the number of boundaries in the dataset, and ```p``` is the average number of vertices in the boundary polygons.

The simplest optimization to this brute-force algorithm would be to implement basic quadrant filtering, which entails computing the geographical quadrant of the coordinate point, doing the same for every boundary in the dataset, eliminating all the boundaries outside of that quadrant, and running the PiP operation on the remaining candidate regions. To accelerate the categorization of boundaries into quadrants, it would be a good idea to temporarily replace the complex boundary polygons with minimum bounding rectangles (MBRs) just for this step since the margin of error introduced with this loss of precision is negligible in this context. This approach is better but ideally, there would be some way to replace quadrant filtering with a more fine-grained, and comparably fast, filtering algorithm. Happily, there is such an algorithm called R-tree-based filtering, which, while data-hungry, fits this criteria.

R-trees can be thought of as binary search trees (BSTs) for geographical data. Both data structures achieve roughly ```O(log n)``` query complexity by hierarchically splitting the search space at different scales and exploiting this to rapidly narrow down the set of possible targets. R-trees are typically populated with boundary MBRs. Revgeocoder does this with ```build_rtree()``` at ```revgeocoder/src/utils/qindex.py```. It then uses the R-tree to quickly narrow down the set of possible regions for every query point in the input data and runs PiP on the remaning candidate regions until it finds a match. Revgeocoder computes and stores the MBRs with ```compute_mbrs()``` in ```revgeocoder/src/utils/geodata.py```. 

Below is a graphical representation of an R-tree that makes its underlying concepts simple to grasp.

![rtree-graphical-representation](https://github.com/meyassu/quaker/raw/main/documentation/img/rtree.png?raw=true)
R-tree graphical representation.<br>


#### Infrastructure
Revgeocoder is a containerized application with a Docker image at [meyassu/revgeocoder:latest](https://hub.docker.com/repository/docker/meyassu/revgeocoder/general) on Docker Hub so it can be executed on any machine running Docker. It also has a simple CI/CD pipeline which automatically builds the Docker image from the Dockerfile and pushes it up to Docker Hub whenever there is a ```git push``` event that impacts the ```revgeocoder/``` directory. The pipeline definition can be found at ```.github/workflows/main.yml```.

#### Instructions
As mentioned in [Infrastructure][#infrastructure], Revgeocoder is a containerized application. To run it, complete the following steps:
1) Install Docker
2) Download run-revgeocoder.sh
3) Set up input user directory (refer to examples/revgeocoder/data) <br>
    a) create an empty directory called ```data``` <br>
    b) data/ must consist of the following subdirectories: config, input, and output <br>
        i) config: this directory must contain a .env file with the following fields: <br>
            &emsp;- ```DATA_TABLE_NAME```: the table that will store the input data in input/ <br>
            &emsp;- ```LOCATION_TABLE_NAME```: the table that will store the (country, province) tuples outputted by Revgeocoder <br>
            - ```RDS```: must be either ```TRUE``` or ```FALSE``` and indicates whether the user database is hosted on an RDS instance <br>
            - ```REGION```: must be included if ```RDS=TRUE```; the region of the connected AWS compute instance <br>
            - ```DB_CERT_FPATH```: must be included if ```RDS=TRUE```; get .pem file from examples/revgeocoder/data and put it in config, set this to ```user_data/config/rds-ca-2019-root.pem``` <br>
            - ```DB_USER```: the database username <br>
            - ```DB_HOST```: the database hostname <br>
            - ```DB_POST```: the database port number (usually 5432 for PostGreSQL databases) <br>
            - ```DB_NAME```: the database name <br>
            - ```BATCH_SIZE```: the batch size <br>
        ii) input/: must contain a single CSV file called ```data.csv``` <br>
        iii) output/: must be empty, the output CSV will be stored here <br>
    c) Type ```chmod +x run-revgeocoder.sh``` to enable execute bit on bash script <br>
    d) run run-revgeocoder.sh and pass it the absolute filpath to data directory: ```./run-revgeocoder.sh  <ABSOLUTE_FILEPATH_DATA_DIR>```

To run Revgeocoder with the example data in examples/revgeocoder/data, do the following:
1) Install Docker
2) Download run-revgeocoder.sh
3) Download examples/revgeocoder/data
4) Type ```chmod +x run-revgeocoder.sh``` to enable execute bit on bash script
5) Run ```./run-revgeocoder.sh <ABSOLUTE_FILEPATH_DATA_DIR>```
Revgeocoder will use a designated backend database on a staging environment to run this process.


    



~~~

~~~

### Econbot
Econbot is a Selenium-based web scraper that compiles a single coherent time-series rGDP dataset from many isolated files on the [FRED](https://fred.stlouisfed.org/) research site. It works by taking in a set of countries from a CSV file, querying the site search engine for rGDP data for each country, ranking the search results according to some internal criteria, navigating to the best page, downloading the CSV files on the page, and then, once its finished going through all the different countries, combining all the files into a single CSV. 

At the moment, Econbot can only get data for a specific set of countries but in the future, it will be a fully generalized program. Currently, its mostly an inert container for the precollected data at econbot/data/rgdp.csv. It can push this precollected data to a database if the user specifies the connection details and credentials in the .env file (more information on configuration and execution in [Instructions](#instructions)). Its web-scraping capabilities can be called if post-2023 data is needed.

Program logs can be found in econbot/logs/logs.txt after program execution.

Note on user-specified databases: both Revgeocoder and Econbot can work with any PostGreSQL database and even support interaction with RDS-hosted databases as long as the program is executed from an AWS Cloud compute instance with IAM authentication privileges. This functionality was built with the boto3 AWS SDK.

## Repository Contents
- .github/workflows/main.yml: CI/CD pipeline definition
- documentation/img/: images for README.md
- .gitignore: the project .gitignore
- Quaker.qvf: the importable Qlik Sense file with all the visualizations
- README.md: this file for documentation
- run-revgeocoder.sh: the bash script for running Revgeocoder (more details can be found at [Instructions](#instructions))
- examples/revgeocoder/data: example input directory for Revgeocoder 
    - config/: configuration files
    - input/: input data file
    - output/: output directory

### Revgeocoder
- revgeocoder
    - logs/: stores program logs written during runtime
    - user_data/: mounting point for user data within container (more details can be found at [Instructions](#instructions))
    - .dockerignore: the .dockerignore for Revgeocoder <br>
    - Dockerfile: the Dockerfile for Revgeocoder <br>
    - environment.yml: serialization of environment that Docker uses to build the various dependencies within the Revgeocoder container
    - data/internal/
        - boundaries.geojson: boundary data for every province and large body of water on the planet
        - mbrs.geojson: the minimum bounding rectangles (MBRs) for each boundary in boundary.geojson; used for spatial indexing with R-tree
    - src/
        - __init__.py: runs basic configuration processes for module
        - main.py: driver program for Revgeocoder
        - core/
            - __init__.py: empty file used to mark core/ as a standalone module
            - qindex.py: pulls from data/interna/mbrs.geojson to build the R-tree for spatial indexing
            - rgc.py: core reverse geocoding process
        - utils/
            - __init__.py: empty file used to mark utils/ as a standalone module
            - database.py: host of functions for interacting with user-specified PostGreSQL database
            - exceptions.py: set of custom exception classes to improve error specificity
            - geodata.py: few functions for dealing with geospatial data
            - validate.py: validates earthquake dataset

### Econbot
- econbot
    - logs/: stores program logs written during runtime
    - environment.yml: serialization of environment / dependencies
    - data/internal/
        -- rgdp.csv: rGDP data for limited set of countries
    - src/:
        - __init__.py: empty file used to mark core/ as a standalone module
        - main.py: driver program for Econbot
        - core/
            - __init__.py: empty file used to mark core/ as a standalone module
            - econbot.py: web scraping functions
        - utils/
            - __init__.py: empty file used to mark core/ as a standalone module
            - database.py: host of functions for interacting with user-specified PostGreSQL database
            - exceptions.py: set of custom exception classes to improve error specificity
            - validate.py: validates rGDP dataset created via web scraping

## Instructions

### Revgeocoder
As mentioned in the [Introduction](#introduction), Revgeocoder is a general-purpose reverse geocoder that works on any CSV with the    


### Econbot

## Future Work

### Revgeocoder

### Econbot
    - train random-forest model
    - single backend database capable of handling several concurrent connections / managing tables etc.