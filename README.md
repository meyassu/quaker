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

Econbot is the web scraper responsible for creating the rGDP time-series dataset. It works by navigating to the FRED website, downloading region-specific rGDP data for > 100 countries as CSV files, and then collating all the CSVs into a single coherent dataset. More details regarding Econbot, including instructions to run it locally can be found in [Econbot](#econbot).

Both of these backend modules will be discussed after the datasets and the visualizations are presented.

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
Revgeocoder is a general-purpose reverse geocoder. It works by indexing an exhaustive dataset containing the geometry and location of every geographical boundary in the world, both maritime and land, with an R-tree, narrowing down the set of possible regions with the R-tree for every query point, and then performing a Point-in-Polygon (PiP) operation on the filtered set of regions. Each of these components of Revgeocoder will be discussed here.

#### Boundary Dataset
The boundary data is sourced from a community-run site known as [NaturalEarth](https://www.naturalearthdata.com/downloads/). All of the land boundaries are at the provicial level (e.g. states, administrative regions) and the maritime boundaries include seas, oceans, and some lakes although most small bodies of water are excluded from the dataset.

NaturalEarth ships all boundary data as a Shapefile directory with several files, each encoding a specific component of the geographical data. It also delivers maritime and land boundary data in separate Shapefile directories. Revgeocoder has a function in revgeocoder/src/utils/geodata.py called _shapefile_to_geojson() which can compress any Shapefile directory into a single GeoJSON file. _shapefile_to_geojson() was used to combine the maritime and land boundary data into a single GeoJSON file called boundaries.geojson located at revgeocoder/data/internal/. This file is the authoritative source of truth for boundary data in this module.

#### Spatial Indexing with R-tree
The naive approach to reverse geocoding a geographical coordinate pair in the context of this project would be to run a PiP operation for the point on every boundary available in boundaries.geojson. Although simple, this algorithm is inefficient with an upper bound complexity of something like O(n * m * p) where n is the number of coordinate pairs in the input, m is the number of boundaries in the dataset, and p is the average number of vertices in the boundary polygons.

The simplest optimization to this brute-force algorithm would be to implement basic quadrant filtering, which entails computing the geographical quadrant of the coordinate point, doing the same for every boundary in the dataset, eliminating all the boundaries outside of that quadrant, and running the PiP operation on the remaining candidate regions. To accelerate the categorization of boundaries into quadrants, it would be a good idea to temporarily replace the complex boundary polygons with minimum bounding rectangles (MBRs) just for this step since the margin of error introduced with this loss of precision is negligible in this context. This approach is better but ideally, there would be some way to replace quadrant filtering with a more fine-grained, and comparably fast, filtering algorithm. Happily, there is such an algorithm called R-tree-based filtering, which, while data-hungry, fits this criteria.

R-trees can be thought of as binary search trees (BSTs) for geographical data. Both data structures achieve roughly O(log n) query complexity by hierarchically splitting the search space at different scales and exploiting this to rapidly narrow down the set of possible targets. R-trees are typically populated with boundary MBRs. Revgeocoder does this with build_rtree() at revgeocoder/src/utils/qindex.py. It then uses the R-tree to quickly narrow down the set of possible regions for every query point in the input data and runs PiP on the remaning candidate regions until it finds a match. Revgeocoder computes and stores the MBRs with compute_mbrs() in revgeocoder/src/utils/geodata.py. 

Below is a graphical representation of an R-tree that makes its underlying concepts simple to grasp.

![rtree-graphical-representation](https://github.com/meyassu/quaker/raw/main/documentation/img/rtree.png?raw=true)
R-tree graphical representation.<br>

#### Reverse Geocoding Algorithm
As described in [Spatial Indexing with R-tree](#spatial-indexing-with-r-tree), the core reverse geocoding algorithm, which is implemented in revgeocoder/src/core/rgc.py, consists of doing the following for every coordinate point in the input: query the R-tree to get the set of candidate regions and perform the PiP operation on each candidate region until a match is found. It is possible to carry out this process for large inputs with batch processing while using a raw CSV file on SSD as a rudimentary database but this is inferior to using a true relational database engine. Revgeocoder is designed to work with any PostGreSQL database the user specifies in their configuration file (details on configuration can be found in [Instructions](#instructions)), and minimizes the volume of in-memory computation via batch processing. The batch size can also be specified in the configuration file by the user. Revgeocoder interfaces with the database via the functions in revgeocoder/src/utils/database.py.

### Infrastructure
The only infrastructural component to Revgeocoder is its database connectivity. The important thing to note here is that Revgeocoder can work with any PostGreSQL database and even supports interaction with RDS-hosted databases as long as the program is executed from an AWS Cloud compute instance with IAM authentication privileges. This functionality was built with the boto3 AWS SDK.

Program logs can be found in revgeocoder/user_data/logs.txt after program execution.

### Econbot
Econbot is a Selenium-based web scraper that compiles a single coherent time-series rGDP dataset from many isolated files on the [FRED](https://fred.stlouisfed.org/) research site. It works by taking in a set of countries from a preloaded PostGreSQL database, querying the [FRED](https://fred.stlouisfed.org/) search engine for rGDP data for each country, ranking the search results according to some internal criteria, navigating to the best page, downloading the CSV files on the page, and then, once its finished going through all the different countries, combining all the CSV files into a single dataset. It then pushes the dataset to the remote PostGreSQL database specified by the user.

At the moment, Econbot can only get data for a specific set of countries but in the future, it will be a fully generalized program. Currently, it just pushes pre-collected data to the user-specified database but its web-scraping capabilities can be called if post-2023 data is needed.

Program logs can be found in econbot/logs/logs.txt after program execution.

## Repository Contents


## Instructions



## Future Work