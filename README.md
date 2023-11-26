# Quaker

## Table of Contents
- [Overview](#meme)
- [The Data](#the-data)
- [Modules](#modules)
- [Repository Contents](#repository-contents)
- [Instructions](#instructions)

## Overview (#meme)
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

R-trees can be thought of as binary search trees (BSTs) for geographical data. Both data structures achieve roughly O(log n) query complexity by hierarchically splitting the search space at different scales and exploiting this to rapidly narrow down the set of possible targets. R-trees are typically populated with boundary MBRs. Revgeocoder does this with build_rtree() at revgeocoder/src/utils/qindex.py. It then uses the R-tree to quickly narrow down the set of possible regions for every query point in the input data and runs PiP on the remaning candidate regions until it finds a match. Below is a graphical representation of an R-tree that makes its underlying concepts simple to grasp.

![rtree-graphical-representation](https://github.com/meyassu/quaker/raw/main/documentation/img/rtree.png?raw=true)
R-tree graphical representation.<br>

#### Reverse Geocoding Algorithm
As mentioned in Spatial Indexing with R-tree()

### Econbot


## Repository Contents


## Instructions