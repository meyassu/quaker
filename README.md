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

To access the visualizations and the associated stories directly, see the [Instructions](#instructions) section.

![spatiotemporal-visualization-1987](https://github.com/meyassu/quaker/raw/main/documentation/img/earthquake_st_distribution.png?raw=true)
Global spatial distribution of earthquakes with magnitude > 6 on Richter scale in 1987.<br><br>

![spatiotemporal-visualization-2011](https://github.com/meyassu/quaker/raw/main/documentation/img/earthquake_st_distribution_2011.png?raw=true)
Global spatial distribution of earthquakes with magnitude > 5 on Richter scale in 2011 (note the cluster of activity around Japan).<br><br>

![spatiotemporal-visualization-japan-2011](https://github.com/meyassu/quaker/raw/main/documentation/img/earthquake_japan_2011.png?raw=true)
Closer look at Japan earthquakes in 2011.<br><br>




### Economic Effects of Earthquakes 
The countries included in these datasets showed a surprising economic resilience to severe earthquakes in that annual rGDP generally remained constant or rose even through seismically intense periods. The most striking example of this is the Japanese economy in the early 2010s.  These visualizations are interactive; users can change the year and country being displayed. Below are screenshots from the Qlik Sense Platform. 

To access the visualizations directly, see the [Instructions](#instructions) section.

![spatiotemporal-visualization-1987](https://github.com/meyassu/quaker/raw/main/documentation/img/earthquakes_rgdp.png?raw=true)
Time-series rGDP data parallel with time-series earthquake frequency data.<br><br>

![spatiotemporal-visualization-2011](https://github.com/meyassu/quaker/raw/main/documentation/img/earthquakes_rgdp_japan.png?raw=true)
Japanese rGDP resilient to 2011 catastrophes.<br><br>

## Modules

### Revgeocoder
Revgeocoder is a general-purpose reverse geocoder. It works by indexing an exhaustive dataset containing the geometry and location of every geographical boundary, both maritime and land, with an R*-tree, narrowing down the possible candidate regions with the R*-tree, and then performing a Point-in-Polygon operation on the filtered set of regions. Each of these components of Revgeocoder will be discussed here.

#### Boundary Dataset
The boundary data is sourced from a community-run site known as [NaturalEarth](https://www.naturalearthdata.com/downloads/) and comes in the form of a Shapefile directory. 


### Econbot


## Repository Contents


## Instructions