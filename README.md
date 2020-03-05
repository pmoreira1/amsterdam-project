<img src="https://bit.ly/2VnXWr2" alt="Ironhack Logo" width="100"/>

#  Amsterdam Project
*Patrick Colijn, Pedro Moreira, Rene Raube*

*Data Analytics, Amsterdam Mar 2020*

## Content
- [Project Description](#project-description)
- [Organization](#organization)
- [Links](#links)

## Project Description
This project will help everyone to get a better understanding of the waste system in Amsterdam.

## Organization
A trello board was used to structure the work for this project. Link in the links section below.

Repository structure:
```
├── Containers - Exploratory Analysis.ipynb
├── data - Contains all data needed for the analysis
│   ├── 2020-stadsdelen.xlsx
│   ├── containers.csv
│   ├── container_types.csv
│   ├── popuplation_data.csv
│   └── wells.csv
├── HeatMap 4.3.2020 version2.ipynb
├── images - Generated Heatmaps
│   ├── Capita per Container.png
│   └── container_per_neighbourhood.png
├── Map - Capita per container.ipynb
├── Map - Containers per Neighbourhood.ipynb
├── README.md - This File
├── scripts - THe fundaments for the data pipeline
│   ├── data_grabber.py - Grab the latest data and store it on db and .csv
│   └── postcode_grabber.py - Postcode checker for the relevant address.
└── shapes - Shapes used for drawing heatmaps
    ├── bc2010zw_region.dbf
    ├── bc2010zw_region.prj
    ├── bc2010zw_region.shp
    ├── bc2010zw_region.shx
    ├── brtk2010_ind2005_region.dbf
    ├── brtk2010_ind2005_region.prj
    ├── brtk2010_ind2005_region.shp
    └── brtk2010_ind2005_region.shx

```
## Links

[Repository](https://github.com/pmoreira1/amsterdam-project)  
[Trello](https://trello.com/b/PnrZx07R/waste-containers-project)  


Data Sources

https://data.overheid.nl

https://api.data.amsterdam.nl/

