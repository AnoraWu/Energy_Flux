#!/bin/bash

# This master shell script runs all scripts and generates datasets used in the paper
# Author: Anora Wu 
# Contact Anora Wu wanru@uchicago.edu (expired at Aug 1st 2026) or AnoraaaBiu@outlook.com
# Date: Feb 23th 2026

# Set data directories
# CHANGE TO YOUR OWN DIRECTORY 
case "$USER" in
  anora)
    export DATA_DIR="/Users/anora/Team MG Dropbox/Wanru Wu/Energy_Flux" ;;
  *)
    echo "Unknown user: $USER" && exit 1 ;;
esac

# Whether run tests
# CHANGE TO FALSE IF CHECK IS NOT NEEDED
export CHECK=True

# Set working directory
# CHANGE TO YOUR OWN DIRECTORY 
cd "/Users/anora/Documents/Github/Energy_Flux/main_code"

### jiangxi_shape.py
# Purpose: create the shapefile for Jiangxi Province  
# Input: 
# 1. {data_fir}/township_shapefile/xiangzhen.shp
# Output:
# 1. {data_fir}/jiangxi_shapefile/jiangxi_shape.csv

python jiangxi_shape.py

### clean_town_operation.py
# Purpose: clean and combine town operation data from 2021 to 2025. 
# Input: 
# 1. {data_fir}/operation/{year}.xls
# 2. {data_fir}/operation/{year}.xlsx
# 3. {data_fir}/township_shapefile/xiangzhen.shp
# Output:
# 1. {data_fir}/intermediate/cleaned_operation.csv

python clean_town_operation.py

### merge_town_to_grid.py
# Purpose: create grids used for analysis; merge the operation data into the panel structure (grid by date).
# Input:
# 1. {data_dir}/jiangxi_shapefile/jiangxi_shape.shp
# 2. {data_dir}/intermediate/cleaned_operation.csv
# Output:
# 1. {data_dir}/intermediate/jx_grid.gpkg
# 2. {data_dir}/intermediate/jx_grid.csv
# 3. {data_dir}/intermediate/grid_with_operation_{year}.csv

python merge_town_to_grid.py

### process raw data on server 
# copied the scripts for processing data from 2020 1st to 100th day as an example
# please find scripts for all dates on "/project/mgreenst/energy_flux/code/"

# step 1: download_2020_1-100.sh 
# Purpose: download the modis data to RCC

# step 2: check_download_2020_1-100.sh 
# Purpose: data files are sometimes omitted in the downloading process, so this script checks if all files are downloaded.

# step 3: clean_modis_2020_1-100.sh 
# Purpose: extract and convert modis datasets to csv files.

# step 4: check_cloud_top_fill_2025.sh
# Purpose: according to page 89 in "/Energy_Flux/modis_l2/Documentation/MODIS 6/6.1 User Guide.pdf" in Dropbox, this scripts check if 
#          cloud optical thickness that have corresponding 1 km cloud top temperature or pressure retrievals set to fill 
#          values are discarded. 
# Note:    only years 2024 and 2025 are checked, and results in "/project/mgreenst/energy_flux/code/log" indicates 
#          all entries with 1km cloud top temperature or pressure retrievals set to fill values are discarded

### merge_modis_to_grid_unfiltered.py
# Purpose: clean and merge the modis data into the panel structure (grid by date).
# Input:
# 1. {data_dir}/intermediate/grid_with_operation_{year}.csv
# 2. {data_dir}/jiangxi_shapefile/jiangxi_shape.shp
# 3. {data_dir}/intermediate/modis/1km_{date}.csv
# 4. {data_dir}/intermediate/modis/5km_{date}.csv
# Output:
# 1. {data_dir}/intermediate/grid_with_operation_cloud_{year}_unfiltered.csv

python merge_modis_to_grid_unfiltered.py