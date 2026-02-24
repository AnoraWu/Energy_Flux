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

# clean_town_operation.py
# Purpose: clean and combine town operation data from 2021 to 2025. 
# Input: 
# 1. {data_fir}/operation/{year}.xls
# 2. {data_fir}/operation/{year}.xlsx
# Output:
# 1. {data_fir}/intermediate/cleaned_operation.csv
# python clean_town_operation.py

# merge_town_to_grid.py
# Purpose: create grids used for analysis; merge the operation data into the panel structure (grid by date).
# Input:
# 1. {data_dir}/jiangxi_shapefile/jiangxi_shape.shp
# 2. {data_dir}/intermediate/cleaned_operation.csv
# Output:
# 1. {data_dir}/intermediate/jx_grid.gpkg
# 2. {data_dir}/intermediate/jx_grid.csv
# 3. {data_dir}/intermediate/grid_with_operation.csv
python merge_town_to_grid.py

