# Author: Anora Wu
# Date: Mar 20th 2026
# Fill the modis cloud data into the constructed panel data.

import geopandas as gpd
import pandas as pd
import numpy as np
import glob
import os

data_dir = "/Users/anora/Team MG Dropbox/Wanru Wu/Energy_Flux"
check = "True"
# data_dir = os.environ['DATA_DIR']
# check = os.environ['CHECK']

# Read panel data with operations
panel_dic = {}
for year in range(2020,2026):
    panel_year = pd.read_csv(f"{data_dir}/intermediate/grid_with_operation_{year}.csv")
    panel_year['time'] = pd.to_datetime(panel_year['time'])
    panel_dic[f"{year}"] = panel_year

# Load and project JX polygon to EPSG:32650 
jx_poly = gpd.read_file(f"{data_dir}/jiangxi_shapefile/jiangxi_shape.shp").geometry.iloc[0]
# Original jx_poly was in "EPSG:4326", convert it to "EPSG:32650" to construct grids in kilometers
# "EPSG:32650" is used between between 114°E and 120°E
jx_poly_proj = gpd.GeoSeries([jx_poly], crs="EPSG:4326").to_crs("EPSG:32650").iloc[0]

# Calculate the bound of JX province            
minx, miny, maxx, maxy = jx_poly_proj.bounds      

grid_size = 5000

# Helper to extract bits from a byte column
def extract_bits(df, column, start, end):
    def _extract(x):
        if pd.isna(x):
            return pd.NA
        bin_str = format(int(x), '08b')
        return int(bin_str[-(end+1):8-start], 2)
    
    return df[column].apply(_extract)

# Read cloud data
modis_dir = f"{data_dir}/intermediate/modis"

all_files = glob.glob(os.path.join(modis_dir, '*.csv'))
for year in range(2020,2023):
    
    files_1km = [f for f in all_files if os.path.basename(f).startswith(f'1km_{year}')]
    files_5km = [f for f in all_files if os.path.basename(f).startswith(f'5km_{year}')]
    df_1km = pd.concat([pd.read_csv(f, index_col=0) for f in sorted(files_1km)], ignore_index=True)
    df_5km = pd.concat([pd.read_csv(f, index_col=0) for f in sorted(files_5km)], ignore_index=True)

    ##### 1KM DATA

    ### Filter out invalid data points based on quality assurance indicator
    # Convert to numerical data and handle NaN
    byte_cols = [col for col in df_1km.columns if col.startswith('qa1_byte_')]
    for col in byte_cols:
        df_1km[col] = pd.to_numeric(df_1km[col], errors='coerce')

    # Extract all flags
    df_1km['usefulness_flag'] = extract_bits(df_1km, 'qa1_byte_0', 0, 0)           # Byte 0, Bit 0

    # Apply base mask — Usefulness Flag == 1
    base_mask = df_1km['usefulness_flag'] == 1
    df_1km_filtered = df_1km[base_mask].copy()
    df_1km_filtered = df_1km_filtered[['lon','lat','cloud_optical_thickness','cloud_optical_thickness_uncertainty','date_time']]

    ### Construct time of each data entry
    df_1km_filtered['date_time'] = pd.to_datetime(df_1km_filtered['date_time']).dt.floor('h')

    ### Find cell id of each data entry
   
    # Get cell_x and cell_y, which starts from 0 (rather than 1)
    # lat and lon already in EPSG:32650 
    df_1km_filtered['cell_x'] = (np.array(df_1km_filtered['lon'])-minx)//grid_size 
    df_1km_filtered['cell_y'] = (np.array(df_1km_filtered['lat'])-miny)//grid_size 

    # Generate cell_id of the cell each operation data is in
    df_1km_filtered['cell_id'] = (
        df_1km_filtered['cell_y'].astype(int).astype(str) + "_" + 
        df_1km_filtered['cell_x'].astype(int).astype(str)
    )

    # Drop useless columns and clean
    df_1km_filtered = df_1km_filtered.drop(columns=['lat','lon','cell_x','cell_y'])
    df_1km_filtered.rename(columns={"date_time":"time"},inplace=True)
    # This will skip NA values when calculating means
    df_1km_filtered_ave = df_1km_filtered.groupby(['time','cell_id']).mean().reset_index()

    panel_dic[f"{year}"] = pd.merge(panel_dic[f"{year}"], df_1km_filtered_ave, on = ['time','cell_id'], how = "left")


    ##### 5KM DATA
    ### Filter out invalid data points based on quality assurance indicator
    # Convert to numerical data and handle NaN
    byte_cols = [col for col in df_5km.columns if col.startswith('qa5_byte_')]
    for col in byte_cols:
        df_5km[col] = pd.to_numeric(df_5km[col], errors='coerce')

    # Extract all flags
    df_5km['usefulness_flag'] = extract_bits(df_5km, 'qa5_byte_1', 0, 0)           # Byte 1, Bit 0
  
    # Apply base mask — Usefulness Flag == 1
    base_mask = df_5km['usefulness_flag'] == 1
    df_5km_filtered = df_5km[base_mask].copy()
    df_5km_filtered = df_5km_filtered[['lon','lat','cloud_fraction','cloud_fraction_night','cloud_fraction_day','date_time']]

    ### Construct time of each data entry
    df_5km_filtered['date_time'] = pd.to_datetime(df_5km_filtered['date_time']).dt.floor('h')

    ### Find cell id of each data entry

    # Get cell_x and cell_y, which starts from 0 (rather than 1)
    # lat and lon already in EPSG:32650 
    df_5km_filtered['cell_x'] = (np.array(df_5km_filtered['lon'])-minx)//grid_size 
    df_5km_filtered['cell_y'] = (np.array(df_5km_filtered['lat'])-miny)//grid_size 

    # Generate cell_id of the cell each operation data is in
    df_5km_filtered['cell_id'] = (
        df_5km_filtered['cell_y'].astype(int).astype(str) + "_" + 
        df_5km_filtered['cell_x'].astype(int).astype(str)
    )

    # Drop useless columns and clean
    df_5km_filtered = df_5km_filtered.drop(columns=['lat','lon','cell_x','cell_y'])
    df_5km_filtered.rename(columns={"date_time":"time"},inplace=True)
    # This will skip NA values when calculating means
    df_5km_filtered_ave = df_5km_filtered.groupby(['time','cell_id']).mean().reset_index()

    panel_dic[f"{year}"] = pd.merge(panel_dic[f"{year}"], df_5km_filtered_ave, on = ['time','cell_id'], how = "left")

    ### Save the data
    panel_dic[f"{year}"].to_csv(f"{data_dir}/intermediate/grid_with_operation_cloud_{year}.csv", index=False)


