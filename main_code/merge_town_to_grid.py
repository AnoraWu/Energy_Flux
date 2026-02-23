# Author: Anora Wu
# Date: Jan 7th 2026
# Construct a panel data, with each day between 2020-2025 being the time variable and each grid being the identity. 
# Each identity has a geometry and a id. 
# Fill in the cloud seeding operation day and location into the time slots and the grid 

import geopandas as gpd
import pandas as pd
import numpy as np
import os
import warnings
from shapely.geometry import box
from pyproj import Transformer


# Change to your rawdata directory
os.chdir('/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding_Anora')

### CONSTRUCT GRID ###

# Load and project JX polygon to EPSG:32650 
jx_poly = gpd.read_file('jiangxi/jiangxi_shape.shp').geometry.iloc[0]
# Original jx_poly was in "EPSG:4326", convert it to "EPSG:32650" to construct grids in kilometers
# "EPSG:32650" is used between between 114°E and 120°E
jx_poly_proj = gpd.GeoSeries([jx_poly], crs="EPSG:4326").to_crs("EPSG:32650").iloc[0]

# Calculate the bound of JX province            
minx, miny, maxx, maxy = jx_poly_proj.bounds              

# Bins for 5km grid
grid_size = 5000
# Create boundaries for grids
# The last bin created by np.arange will cover the maxx and maxy
x_bins = np.arange(minx, maxx + grid_size, grid_size)
y_bins = np.arange(miny, maxy + grid_size, grid_size)

# Create box geometries that covered all jx_poly_proj based on generated bins 
polygons = []
for i in range(len(y_bins)-1):
    for j in range(len(x_bins)-1):
        # create 5km * 5km grids
        poly = box(x_bins[j], y_bins[i], x_bins[j+1], y_bins[i+1])
        polygons.append({
            "cell_id": f"{i}_{j}", # use i_j here because y_bins are row numbers while x_bins are column numbers
            "geometry": poly,
            "cell_y": i,
            "cell_x": j
        })
jx_grid = gpd.GeoDataFrame(polygons, crs="EPSG:32650")

# Filtering out grids outside JX, only keeping grids whose area of intersection with JX is larger than 0
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=RuntimeWarning, message= "invalid value encountered in intersects")
    jx_grid = jx_grid[jx_grid.geometry.intersection(jx_poly_proj).area > 0]

# Save as GeoPackage 
jx_grid.to_file("jiangxi/jx_grid.gpkg", layer='grid', driver="GPKG")

# Save as CSV for later use
df = pd.DataFrame(jx_grid.drop(columns='geometry'))
df.to_csv("jiangxi/jx_grid.csv", index=False)


### FILL IN OPERATION DATA ###

operation_data = pd.read_csv('operation/cleaned_data.csv')

# Correct one mistake in the data
condition = (operation_data['date'] == "2022-10-27") & (operation_data['start_time'] == "09:42") & (operation_data['city_o'] == "九江市")
operation_data.loc[condition, 'lon'] = 115.56
operation_data.loc[condition, 'lat'] = 29.043

# Add year and day of year to operation data
operation_data['date'] = pd.to_datetime(operation_data['date'])
operation_data['day'] = operation_data['date'].dt.dayofyear
operation_data['year'] = operation_data['date'].dt.year

# Transfer lat and lon to EPSG:32650
# "always_xy=True" ensures using the traditional GIS order, 
# that is longitude, latitude for EPSG:4326 and easting, northing for EPSG:32650
transformer = Transformer.from_crs("EPSG:4326", "EPSG:32650", always_xy=True)
operation_data['xs'], operation_data['ys'] = transformer.transform(
    operation_data['lon'].values, 
    operation_data['lat'].values
)

# Get cell_x and cell_y, which starts from 0 (rather than 1)
operation_data['cell_x'] = (np.array(operation_data['xs'])-minx)//grid_size 
operation_data['cell_y'] = (np.array(operation_data['ys'])-miny)//grid_size 

# Generate cell_id of the cell each operation data is in
operation_data['cell_id'] = (
    operation_data['cell_y'].astype(int).astype(str) + "_" + 
    operation_data['cell_x'].astype(int).astype(str)
)

# Filtering out invalid entries (operation outside JX)
valid_cells = set(jx_grid['cell_id'])
operation_data = operation_data[operation_data['cell_id'].isin(valid_cells)]

# Construct panel
date_range = pd.date_range(start='2020-01-01', end='2025-12-31', freq='D')
dates_df = pd.DataFrame({'date': date_range})
dates_df['year'] = dates_df['date'].dt.year
dates_df['month'] = dates_df['date'].dt.month
dates_df['day'] = dates_df['date'].dt.day

grid_ids = jx_grid[['cell_id']].copy()
dates_df['key'] = 1
grid_ids['key'] = 1
panel_df = pd.merge(dates_df, grid_ids, on='key').drop('key', axis=1)

# Count operation times
op_counts = operation_data.groupby(['date', 'cell_id']).size().reset_index(name='op_count')
op_counts['date'] = pd.to_datetime(op_counts['date'])

# Merge into the grid
final_panel = pd.merge(panel_df, op_counts, on=['date', 'cell_id'], how='left')
final_panel['op_count'] = final_panel['op_count'].fillna(0).astype(int)
final_grid = pd.merge(final_panel, jx_grid, on=['cell_id'], how='left')

final_grid.to_csv("intermediate/grid_1.csv")

