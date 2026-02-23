# Author: Anora Wu
# Date: Jan 7th 2026
# Construct the geographical 5km by 5km grids for Jiangxi Province

import geopandas as gpd
import pandas as pd
import numpy as np
import os
import warnings
from shapely.geometry import MultiPolygon
from shapely.geometry import box


# Change to your rawdata directory
os.chdir('/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding_Anora')

# Load and project JX polygon to EPSG:4527 
jx_poly = gpd.read_file('jiangxi/jiangxi_shape.shp').geometry.iloc[0]
jx_poly_proj = gpd.GeoSeries([jx_poly], crs="EPSG:4326").to_crs("EPSG:4527").iloc[0]

# Calculate the bound of JX province            
minx, miny, maxx, maxy = jx_poly_proj.bounds              

# Bins for grid
grid_size = 5000
x_bins = np.arange(minx, maxx + grid_size, grid_size)
y_bins = np.arange(miny, maxy + grid_size, grid_size)

polygons = []
for i in range(len(y_bins)-1):
    for j in range(len(x_bins)-1):
        # create 5km * 5km grids
        poly = box(x_bins[j], y_bins[i], x_bins[j+1], y_bins[i+1])
        polygons.append({
            "cell_id": f"{i}_{j}",
            "geometry": poly,
            "cell_y": i,
            "cell_x": j
        })

jx_grid = gpd.GeoDataFrame(polygons, crs="EPSG:4527")

# Filtering out grids outside JX
with warnings.catch_warnings():
    warnings.filterwarnings("ignore", category=RuntimeWarning, message="invalid value encountered in intersects")
    jx_grid = jx_grid[jx_grid.geometry.intersects(jx_poly_proj)]

# Save as GeoPackage 
jx_grid.to_file("jiangxi/jx_grid.gpkg", layer='grid', driver="GPKG")

# Save as CSV for later use
df = pd.DataFrame(jx_grid.drop(columns='geometry'))
df.to_csv("jiangxi/jx_grid.csv", index=False)