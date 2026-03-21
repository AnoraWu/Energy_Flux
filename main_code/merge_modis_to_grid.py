# Author: Anora Wu
# Date: Mar 20th 2026
# Fill the modis cloud data into the constructed panel data.

import geopandas as gpd
import pandas as pd
import numpy as np
import glob
import os

data_dir = os.environ['DATA_DIR']
check = os.environ['CHECK']

# Read all data
modis_dir = f"{data_dir}/intermediate/modis"
all_files = glob.glob(os.path.join(modis_dir, '*.csv'))

for year in range(2020,2021):
    files_1km = [f for f in all_files if os.path.basename(f).startswith(f'1km_{year}')]
    files_5km = [f for f in all_files if os.path.basename(f).startswith(f'5km_{year}')]
    df_1km = pd.concat([pd.read_csv(f, index_col=0) for f in sorted(files_1km)], ignore_index=True)
    df_5km = pd.concat([pd.read_csv(f, index_col=0) for f in sorted(files_5km)], ignore_index=True)

