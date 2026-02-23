import xarray as xr
import numpy as np
import os
import geopandas as gpd
import pandas as pd
from multiprocessing import Pool


# The files were in netCDF3zip format, after manually unzipping, they are netCDF3 now
def clean_nc_file(file, districts):
   with xr.open_dataset(file) as ds:
        lons = ds["lon"].values
        lats = ds["lat"].values

        # Create GeoDataFrame of points
        point_gdf = gpd.GeoDataFrame(
            geometry=gpd.points_from_xy(lons, lats),
            crs="EPSG:4326"
        )

        # Spatial join - extract county code
        joined = gpd.sjoin(point_gdf, districts, how="left", predicate="within")

        # Extract date
        dates = pd.to_datetime(ds["time"].values).tz_localize("UTC").tz_convert("Asia/Shanghai").tz_localize(None).values.astype("datetime64[D]").astype("U10")
        ds["date"] = xr.DataArray(dates, dims="time")

        # Add back to the netcdf
        ds["region"] = xr.DataArray(joined["dt_adcode"].to_numpy(), dims="time")
        print("added region and time variable")

        # Filter valid rows
        cat1 = pd.Categorical(ds["region"].values)
        mask = (cat1.codes >= 0) 
        ds_masked = ds.isel(time=mask)  
        print("masked")

        # Created combined group of region and time
        region_vals = ds_masked["region"].values
        date_vals = ds_masked["date"].values
        combined_labels = [f"{r}_{d}" for r, d in zip(region_vals, date_vals)]
        ds_masked["combined_group"] = xr.DataArray(combined_labels, dims=["time"])
        print("assigned combined group")

        # Select variables to keep
        variables_to_keep = ['CERES_solar_zenith_at_surface',
                             'CERES_SW_TOA_flux___upwards',
                             'CERES_SW_radiance___upwards',
                             'CERES_LW_TOA_flux___upwards',
                             'CERES_downward_SW_surface_flux___Model_B',
                             'CERES_net_SW_surface_flux___Model_B',
                             'CERES_downward_SW_surface_flux___Model_B__clearsky',
                             'CERES_downward_LW_surface_flux___Model_B',
                             'CERES_net_LW_surface_flux___Model_B',
                             'CERES_downward_LW_surface_flux___Model_B__clearsky',
                             'combined_group']
        ds_selected = ds_masked[variables_to_keep]
        print("selected target variables")

        # Convert to dataframe
        df_temp = ds_selected.to_dataframe()
        print("converted")

        # set CERES_SW_TOA_flux___upwards and CERES_SW_radiance___upwards to NaN if zero
        cols = ["CERES_SW_TOA_flux___upwards",
                "CERES_SW_radiance___upwards",
                "CERES_net_SW_surface_flux___Model_B",
                "CERES_solar_zenith_at_surface"]
        df_temp.loc[df_temp["CERES_solar_zenith_at_surface"] > 90, cols] = np.nan
        # normalize the flux variables
        df_temp["cos_solar"] = np.cos(np.deg2rad(df_temp["CERES_solar_zenith_at_surface"]))
        # create normalized versions
        for col in cols[:-1]:
            df_temp[col + "_norm"] = df_temp[col] / df_temp['cos_solar']

        # Calculate means by combined group, clean the data
        df_final = df_temp.groupby(['combined_group']).mean() # NAvalues are ignored
        df_final = df_final.reset_index()
        df_final['adcode'] = df_final['combined_group'].str[0:6]
        df_final['date'] = df_final['combined_group'].str[7:]
        df_final.drop(columns=["combined_group"],inplace=True)
        print(file, "completed")

        return df_final
        
if __name__ == "__main__":

    # load all files
    os.chdir("/Users/anora/Library/CloudStorage/Dropbox-TeamMG/Wanru Wu/Cloudseeding_Anora/SSF/raw/terra")

    path = os.getcwd() 
    
    # Get the list of all files and directories 
    all_files = os.listdir(path) 
    data_files_terra = [file for file in all_files if file.find("CERES_SSF_Terra-XTRK_Edition4A_Subset")!=-1]

    # Load the county-level district file
    county_gdf = gpd.read_file(os.path.dirname(os.getcwd()) + "/district/district.shp")
    county_gdf = county_gdf.to_crs("EPSG:4326")

    map_iterables = [(file,county_gdf) for file in data_files_terra]

    # List used to store all dataframes created from nc files
    with Pool(9) as pool:
        df_list = pool.starmap(clean_nc_file, map_iterables)
    
    df = pd.concat(df_list)
    output_path = os.path.dirname(os.path.dirname(os.getcwd())) + '/intermediate/combined_ssf.csv'
    df.to_csv(output_path, index=False)
    