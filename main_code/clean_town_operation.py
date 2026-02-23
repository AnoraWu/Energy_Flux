import geopandas as gpd
from shapely.geometry import MultiPolygon
import pandas as pd

### Clean Township Shapefile

# load data
townshape = gpd.read_file('/Users/anora/Library/CloudStorage/Dropbox-TeamMG/Wanru Wu/Cloudseeding_Anora/township_shapefile/xiangzhen.shp')

# data clean
townshape = townshape[townshape['省'] == "江西省"]
townshape = townshape[['省','市','县','乡','geometry']]
townshape.columns = ["prov","city","county","town","geometry"]
townshape = townshape.reset_index(drop=True)

# check geometry
s1 = set(townshape['geometry'].apply(lambda g: g.geom_type))
print(s1)

# Two types exists: polygon and multipolygon, all convert 
# to multipolygon to avoid geometry issues
s2 = set()
townshape['geometry'] = townshape['geometry'].apply(
    lambda g: MultiPolygon([g]) if g.geom_type == 'Polygon' else g
)
s2 = set(townshape['geometry'].apply(lambda g: g.geom_type))
print(s2) # should only have multipolygon


### Clean 2020 Cloudseeding Operation Data

# import files
operation_2020 = pd.read_excel("/Users/anora/Library/CloudStorage/Dropbox-TeamMG/Wanru Wu/Cloudseeding_Anora/operation/2020.xlsx")

# keep useful columns and rename
use_cols = {'GPS经度':'lon','GPS纬度':'lat','作业日期':'date','作业器具类型':'tool','作业类型':'type',
        '高炮炮弹用量(发)':'num_gaopao','火箭弹用量（枚）':'num_rocket',
        '烟条用量（根）':'num_cigar','其他用量':'num_other','作业开始时间':'start_time','作业结束时间':'end_time',
        '作业前天气状况':'weather_before','作业后天气状况':'weather_after','作业面积（平方公里）':'area',
        '作业效果':'effect','作业市':'city_o','作业县':'county_o','作业地点':'location_o'}
operation_2020 = operation_2020[list(use_cols.keys())]
operation_2020 = operation_2020.rename(columns=use_cols)

# extract the town of operation
operation_2020_gdf = gpd.GeoDataFrame(
    operation_2020,
    geometry=gpd.points_from_xy(operation_2020.lon, operation_2020.lat),
    crs="EPSG:4326"  # Set the Coordinate Reference System (CRS)
)
joined_2020 = gpd.sjoin(operation_2020_gdf, townshape, how="left", predicate="within")

### Clean 2021-2025 Cloudseeding Operation Data

data_list_2021_2025 = []
for i in range(2021,2026):
    # import files
    operation = pd.read_excel(f"/Users/anora/Library/CloudStorage/Dropbox-TeamMG/Wanru Wu/Cloudseeding_Anora/operation/{i}.xls")

    # keep useful columns and rename
    use_cols_i = {'作业日期':'date', '作业开始时间':'start_time','作业结束时间':'end_time',
                  '所属市':'city_o','所属县':'county_o','作业器具类型':'tool', '作业用量':'num',
                  '作业前天气':'weather_before','作业后天气':'weather_after','作业面积':'area','作业效果':'effect',
                  '服务领域':'field','经度':'lon','纬度':'lat'}
    operation = operation[list(use_cols_i.keys())]
    operation = operation.rename(columns=use_cols_i)

    # extract the town of operation
    operation_gdf = gpd.GeoDataFrame(
        operation,
        geometry=gpd.points_from_xy(operation.lon, operation.lat),
        crs="EPSG:4326"  # Set the Coordinate Reference System (CRS)
    )
    joined = gpd.sjoin(operation_gdf, townshape, how="left", predicate="within")
    data_list_2021_2025.append(joined)

joined_2021_2025 = pd.concat(data_list_2021_2025, ignore_index=True)
data = pd.concat([joined_2021_2025, joined_2020], ignore_index=True)
data.to_csv("/Users/anora/Library/CloudStorage/Dropbox-TeamMG/Wanru Wu/Cloudseeding_Anora/operation/cleaned_data.csv")
