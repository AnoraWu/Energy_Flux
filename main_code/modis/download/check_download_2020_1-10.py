import os
import glob
import warnings
warnings.filterwarnings('ignore') 


### Check Downloaded MODIS Data 
print("start checking..")

parent_folder_path = '/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding_Anora/MODIS_L2'
os.chdir(parent_folder_path)

cloud_file = "MOD06_L2"
geo_file = "MOD03"

years = [str(y) for y in range(2020,2021)]
days  = ["{:03d}".format(num) for num in range(1,8)] # keep day 366, ignore in nonleap years
hours = ["{:02d}".format(num) for num in range(0, 24)]
mins  = ["{:02d}".format(num) for num in range(0, 60, 5)]

# although we only download 20 days once a time,
# but we iterating over all the days to avoid manual operation errors
for year in years:
    for day in days:

        cloud_list = glob.glob(f"{cloud_file}/{year}/{day}/{cloud_file}.A{year}{day}.*.hdf")
        geo_list   = glob.glob(f"{geo_file}/{year}/{day}/{geo_file}.A{year}{day}.*.hdf")

        for hour in hours:
            for min in mins:
                geo_path = glob.glob(geo_file + "/" + year + "/" + day + "/" + geo_file + ".A" + year + day + "." + hour + min + "*.hdf")

                if len(geo_path) == 0: #no files exist, go next
                    print(year,day,hour,min,"mod03 file doesn't exist")


for year in years:
    for day in days:

        cloud_list = glob.glob(f"{cloud_file}/{year}/{day}/{cloud_file}.A{year}{day}.*.hdf")
        geo_list   = glob.glob(f"{geo_file}/{year}/{day}/{geo_file}.A{year}{day}.*.hdf")


        for hour in hours:
            for min in mins:
                cloud_path = glob.glob(cloud_file + "/" + year + "/" + day + "/" + cloud_file + ".A" + year + day + "." + hour + min + "*.hdf")

                if len(cloud_path) == 0: #no files exist, go next
                    print(year,day,hour,min,"mod06 file doesn't exist")


print("finished checking")