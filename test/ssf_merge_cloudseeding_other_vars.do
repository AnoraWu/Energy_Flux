clear all
cd "/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding_Anora/SSF/intermediate"

import delimited "combined_ssf_droplets_long.csv", clear


gen year = real(substr(date, 1, 4))
gen month = real(substr(date, 6, 2))
gen day = real(substr(date, 9, 2))
drop date
ren adcode dt_adcode

duplicates report year month day dt_adcode

tempfile flux_data
save `flux_data'


* merge with skeleton
use "skeleton_merged2024_ssf.dta"

replace dt_adcode = ct_adcode if dt_adcode == .
replace ct_adcode = pr_adcode if ct_adcode ==.

merge 1:1 year month day dt_adcode using `flux_data'

preserve
keep if _merge == 2
* dt_adcode == 710000 is unmatched, we drop those data
* dates after 2024 sep 1st are not available in skeleton, we drop as well
restore

drop if _merge == 2
drop _merge
* drop the index variable
drop v1


* final clean var name and label
 v9 stddev_of_ice_particle_effective v11 v12 v13 v14 v15 v16 v17 v18 v19 mean_liquid_water_path_for_cloud v21 stddev_of_liquid_water_path_for_v23 mean_ice_water_path_for_cloud_la v25 stddev_of_ice_water_path_for_clo v27


ren mean_water_particle_radius_for_c mean_water_radius_37_0 
label var mean_water_radius_37_0 "CERES mean water particle radius for cloud layer 3.7, layer 0"

ren v5 mean_water_radius_37_1
label var mean_water_radius_37_1 "CERES mean water particle radius for cloud layer 3.7, layer 1"

ren stddev_of_water_particle_radius_v7 stddev_water_radius_37_0 
label var stddev_water_radius_37_0 "CERES stddev water particle radius for cloud layer 3.7, layer 0"

ren v9 stddev_water_radius_37_1
label var stddev_water_radius_37_1 "CERES stddev water particle radius for cloud layer 3.7, layer 1"

ren mean_water_particle_radius_for_c mean_water_radius_37_0 
label var mean_water_radius_37_0 "CERES mean water particle radius for cloud layer 3.7, layer 0"

ren mean_water_particle_radius_for_c mean_water_radius_37_0 
label var mean_water_radius_37_0 "CERES mean water particle radius for cloud layer 3.7, layer 0"

ren mean_water_particle_radius_for_c mean_water_radius_37_0 
label var mean_water_radius_37_0 "CERES mean water particle radius for cloud layer 3.7, layer 0"

ren mean_water_particle_radius_for_c mean_water_radius_37_0 
label var mean_water_radius_37_0 "CERES mean water particle radius for cloud layer 3.7, layer 0"

ren mean_water_particle_radius_for_c mean_water_radius_37_0 
label var mean_water_radius_37_0 "CERES mean water particle radius for cloud layer 3.7, layer 0"

ren mean_water_particle_radius_for_c mean_water_radius_37_0 
label var mean_water_radius_37_0 "CERES mean water particle radius for cloud layer 3.7, layer 0"

ren mean_water_particle_radius_for_c mean_water_radius_37_0 
label var mean_water_radius_37_0 "CERES mean water particle radius for cloud layer 3.7, layer 0"

ren mean_water_particle_radius_for_c mean_water_radius_37_0 
label var mean_water_radius_37_0 "CERES mean water particle radius for cloud layer 3.7, layer 0"

ren mean_water_particle_radius_for_c mean_water_radius_37_0 
label var mean_water_radius_37_0 "CERES mean water particle radius for cloud layer 3.7, layer 0"




save "skeleton_merged2024_ssf_complete.dta", replace

