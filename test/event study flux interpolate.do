///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//画图:2025/9/20
//Yang Zhang
///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

clear all
set more off

if c(username)=="dengzichen" {
	global dir "~/Dropbox/Cloud Seeding"
}
else if c(username)=="13429" {
	global dir "E:/Dropbox/Cloud Seeding"
}
else if c(username)=="sw" {
	global dir "/Users/shaoda/Dropbox/Cloud Seeding"
}
else if c(username)=="AW" {
	global dir "F:\dropbox\Dropbox\Cloud Seeding"
}
else if c(username) == "anora"{
	global dir "/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding/Cloud Seeding"
}
else {
	global dir ""
}

cd "$dir"
global raw_data "$dir/data/raw"
global data "$dir/data"

***************************************************************************************************
// define output directories
global data_tem "$data/tem/match/pm_5_int"
global final "/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding_Anora/SSF/final/basic_did"
global output "/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding_Anora/SSF/output/basic_did"


///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
//1.Precipitation.jpg
clear all 
use "$dir/data/raw/skeleton_merged2024.dta"

replace dt_adcode = ct_adcode if dt_adcode == .

replace imply = 0 if imply == .

gen cohort =.
bysort county: egen maxcs = max(imply)
replace cohort = date if maxcs == 1	
gen never_treated=(cohort==.)

bysort county year month: gen cohort_first_event_date = date if imply == 1
bysort county year month: egen maxcohort = max(cohort_first_event_date)
replace cohort = maxcohort if maxcohort != .
drop if maxcohort==.

gen refy=date-cohort
replace refy = . if maxcohort == .

drop if refy > 10 & refy != .
drop if refy < -7


tab refy,miss gen(devent)
des devent*

replace devent7 = 0

keep if refy != .
keep year month day prov city county devent* refy

merge 1:1 year month day prov city county using "$raw_data/skeleton_merged2024_ssf_resid.dta"
keep if _merge == 3
drop _merge

save "$dir/data/tem/event study flux interpolate.dta", replace

***no controls
*CERES SW TOA flux - upwards
reghdfe sw_toa_flux_up devent*, absorb(dt_adcode date) vce(cluster city)
coefplot, yline(0, lp(solid) lc(cranberry)) xline(7.5, lp(dash) lc(black)) baselevels omitted vert keep(devent*) xlabel(1 "-7" 2 "-6" 3 "-5" 4 "-4" 5 "-3" 6 "-2" 7 "-1" 8 "0" 9 "1" 10 "2" 11 "3" 12 "4" 13 "5" 14 "6" 15 "7" 16 "8" 17 "9" 18 "10", labsize(medsmall)) ylabel(, nogrid) ciopt(lcolor(black)) mcolor(black)  ///
    xtitle("Days Relative to Cloud Seeding Day")
graph export "$outcome/SW TOA flux interpolate.jpg", replace

*CERES SW radiance - upwards
reghdfe sw_radiance_up devent*, absorb(dt_adcode date) vce(cluster city)
coefplot, yline(0, lp(solid) lc(cranberry)) xline(7.5, lp(dash) lc(black)) baselevels omitted vert keep(devent*) xlabel(1 "-7" 2 "-6" 3 "-5" 4 "-4" 5 "-3" 6 "-2" 7 "-1" 8 "0" 9 "1" 10 "2" 11 "3" 12 "4" 13 "5" 14 "6" 15 "7" 16 "8" 17 "9" 18 "10", labsize(medsmall)) ylabel(, nogrid) ciopt(lcolor(black)) mcolor(black)  ///
    xtitle("Days Relative to Cloud Seeding Day")
graph export "$outcome/SW radiance interpolate.jpg", replace

*CERES net SW surface flux - Model B
reghdfe net_sw_surface_flux devent*, absorb(dt_adcode date) vce(cluster city)
coefplot, yline(0, lp(solid) lc(cranberry)) xline(7.5, lp(dash) lc(black)) baselevels omitted vert keep(devent*) xlabel(1 "-7" 2 "-6" 3 "-5" 4 "-4" 5 "-3" 6 "-2" 7 "-1" 8 "0" 9 "1" 10 "2" 11 "3" 12 "4" 13 "5" 14 "6" 15 "7" 16 "8" 17 "9" 18 "10", labsize(medsmall)) ylabel(, nogrid) ciopt(lcolor(black)) mcolor(black)  ///
    xtitle("Days Relative to Cloud Seeding Day")
graph export "$outcome/net SW surface flux interpolate.jpg", replace


*** controlling for solar_zenith
*CERES SW TOA flux - upwards
reghdfe sw_toa_flux_up devent* solar_zenith, absorb(dt_adcode date) vce(cluster city)
coefplot, yline(0, lp(solid) lc(cranberry)) xline(7.5, lp(dash) lc(black)) baselevels omitted vert keep(devent*) xlabel(1 "-7" 2 "-6" 3 "-5" 4 "-4" 5 "-3" 6 "-2" 7 "-1" 8 "0" 9 "1" 10 "2" 11 "3" 12 "4" 13 "5" 14 "6" 15 "7" 16 "8" 17 "9" 18 "10", labsize(medsmall)) ylabel(, nogrid) ciopt(lcolor(black)) mcolor(black)  ///
    xtitle("Days Relative to Cloud Seeding Day")
graph export "$outcome/SW TOA flux_zenith interpolate.jpg", replace

*CERES SW radiance - upwards
reghdfe sw_radiance_up devent* solar_zenith, absorb(dt_adcode date) vce(cluster city)
coefplot, yline(0, lp(solid) lc(cranberry)) xline(7.5, lp(dash) lc(black)) baselevels omitted vert keep(devent*) xlabel(1 "-7" 2 "-6" 3 "-5" 4 "-4" 5 "-3" 6 "-2" 7 "-1" 8 "0" 9 "1" 10 "2" 11 "3" 12 "4" 13 "5" 14 "6" 15 "7" 16 "8" 17 "9" 18 "10", labsize(medsmall)) ylabel(, nogrid) ciopt(lcolor(black)) mcolor(black)  ///
    xtitle("Days Relative to Cloud Seeding Day")
graph export "$outcome/SW radiance_zenith interpolate.jpg", replace

*CERES net SW surface flux - Model B
reghdfe net_sw_surface_flux devent* solar_zenith, absorb(dt_adcode date) vce(cluster city)
coefplot, yline(0, lp(solid) lc(cranberry)) xline(7.5, lp(dash) lc(black)) baselevels omitted vert keep(devent*) xlabel(1 "-7" 2 "-6" 3 "-5" 4 "-4" 5 "-3" 6 "-2" 7 "-1" 8 "0" 9 "1" 10 "2" 11 "3" 12 "4" 13 "5" 14 "6" 15 "7" 16 "8" 17 "9" 18 "10", labsize(medsmall)) ylabel(, nogrid) ciopt(lcolor(black)) mcolor(black)  ///
    xtitle("Days Relative to Cloud Seeding Day")
graph export "$outcome/net SW surface flux_zenith interpolate.jpg", replace








