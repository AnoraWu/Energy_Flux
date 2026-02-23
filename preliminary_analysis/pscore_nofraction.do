***********************************************************************************************************
** this do file 
** data files used: 
** data files produced:
** last update: 09/14/2025
***********************************************************************************************************
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
global final "/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding_Anora/SSF/final/match_y"
global output "/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding_Anora/SSF/output/match_y"


*** cloud condition -7 ~ -1 
**** data preparation 
use "$raw_data/skeleton_merged2024_ssf.dta", clear

drop if pre_mars==. & thickness ==.& GPM_20_20==. & velocity==. & air_fraction_mean==. 

replace dt_adcode = ct_adcode if dt_adcode == .
replace ct_adcode = pr_adcode if ct_adcode ==.

drop date
gen date = mdy(month, day, year)
tsset dt_adcode date

forval i = 1/7 {
	gen velocity_pre`i' = l`i'.velocity
	gen air_pre`i' = l`i'.air_fraction_mean
	gen thickness_pre`i' = l`i'.thickness
}

logit imply thickness_pre1 thickness_pre2 thickness_pre3 thickness_pre4 thickness_pre5 thickness_pre6 thickness_pre7 ///
	velocity_pre1 velocity_pre2 velocity_pre3 velocity_pre4 velocity_pre5 velocity_pre6 velocity_pre7 ///
	air_pre1 air_pre2 air_pre3 air_pre4 air_pre5 air_pre6 air_pre7 

** estimate the propensity scores for each unit
predict pscore, pr

keep year month day dt_adcode ct_adcode pr_adcode imply pscore 

save "$final/pscore_nofraction.dta", replace
	
	

****with short run cloud condition, i.e. day 0 
// use "$raw_data/skeleton_merged2024.dta", clear
//
// drop if pre_mars==. & thickness ==. & fraction==.& GPM_20_20==. & velocity==. & air_fraction_mean==. 
//
// replace dt_adcode = ct_adcode if dt_adcode == .
// replace ct_adcode = pr_adcode if ct_adcode ==.
//
// gen date = mdy(month, day, year)
// tsset dt_adcode date
//
// forval i = 1/7 {
// 	gen thickness_pre`i' = l`i'.thickness
// 	gen thickness_pre`i' = l`i'.fraction
// 	gen velocity_pre`i' = l`i'.velocity
// 	gen air_pre`i' = l`i'.air_fraction_mean
// }
//
// logit imply thickness fraction velocity air_fraction_mean thickness_pre1 thickness_pre2 thickness_pre3 thickness_pre4 thickness_pre5 thickness_pre6 thickness_pre7 thickness_pre1 thickness_pre2 thickness_pre3 thickness_pre4 thickness_pre5 thickness_pre6 thickness_pre7 velocity_pre1 velocity_pre2 velocity_pre3 velocity_pre4 velocity_pre5 velocity_pre6 velocity_pre7 air_pre1 air_pre2 air_pre3 air_pre4 air_pre5 air_pre6 air_pre7 
//
// ** estimate the propensity scores for each unit
// predict pscore, pr
//
// keep year month day dt_adcode ct_adcode pr_adcode imply pscore 
//
// save "$data_tem/pscore_day0.dta", replace

