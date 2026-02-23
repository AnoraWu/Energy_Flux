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
global final "/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding_Anora/SSF/final"
global output "/Users/anora/Team MG Dropbox/Wanru Wu/Cloudseeding_Anora/SSF/output"


*** (1) Matching using integer value of rainfall and forecast  ========================
**** data preparation 
use "$raw_data/skeleton_merged2024_ssf.dta", clear

* drop unused variables
drop ice_mean liquid_mean air_fraction_mean evp pressure SSH 
drop tem_avg tem_max tem_min win_max win_inst_max 
drop 天气情况 pre_ch alert alertn published_time operation_type 
drop aqi pm25 lat lon smci wind_2min tem_0cm inversion

drop date
gen date = mdy(month, day, year)
tsset dt_adcode date

forval i = 1/7 {
	gen mars_pre`i' = int(l`i'.pre_mars)
}

forval i = 1/2 {
	gen mars_late`i' = int(1*f`i'.pre_mars)
}

forval i = 1/7 {
	gen sw_toa_flux_up`i' = int(0.02*l`i'.sw_toa_flux_up)
// 	gen solar_zenith`i' = int(0.1*l`i'.solar_zenith)
}

gen rain_pre1 = int(1*l1.rain_IDW)		
replace pre_mars = int(1*pre_mars)

keep mars_pre7 mars_pre6 mars_pre5 mars_pre4 mars_pre3 mars_pre2 mars_pre1 pre_mars mars_late1 mars_late2 rain_pre1 sw_toa_flux_up* county prov city dt_adcode ct_adcode pr_adcode year month day imply county_level city_level 

merge 1:1 dt_adcode ct_adcode pr_adcode year month day using "$data_tem/pscore.dta"
drop _merge
drop if pscore ==. // delete those useless observations 
gen id_t = _n if imply == 1
gen id_c = _n if imply == 0

save "$final/int_forecast_rain_temperature_01.dta", replace

********treated group 
keep if imply == 1
drop id_c 
drop prov city county 
rename pscore pscore_t

drop if mars_pre7==. | mars_pre6==.|mars_pre5==.|mars_pre4==.|mars_pre3==.| mars_pre2==.| mars_pre1==.| pre_mars==.| mars_late1==.| mars_late2==. 
drop if rain_pre1 == . 
drop if sw_toa_flux_up7 ==. | sw_toa_flux_up6 ==. | sw_toa_flux_up5 ==. | sw_toa_flux_up4 ==. | sw_toa_flux_up3 ==. | sw_toa_flux_up2 ==. | sw_toa_flux_up1==. | sw_toa_flux_up==.
// drop if solar_zenith7 ==. | solar_zenith6 ==. | solar_zenith5 ==. | solar_zenith4 ==. | solar_zenith3 ==. | solar_zenith2 ==. | solar_zenith1==.  //50,873 observations left


drop if mars_pre7==0 & mars_pre6==0 & mars_pre5==0 & mars_pre4==0 & mars_pre3 ==0 & mars_pre2 ==0 & mars_pre1==0 &mars_late1==0 &mars_late2==0 & pre_mars==0 & rain_pre1==0  & sw_toa_flux_up7 == 0 & sw_toa_flux_up6==0 & sw_toa_flux_up5==0 & sw_toa_flux_up4==0 & sw_toa_flux_up3==0 & sw_toa_flux_up2 ==0 & sw_toa_flux_up1 ==0 // 0 observations deleted

save "$final/treated_no0.dta", replace

*** Control group
use "$final/int_forecast_rain_temperature_01.dta", clear

drop if imply == 1

drop if mars_pre7==. | mars_pre6==.|mars_pre5==.|mars_pre4==.|mars_pre3==.| mars_pre2==.| mars_pre1==.| pre_mars==.| mars_late1==.| mars_late2==. 
drop if rain_pre1 == . 
drop if sw_toa_flux_up7 ==. | sw_toa_flux_up6 ==. | sw_toa_flux_up5 ==. | sw_toa_flux_up4 ==. | sw_toa_flux_up3 ==. | sw_toa_flux_up2 ==. | sw_toa_flux_up1==. | sw_toa_flux_up==. // 
// drop if solar_zenith7 ==. | solar_zenith6 ==. | solar_zenith5 ==. | solar_zenith4 ==. | solar_zenith3 ==. | solar_zenith2 ==. | solar_zenith1==. 

drop if mars_pre7==0 & mars_pre6==0 & mars_pre5==0 & mars_pre4==0 & mars_pre3 ==0 & mars_pre2 ==0 & mars_pre1==0 &mars_late1==0 &mars_late2==0 & pre_mars==0 & rain_pre1==0  & sw_toa_flux_up7 == 0 & sw_toa_flux_up6==0 & sw_toa_flux_up5==0 & sw_toa_flux_up4==0 & sw_toa_flux_up3==0 & sw_toa_flux_up2 ==0 & sw_toa_flux_up1 ==0 // 0 observations deleted

drop id_t
drop prov city county city_level county_level
rename pscore pscore_c

egen control_id = group(mars_pre7 mars_pre6 mars_pre5 mars_pre4 mars_pre3 mars_pre2 mars_pre1 pre_mars mars_late1 mars_late2 rain_pre1 sw_toa_flux_up7 sw_toa_flux_up6 sw_toa_flux_up5 sw_toa_flux_up4 sw_toa_flux_up3 sw_toa_flux_up2 sw_toa_flux_up1) 

save "$final/control_no0_all.dta", replace


**** exact matching 
drop dt_adcode ct_adcode pr_adcode year month day imply id_c pscore_c
duplicates drop control_id, force

joinby mars_pre7 mars_pre6 mars_pre5 mars_pre4 mars_pre3 mars_pre2 mars_pre1 pre_mars mars_late1 mars_late2 rain_pre1 sw_toa_flux_up7 sw_toa_flux_up6 sw_toa_flux_up5 sw_toa_flux_up4 sw_toa_flux_up3 sw_toa_flux_up2 sw_toa_flux_up1 using "$final/treated_no0.dta"


keep control_id id_t pscore_t imply 

save "$final/matchid_no0.dta", replace


****only keep treatments matched with controls 
keep id_t
merge 1:1 id_t using "$final/treated_no0.dta"
keep if _merge==3 // 15408 matched
drop _merge

* Drop treatments that occur within 14 days of the first cloud seeding event at a given location
gen date = mdy(month, day, year)
gen drop_flag = 0
bysort dt_adcode ct_adcode pr_adcode (date): replace drop_flag = 1 if _n > 1 & date - date[_n-1] <= 14	

drop if drop_flag == 1 // 13,158 deleted

foreach var of varlist mars_pre1-sw_toa_flux_up7{
	drop `var'
}
drop pre_mars date drop_flag

save "$final/treated_no0_matched.dta", replace //4430 points


***Adjust the matchid based on treatment
use "$final/matchid_no0.dta", clear

merge 1:1 id_t using "$final/treated_no0_matched.dta"
keep if _merge==3 // 4430 left
keep control_id id_t pscore_t imply

save "$final/matchid_no0.dta", replace


***control group 
use "$final/control_no0_all.dta", clear

keep dt_adcode ct_adcode pr_adcode year month day control_id id_c pscore_c imply

xtile quart_bp = id_c, nq(10) // divided into 10 subsamples -- it's faster than matching in one file 
tab quart_bp

save "$final/control_no0_all_quart.dta", replace

use "$final/control_no0_all_quart.dta", clear
* It takes approximately 20min to run the loop below. (这里是因为之前不加soil的匹配上的非常多，所以要花很长时间；如果匹配上的不多，或许可以不需要这个循环，直接运行)
//在这里要注意提前创建\match_no0\文件夹避免报错，以及更改改文件为可阅读
forvalues i = 1/10{
    use "$final/control_no0_all_quart.dta", clear
    keep if quart_bp == `i'
	
	joinby control_id using "$final/matchid_no0.dta"
	drop quart_bp control_id
	
	gen pscore_d = abs(pscore_t-pscore_c)
	sort id_t pscore_d
	by id_t: keep if _n==1
	
	drop pscore_c pscore_t
	save "$final/match_no0\match_`i'.dta", replace
	
	keep id_c id_t 
	merge 1:1 id_t using "$final/treated_no0_matched.dta", keep(match)
	drop _merge pscore_t sw_toa_flux_up
	
	append using "$final/match_no0\match_`i'.dta"
	
	sort id_t imply
	replace pscore_d = pscore_d[_n-1] if pscore_d==.
	tab imply
	save "$final/match_no0\match_`i'.dta", replace
}

** Combine mathced data and find the best psm ones. 
clear
cd "$final/match_no0"
openall

sort id_t imply pscore_d
by id_t imply: keep if _n ==1
tab imply

bys id_t id_c: replace city_level = city_level[_n+1] if city_level==.
bys id_t id_c: replace county_level = county_level[_n+1] if county_level==.

save "$final/psm_no0_matched.dta", replace


** Expand the dataset to include a time window from -7 to +7 days relative to the cloud seeding event
gen date=mdy(month,day,year)

expand 18
sort dt_adcode date

bysort id_t id_c dt_adcode date: gen refy = _n - 8
tab refy 

gen shifted_date = date + refy
replace month = month(shifted_date)
replace day = day(shifted_date)
replace year = year(shifted_date)
rename date event_date
rename shifted_date date

*merge prov city county
merge m:1 year month day dt_adcode ct_adcode pr_adcode using "$raw_data/skeleton_merged2024_ssf.dta" , keep(match master)
sort _merge // 55 unmatched, all from 2024/09
drop _merge

*merge meteorological data
merge m:1 year month day prov city county using "$raw_data/skeleton_merged2024_ssf.dta", keep(match master) 
drop _merge

save "$final/psm_10days.dta", replace


use "$final/psm_10days.dta", clear
gen event = refy+7
fvset base 6 event

egen unique_county=group(dt_adcode id_t)

egen doy=group(month day)

egen calendar_month=group(year month)

gen cluster=.
replace cluster = id_t if imply==1
replace cluster = id_c if imply==0


*************** Graphs
reghdfe sw_toa_flux_up i.event##c.imply, absorb(unique_county i.refy#i.id_t year doy) vce(cluster cluster calendar_month)

save "$final/result.dta", replace

coefplot, yline(0, lp(solid) lc(cranberry)) xline(7.5, lp(dash) lc(black)) baselevels omitted vert keep(*event#c.imply) xlabel(1 "-7" 2 "-6" 3 "-5" 4 "-4" 5 "-3" 6 "-2" 7 "-1" 8 "0" 9 "1" 10 "2" 11 "3" 12 "4" 13 "5" 14 "6" 15 "7", labsize(medsmall)) ylabel (, nogrid) ciopt(lcolor(black)) mcolor(black)  ///
    xtitle("Days Relative to Cloud Seeding Day") scheme(stcolor)	
graph export "F:\dropbox\Dropbox\Predoc_Project\Cloud_Seeding\output\draft\psm_temperature.png", replace



//weighted results
//1.weighted by county area
clear all
use "$final/psm_10days.dta", clear
gen event = refy+7
fvset base 6 event

egen unique_county=group(dt_adcode id_t)

egen doy=group(month day)

egen calendar_month=group(year month)

gen cluster=.
replace cluster = id_t if imply==1
replace cluster = id_c if imply==0

merge m:1 year prov city county using "F:\research\dataset\county\county_panel_line.dta"
keep if _merge == 3
drop _merge 

reghdfe tem_IDW i.event##c.imply [aw=county_area], absorb(unique_county i.refy#i.id_t year doy) vce(cluster cluster calendar_month)

coefplot, yline(0, lp(solid) lc(cranberry)) xline(7.5, lp(dash) lc(black)) baselevels omitted vert keep(*event#c.imply) xlabel(1 "-7" 2 "-6" 3 "-5" 4 "-4" 5 "-3" 6 "-2" 7 "-1" 8 "0" 9 "1" 10 "2" 11 "3" 12 "4" 13 "5" 14 "6" 15 "7", labsize(medsmall)) ylabel (, nogrid) ciopt(lcolor(black)) mcolor(black)  ///
    xtitle("Days Relative to Cloud Seeding Day") scheme(stcolor)	
graph export "F:\dropbox\Dropbox\Predoc_Project\Cloud_Seeding\output\draft\psm_temperature_weightedby_area.png", replace


//2.weighted by agriculture area
clear all
use "$final/psm_10days.dta", clear
gen event = refy+7
fvset base 6 event

egen unique_county=group(dt_adcode id_t)

egen doy=group(month day)

egen calendar_month=group(year month)

gen cluster=.
replace cluster = id_t if imply==1
replace cluster = id_c if imply==0

merge m:1 year prov city county using "F:\research\dataset\county\county_panel_line.dta"
keep if _merge == 3
drop _merge 

reghdfe tem_IDW i.event##c.imply [aw=population], absorb(unique_county i.refy#i.id_t year doy) vce(cluster cluster calendar_month)

coefplot, yline(0, lp(solid) lc(cranberry)) xline(7.5, lp(dash) lc(black)) baselevels omitted vert keep(*event#c.imply) xlabel(1 "-7" 2 "-6" 3 "-5" 4 "-4" 5 "-3" 6 "-2" 7 "-1" 8 "0" 9 "1" 10 "2" 11 "3" 12 "4" 13 "5" 14 "6" 15 "7", labsize(medsmall)) ylabel (, nogrid) ciopt(lcolor(black)) mcolor(black)  ///
    xtitle("Days Relative to Cloud Seeding Day") scheme(stcolor)	
graph export "F:\dropbox\Dropbox\Predoc_Project\Cloud_Seeding\output\draft\psm_temperature_weightedby_population.png", replace





