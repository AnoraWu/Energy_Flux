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
	global dir "/Users/anora/Library/CloudStorage/Dropbox-TeamMG/Wanru Wu/Cloudseeding/Cloud Seeding"
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
global final "/Users/anora/Library/CloudStorage/Dropbox-TeamMG/Wanru Wu/Cloudseeding_Anora/SSF/final/match_y"
global output "/Users/anora/Library/CloudStorage/Dropbox-TeamMG/Wanru Wu/Cloudseeding_Anora/SSF/output/match_y"



*** (1) Matching using integer value of rainfall and forecast  ========================
**** data preparation 
use "$raw_data/skeleton_merged2024_ssf.dta", clear

* drop unused variables
drop ice_mean liquid_mean air_fraction_mean evp pressure SSH 
drop tem_avg tem_max tem_min win_max win_inst_max 
drop 天气情况 pre_ch alert alertn published_time operation_type 
drop aqi pm25 lat lon smci wind_2min tem_0cm inversion

**** construct residuals for flux variables
// replace sw_toa_flux_up = . if sw_toa_flux_up == 0
// replace sw_radiance_up = . if sw_radiance_up == 0

* draw scatter plots to show relationship of zenith and flux variable
// preserve
// generate random = runiform()
// keep if random <= 0.001
// foreach var in sw_toa_flux_up net_sw_surface_flux sw_radiance_up {
// 	local vlabel : variable label `var'
// 	scatter `var' solar_zenith, title("relationship of zenith and `var'") xtitle("solar zenith") ytitle("`vlabel'")
// 	graph export "$output/zenith_`var'.png", as(png) replace
// }
// restore


* set panel data structure for later creating lagged variables
drop date
gen date = mdy(month, day, year)
tsset dt_adcode date

forval i = 1/7 {
	gen mars_pre`i' = int(l`i'.pre_mars)
}

forval i = 1/2 {
	gen mars_late`i' = int(1*f`i'.pre_mars)
}

gen rain_pre1 = int(1*l1.rain_IDW)		
replace pre_mars = int(1*pre_mars)

forval i = 1/7 {
	gen thickness_`i' = round(l`i'.thickness, 5)
}

forval i = 1/7 {
	gen fraction_`i' = round(l`i'.fraction, 0.1)
}



local varlist thickness fraction

foreach var of local varlist {
	
	preserve

    di "Running matching pipeline for variable: `var'"
	
	
	keep mars_pre7 mars_pre6 mars_pre5 mars_pre4 mars_pre3 mars_pre2 mars_pre1 pre_mars mars_late1 mars_late2 rain_pre1 `var'* dt_adcode ct_adcode pr_adcode year month day imply 
	merge 1:1 dt_adcode ct_adcode pr_adcode year month day using "$final/pscore_no`var'.dta"
	
	drop _merge
	drop if pscore ==. // delete those useless observations 
	gen id_t = _n if imply == 1
	gen id_c = _n if imply == 0

	save "$final/int_forecast_rain_temperature_01.dta", replace
	
	
	
	******** Treated group 
	keep if imply == 1

	drop id_c 
	rename pscore pscore_t

	drop if mars_pre7==. | mars_pre6==.|mars_pre5==.|mars_pre4==.|mars_pre3==.| mars_pre2==.| mars_pre1==.| pre_mars==.| mars_late1==.| mars_late2==. 
	drop if rain_pre1 == . 
	drop if `var'_1==. | `var'_2==. | `var'_3==. | `var'_4==. | `var'_5==. | `var'_6==. | `var'_7==.
	drop if mars_pre7==0 & mars_pre6==0 & mars_pre5==0 & mars_pre4==0 & mars_pre3 ==0 & mars_pre2 ==0 & mars_pre1==0 & mars_late1==0 &mars_late2==0 & pre_mars==0 & rain_pre1==0 & ///
	`var'_1==0 & `var'_2==0 & `var'_3==0 & `var'_4==0 & `var'_5==0 & `var'_6==0 & `var'_7==0 
	

	* Drop treatments that occur within 14 days of the first cloud seeding event at a given location
	gen date = mdy(month, day, year)
	gen drop_flag = 0
	bysort dt_adcode ct_adcode pr_adcode (date): replace drop_flag = 1 if _n > 1 & date - date[_n-1] <= 14	
	drop if drop_flag == 1 
	drop drop_flag

	save "$final/treated_no0.dta", replace
	
	
	
	******** Control group
	use "$final/int_forecast_rain_temperature_01.dta"
	drop if imply == 1

	drop id_t
	rename pscore pscore_c

	drop if mars_pre7==. | mars_pre6==.|mars_pre5==.|mars_pre4==.|mars_pre3==.| mars_pre2==.| mars_pre1==.| pre_mars==.| mars_late1==.| mars_late2==. 
	drop if rain_pre1 ==. 
	drop if `var'_1==. | `var'_2==. | `var'_3==. | `var'_4==. | `var'_5==. | `var'_6==. | `var'_7==.
	drop if mars_pre7==0 & mars_pre6==0 & mars_pre5==0 & mars_pre4==0 & mars_pre3 ==0 & mars_pre2 ==0 & mars_pre1==0 &mars_late1==0 &mars_late2==0 & pre_mars==0 & rain_pre1==0 & ///
	`var'_1==0 & `var'_2==0 & `var'_3==0 & `var'_4==0 & `var'_5==0 & `var'_6==0 & `var'_7==0 
	
	ds `var'_*
	local myvars `r(varlist)'

	egen control_id = group(mars_pre7 mars_pre6 mars_pre5 mars_pre4 mars_pre3 mars_pre2 mars_pre1 pre_mars mars_late1 mars_late2 rain_pre1 `myvars')
	save "$final/control_no0_all.dta", replace
	
	
	
	******* exact matching 
	drop dt_adcode ct_adcode pr_adcode year month day imply id_c pscore_c
	duplicates drop control_id, force

	* 只保留 treated 里面能找到对于 control 的, control 里面也只保留了能跟 treated match 的
	ds `var'_*
	local myvars `r(varlist)'
	
	joinby mars_pre7 mars_pre6 mars_pre5 mars_pre4 mars_pre3 mars_pre2 mars_pre1 pre_mars mars_late1 mars_late2 rain_pre1 `myvars' using "$final/treated_no0.dta"
	keep control_id id_t pscore_t imply 

	* treated 里面能找到对于 control 的,id_t, 以及对应的 match 上的 control_id
	save "$final/matchid_no0.dta", replace
	
	
	
	****only keep treatments matched with controls 
	keep id_t
	*  把 treated 里面跟 control match 不上的 observation 去掉
	merge 1:1 id_t using "$final/treated_no0.dta"
	keep if _merge==3 
	drop _merge

	foreach temp_var of varlist mars_pre1-mars_late2{
		drop `temp_var'
	}
	drop pre_mars date rain_pre1 `var'*

	* 只剩下 id_t year month day dt_adcode ct_adcode pr_adcode city_level county_level imply pscore_t
	save "$final/treated_no0_matched.dta", replace 

	
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
		save "$final/match_no0/match_`i'.dta", replace
		
		keep id_c id_t 
		merge 1:1 id_t using "$final/treated_no0_matched.dta", keep(match)
		drop _merge pscore_t 
		
		append using "$final/match_no0/match_`i'.dta"
		
		sort id_t imply
		replace pscore_d = pscore_d[_n-1] if pscore_d==.
		tab imply
		save "$final/match_no0/match_`i'.dta", replace
	}
	
	
	** Combine matched data and find the best psm ones. 
	clear
	cd "$final/match_no0"
	openall

	* 把 10 个分块 append 在一起了，重新 filter 最小的 pscore_d
	sort id_t imply pscore_d
	by id_t imply: keep if _n ==1
	tab imply

	save "$final/psm_no0_matched.dta", replace


	** Expand the dataset to include a time window from -7 to +10 days relative to the cloud seeding event
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
	drop _merge

	save "$final/psm_10days.dta", replace


	*** event study variables
	use "$final/psm_10days.dta", clear
	gen event = refy+7
	fvset base 6 event

	egen unique_county=group(dt_adcode id_t)

	egen doy=group(month day)

	egen calendar_month=group(year month)

	gen cluster=.
	replace cluster = id_t if imply==1
	replace cluster = id_c if imply==0
	
	reghdfe `var' i.event##c.imply, absorb(unique_county i.refy#i.id_t year doy) vce(cluster cluster calendar_month)

	coefplot, yline(0, lp(solid) lc(cranberry)) xline(7.5, lp(dash) lc(black)) ///
		baselevels omitted vert keep(*event#c.imply) ///
		xlabel(1 "-7" 2 "-6" 3 "-5" 4 "-4" 5 "-3" 6 "-2" 7 "-1" 8 "0" 9 "1" 10 "2" 11 "3" 12 "4" 13 "5" 14 "6" 15 "7" 16 "8" 17 "9" 18 "10", labsize(medsmall)) ///
		ylabel(, nogrid) ciopt(lcolor(black)) mcolor(black) ///
		xtitle("Days Relative to Cloud Seeding Day") ///
		ytitle("`: variable label `var''")  ///
		title("Effect of Cloud Seeding on `: variable label `var''", size(medsmall)) 

	graph export "$output/psm_`var'_match_y.png", width(2200) height(1600) replace 
	
	
	* histograms
	foreach x of numlist 0/3 7 10 {
		
		use "$final/psm_10days.dta", clear
		gen event = refy+7
		
		* define post group
		local cut = `x' + 7
		drop if event > `cut'
		
		gen post = event >= 7
		label var post "Post period (event>=0)"
		label define postlbl 0 "Pre" 1 "Post"
		label values post postlbl

		* define treated group
		label define trtlbl 0 "Control" 1 "Treated"
		label values imply trtlbl

		hist `var' if imply==0 & post==0, percent width(0.2)  ///
			title("Control - Pre") name(g1, replace)

		hist `var' if imply==0 & post==1, percent width(0.2)  ///
			title("Control - Post") name(g2, replace)

		hist `var' if imply==1 & post==0, percent width(0.2)  ///
			title("Treated - Pre") name(g3, replace)

		hist `var' if imply==1 & post==1, percent width(0.2)  ///
			title("Treated - Post") name(g4, replace)

		graph combine g1 g2 g3 g4, col(2) title("Distribution of `: variable label `var''") ///
		    note("Post includes post `x' days after treatment.", position(7))
		graph export "/Users/anora/Library/CloudStorage/Dropbox-TeamMG/Wanru Wu/Cloudseeding_Anora/SSF/output/histograms/psm_histogram_`var'_`x'.png", width(2200) height(1600) replace 
  	}


	restore
}
	


