***********************************************************************************************************
** this do file 
** data files used: 
** data files produced:
** last update: 09/14/2025
***********************************************************************************************************
clear all
set more off


* histograms
foreach x of numlist 0/3 7 10 {
	
	use "/Users/anora/Library/CloudStorage/Dropbox-TeamMG/Wanru Wu/Cloudseeding/Cloud Seeding/data/tem/match/forecast_rain/psm_10days.dta", clear
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

	hist rain_IDW if imply==0 & post==0, percent width(0.2)  ///
		title("Control - Pre") name(g1, replace)

	hist rain_IDW if imply==0 & post==1, percent width(0.2)  ///
		title("Control - Post") name(g2, replace)

	hist rain_IDW if imply==1 & post==0, percent width(0.2)  ///
		title("Treated - Pre") name(g3, replace)

	hist rain_IDW if imply==1 & post==1, percent width(0.2)  ///
		title("Treated - Post") name(g4, replace)

	graph combine g1 g2 g3 g4, col(2) title("Distribution of `: variable label rain_IDW'") ///
		note("Post includes post `x' days after treatment.", position(7))
	graph export "/Users/anora/Library/CloudStorage/Dropbox-TeamMG/Wanru Wu/Cloudseeding_Anora/SSF/output/histograms/psm_histogram_rain_IDW_`x'.png", width(2200) height(1600) replace 
}



