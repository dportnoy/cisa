%LET _CLIENTTASKLABEL='Compile beneficiary data from the before period';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='\\ccwdata.org\Profiles\fku838\Documents\Projects\cisat\cisat.egp';
%LET _CLIENTPROJECTNAME='cisat.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;
/* If debug_mode=1, only a small subset of the beneficiaries will be processed.
  This is useful for debugging, because the code runs very fast. */
%let debug_mode = 0;
%let debug_threshold = 10000;

%let proj_cn = CISAT;
%let userlib = FKU838SL;
%let sharedlib = SH026250;

%let zcta_tbl = &userlib..&proj_cn._ZIP_TO_ZCTA;
%let census_tbl = &userlib..&proj_cn._CENSUS;

/* All variables from 2010-2011 are prefixed with "B", meaning "Before".
 All variables from 2013-2014 are prefixed with "A", meaning "After". */
%let flag = B;

%macro create_census_indexes;
proc sql;
create unique index ZIP
on &zcta_tbl (ZIP);

create index ZCTA
on &zcta_tbl (ZCTA);

create unique index ZCTA
on &census_tbl (ZCTA);
quit;
%mend;


%macro list_eligible_beneficiaries(output);
/* Notice that we first produce the counts of beneficiaries that appear in
 the paper, and then we actually produce the cohort of eligible beneficiaries.*/

/* Count all beneficiarties in years 2010, 2011, 2013 and 2014. */
proc sql;
create table LEB_A as
select a.BENE_ID from
(select BENE_ID from BENE_CC.MBSF_AB_2010
	%if &debug_mode %then where BENE_ID < &debug_threshold;) a,
(select BENE_ID from BENE_CC.MBSF_AB_2011
	%if &debug_mode %then where BENE_ID < &debug_threshold;) b,
(select BENE_ID from BENE_CC.MBSF_AB_2013
	%if &debug_mode %then where BENE_ID < &debug_threshold;) c,
(select BENE_ID from BENE_CC.MBSF_AB_2014
	%if &debug_mode %then where BENE_ID < &debug_threshold;) d
where a.BENE_ID = b.BENE_ID
	and a.BENE_ID = b.BENE_ID
	and a.BENE_ID = c.BENE_ID
	and a.BENE_ID = d.BENE_ID
	%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

select "LEB_A" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from LEB_A;

drop table LEB_A;
quit;


/* Count all beneficiarties in years 2010, 2011, 2013 and 2014 
 with full parts A and B enrollment. */
proc sql;
create table LEB_B as
select a.BENE_ID from
(select BENE_ID from BENE_CC.MBSF_AB_2010
	where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_DEATH_DT is null /* The beneficiary must be alive until the end of the year. */
	%if &debug_mode %then and BENE_ID < &debug_threshold;) a,
(select BENE_ID from BENE_CC.MBSF_AB_2011
	where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_DEATH_DT is null
	%if &debug_mode %then and BENE_ID < &debug_threshold;) b,
(select BENE_ID from BENE_CC.MBSF_AB_2013
	where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_DEATH_DT is null
	%if &debug_mode %then and BENE_ID < &debug_threshold;) c,
(select BENE_ID from BENE_CC.MBSF_AB_2014
	where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_DEATH_DT is null
	%if &debug_mode %then and BENE_ID < &debug_threshold;) d
where a.BENE_ID = b.BENE_ID
	and a.BENE_ID = b.BENE_ID
	and a.BENE_ID = c.BENE_ID
	and a.BENE_ID = d.BENE_ID
	%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

select "LEB_B" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from LEB_B;

drop table LEB_B;
quit;


/* Count all beneficiarties in years 2010, 2011, 2013 and 2014 
 with full parts A and B enrollment and zero HMO (part C) enrollment */
proc sql;
create table LEB_C as
select a.BENE_ID from
(select BENE_ID from BENE_CC.MBSF_AB_2010
	where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_DEATH_DT is null /* The beneficiary must be alive until the end of the year. */
	and BENE_HMO_CVRAGE_TOT_MONS = 0
	%if &debug_mode %then and BENE_ID < &debug_threshold;) a,
(select BENE_ID from BENE_CC.MBSF_AB_2011
	where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_DEATH_DT is null
	and BENE_HMO_CVRAGE_TOT_MONS = 0
	%if &debug_mode %then and BENE_ID < &debug_threshold;) b,
(select BENE_ID from BENE_CC.MBSF_AB_2013
	where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_DEATH_DT is null
	and BENE_HMO_CVRAGE_TOT_MONS = 0
	%if &debug_mode %then and BENE_ID < &debug_threshold;) c,
(select BENE_ID from BENE_CC.MBSF_AB_2014
	where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_DEATH_DT is null
	and BENE_HMO_CVRAGE_TOT_MONS = 0
	%if &debug_mode %then and BENE_ID < &debug_threshold;) d
where a.BENE_ID = b.BENE_ID
	and a.BENE_ID = b.BENE_ID
	and a.BENE_ID = c.BENE_ID
	and a.BENE_ID = d.BENE_ID
	%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

select "LEB_C" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n

from LEB_C;

drop table LEB_C;
quit;


/* Produce the final cohort of elible beneficiaries:
	full parts A and B enrollment,
	zero HMO (part C) enrollment,
	and no change of U.S. state of residence. */
proc sql;
create table &output as
select a.BENE_ID from
(select BENE_ID, STATE_CODE from BENE_CC.MBSF_AB_2010
	where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_DEATH_DT is null /* The beneficiary must be alive until the end of the year. */
	and BENE_HMO_CVRAGE_TOT_MONS = 0
	%if &debug_mode %then and BENE_ID < &debug_threshold;) a,
(select BENE_ID, STATE_CODE from BENE_CC.MBSF_AB_2011
	where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_DEATH_DT is null
	and BENE_HMO_CVRAGE_TOT_MONS = 0
	%if &debug_mode %then and BENE_ID < &debug_threshold;) b,
(select BENE_ID, STATE_CODE from BENE_CC.MBSF_AB_2013
	where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_DEATH_DT is null
	and BENE_HMO_CVRAGE_TOT_MONS = 0
	%if &debug_mode %then and BENE_ID < &debug_threshold;) c,
(select BENE_ID, STATE_CODE from BENE_CC.MBSF_AB_2014
	where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_DEATH_DT is null
	and BENE_HMO_CVRAGE_TOT_MONS = 0
	%if &debug_mode %then and BENE_ID < &debug_threshold;) d
where a.BENE_ID = b.BENE_ID
	and a.BENE_ID = b.BENE_ID
	and a.BENE_ID = c.BENE_ID
	and a.BENE_ID = d.BENE_ID
	and a.STATE_CODE = b.STATE_CODE
	and a.STATE_CODE = b.STATE_CODE
	and a.STATE_CODE = c.STATE_CODE
	and a.STATE_CODE = d.STATE_CODE
	%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output;
quit;
%mend;


%macro extract_aco_cohort(output);
/* Count all ACO beneficiaries in both years 2013 and 2014 */
proc sql;
create table EAC_A as
select a.BENE_ID, a.ACO_NUM
from ACO.BENEFICIARY_SSP_2013 a, ACO.BENEFICIARY_SSP_2014 b
where a.BENE_ID = b.BENE_ID
%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

create index BENE_ID
on EAC_A (BENE_ID);

select "EAC_A" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from EAC_A;
quit;


/* Count all ACO beneficiaries with full assignment during both 2013 and 2014 */
proc sql;
create table EAC_B as
select a.BENE_ID
from ACO.BENEFICIARY_SSP_2013 a, ACO.BENEFICIARY_SSP_2014 b
where a.BENE_ID = b.BENE_ID
	and a.Q4_ASSIGN = 1 and b.Q4_ASSIGN = 1
%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

create index BENE_ID
on EAC_B (BENE_ID);

select "EAC_B" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from EAC_B;
quit;


/* Extract the desired ACO cohort:
	eligible beneficiaries with full ACO assignment during both 2013 and 2014. */
proc sql;
create table EAC_C as
select a.BENE_ID, a.ACO_NUM as ACO_NUM_13, b.ACO_NUM as ACO_NUM_14
from ACO.BENEFICIARY_SSP_2013 a, ACO.BENEFICIARY_SSP_2014 b,
	&elig_beneficiaries_table c
where a.BENE_ID = b.BENE_ID
	and a.BENE_ID = c.BENE_ID
	and a.Q4_ASSIGN = 1 and b.Q4_ASSIGN = 1
%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

create index BENE_ID
on EAC_C (BENE_ID);

select "EAC_C" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from EAC_C;
quit;


/* There are 5 beneficiaries that appear more than once in SSP tables.
  Remove them as outliers. */
proc sql;
create table EAC_D as
select BENE_ID, count(*) as ROWS
from EAC_C
group by BENE_ID
having ROWS > 1;

create table &output as
select * from EAC_C
where BENE_ID not in (select BENE_ID from EAC_D);

drop table EAC_A, EAC_B, EAC_C, EAC_D;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output;
quit;
%mend;


%macro extract_non_aco_cohort(output);
/* The Non-ACO cohort is everyone eligible that was never in an SSP ACO at any time. */
proc sql;
create table &output as
select a.BENE_ID
from &elig_beneficiaries_table a
left join (select distinct BENE_ID from ACO.BENEFICIARY_SSP_2013
	union select distinct BENE_ID from ACO.BENEFICIARY_SSP_2014) b
on a.BENE_ID = b.BENE_ID
where b.BENE_ID is null
%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output;
quit;
%mend;


%macro add_match_data(input, output);
%let ab_variables = BENE_AGE_AT_END_REF_YR BENE_RACE_CD BENE_SEX_IDENT_CD BENE_ZIP_CD STATE_CODE
	BENE_ENTLMT_RSN_CURR;

%let cc_variables = AMI ALZH_DEMEN ATRIAL_FIB CATARACT CHRONICKIDNEY COPD CHF DIABETES GLAUCOMA
	HIP_FRACTURE ISCHEMICHEART DEPRESSION OSTEOPOROSIS RA_OA STROKE_TIA CANCER_BREAST CANCER_COLORECTAL
	CANCER_PROSTATE	CANCER_LUNG CANCER_ENDOMETRIAL ANEMIA ASTHMA HYPERL HYPERP HYPERT HYPOTH;

%let cu_variables = PHYS_EVENTS PTB_DRUG_EVENTS HOP_VISITS SNF_COV_DAYS HOS_COV_DAYS HH_VISITS ASC_EVENTS
	EM_EVENTS ANES_EVENTS DIALYS_EVENTS OPROC_EVENTS IMG_EVENTS TEST_EVENTS DME_EVENTS OTHC_EVENTS
	READMISSIONS PTD_FILL_CNT;

%let medicare_pmt_vars = ACUTE_MDCR_PMT ANES_MDCR_PMT ASC_MDCR_PMT DIALYS_MDCR_PMT DME_MDCR_PMT EM_MDCR_PMT
	HH_MDCR_PMT HOP_MDCR_PMT HOS_MDCR_PMT IMG_MDCR_PMT OPROC_MDCR_PMT OTHC_MDCR_PMT PHYS_MDCR_PMT
	PTB_DRUG_MDCR_PMT PTD_MDCR_PMT SNF_MDCR_PMT TEST_MDCR_PMT OIP_MDCR_PMT PTD_TOTAL_RX_CST;
/*
From exploratory analyses, the primary payer variables are zero in nearly all cases,
therefore we removed it.
%let primary_payer_vars = ACUTE_PRMRY_PMT ANES_PRMRY_PMT ASC_PRMRY_PMT DIALYS_PRMRY_PMT DME_PRMRY_PMT
	EM_PRMRY_PMT HH_PRMRY_PMT HOP_PRMRY_PMT HOS_PRMRY_PMT IMG_PRMRY_PMT OIP_PRMRY_PMT OPROC_PRMRY_PMT
	OTHC_PRMRY_PMT PHYS_PRMRY_PMT PTB_DRUG_PRMRY_PMT SNF_PRMRY_PMT TEST_PRMRY_PMT;
*/
%let beneficiary_pmt_vars = ACUTE_BENE_PMT ANES_BENE_PMT ASC_BENE_PMT DIALYS_BENE_PMT DME_BENE_PMT EM_BENE_PMT
	HOP_BENE_PMT IMG_BENE_PMT OIP_BENE_PMT OPROC_BENE_PMT OTHC_BENE_PMT PHYS_BENE_PMT PTB_DRUG_BENE_PMT
	SNF_BENE_PMT TEST_BENE_PMT PTD_BENE_PMT;

%do YR=2010 %to 2011;
	proc sql;
	create table AMD_A_&YR as
	select a.*,
		%do VA=1 %to %sysfunc(countw(&ab_variables));
			%let var = %scan(&ab_variables, &VA);
			&var as &flag.&var,
		%end;
		/* Assign binary flag of 1 for all chronic conditions that the beneficiary has. */
		%do VA=1 %to %sysfunc(countw(&cc_variables));
			%let var = %scan(&cc_variables, &VA);
			case when &var._EVER is null then 0 else 1 end as &flag.&var._FLAG,
		%end;
		%do VA=1 %to %sysfunc(countw(&cu_variables));
			%let var = %scan(&cu_variables, &VA);
			coalesce(&var, 0) as &flag.&var,
		%end;
		/* Sum all Medicare payment variables into one. */
		%do VA=1 %to %sysfunc(countw(&medicare_pmt_vars));
			%let var = %scan(&medicare_pmt_vars, &VA);
			%if &VA > 1 %then +; coalesce(&var, 0)
		%end; as &flag.MEDICARE_PMTS,
		/*
		%do VA=1 %to %sysfunc(countw(&primary_payer_vars));
			%let var = %scan(&primary_payer_vars, &VA);
			%if &VA > 1 %then +; coalesce(&var, 0)
		%end; as &flag.PRIM_PAYER_PMTS,
		*/
		/* Sum all beneficiary payment variables into one. */
		%do VA=1 %to %sysfunc(countw(&beneficiary_pmt_vars));
			%let var = %scan(&beneficiary_pmt_vars, &VA);
			%if &VA > 1 %then +; coalesce(&var, 0)
		%end; as &flag.BENEFICIARY_PMTS,
		/* Sum the emergency room visits */
		coalesce(HOP_ER_VISITS, 0) + coalesce(IP_ER_VISITS, 0) as &flag.ER_VISITS,
		/* Sum the inpatient days */
		coalesce(ACUTE_COV_DAYS, 0) + coalesce(OIP_COV_DAYS, 0) as &flag.INPAT_DAYS,
		PLAN_CVRG_MOS_NUM as &flag.PLAN_CVRG_MOS_NUM
	from &input a
	inner join BENE_CC.MBSF_AB_&YR b on a.BENE_ID = b.BENE_ID
	inner join BENE_CC.MBSF_CC_&YR c on a.BENE_ID = c.BENE_ID
	inner join BENE_CC.MBSF_CU_&YR d on a.BENE_ID = d.BENE_ID
	inner join BENE_CC.MBSF_D_&YR e on a.BENE_ID = e.BENE_ID
	%if &debug_mode %then where a.BENE_ID < &debug_threshold;;
	quit;

	data AMD_B_&YR;
		set AMD_A_&YR;
		&flag.NUM_CC = %do VA=1 %to %sysfunc(countw(&cc_variables));
				%if &VA>1 %then %str(+); &flag.%scan(&cc_variables, &VA)_FLAG
			%end;;
	run;

	proc sql;
	drop table AMD_A_&YR;
	quit;
%end;


/* Join years 2010 and 2011.
 Notice that variables from each group of variables are joined in a different
 way. The concept is to aggregate two years into one unit of time, much
 like the Master Beneficiary Summary File aggregates 365 days into one year. */
proc sql;
create table AMD_C as
select a.BENE_ID,
	%do VA=1 %to %sysfunc(countw(&ab_variables));
		%let var = %scan(&ab_variables, &VA);
		/* For this group, prefer the variables of 2011 because, in case they changed
		 since 2010, the ones from 2011 are probably a better representation of the
		 entire study period (2010-2014). */
		b.&flag.&var as &flag.&var,
	%end;
	%do VA=1 %to %sysfunc(countw(&cc_variables));
		%let var = %scan(&cc_variables, &VA);
		/* For this group, assign 1 if the flag was 1 at any year. */
		case when a.&flag.&var._FLAG = 1 or b.&flag.&var._FLAG = 1
			then 1 else 0 end as &flag.&var._FLAG,
	%end;
	/* For the variables below, sum the values from both years. */
	%do VA=1 %to %sysfunc(countw(&cu_variables));
		%let var = %scan(&cu_variables, &VA);
		a.&flag.&var + b.&flag.&var as &flag.&var,
	%end;	
	a.&flag.MEDICARE_PMTS + b.&flag.MEDICARE_PMTS as &flag.MEDICARE_PMTS,
	a.&flag.BENEFICIARY_PMTS + b.&flag.BENEFICIARY_PMTS as &flag.BENEFICIARY_PMTS,
	a.&flag.ER_VISITS + b.&flag.ER_VISITS as &flag.ER_VISITS,
	a.&flag.INPAT_DAYS + b.&flag.INPAT_DAYS as &flag.INPAT_DAYS,
	input(a.&flag.PLAN_CVRG_MOS_NUM, z2.) + input(b.&flag.PLAN_CVRG_MOS_NUM, z2.)
		as &flag.PLAN_CVRG_MOS_NUM
from AMD_B_2010 a inner join AMD_B_2011 b
on a.BENE_ID = b.BENE_ID;
quit;

/* Count the number of chronic conditions. */
data &output;
set AMD_C;
&flag.NUM_CC = %do VA=1 %to %sysfunc(countw(&cc_variables));
	%if &VA>1 %then %str(+); &flag.%scan(&cc_variables, &VA)_FLAG
%end;;
run;

proc sql;
drop table AMD_B_2010, AMD_B_2011, AMD_C;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output;
quit;
%mend;


%macro add_zcta(input, output);
/* Add the ZIP Code Tabulation Area that matches the beneficiary ZIP code
  using the crosswalk map. */
proc sql;
create table &output as
select a.*, ZCTA as &flag.ZCTA
from &input a inner join &zcta_tbl b
on SUBSTR(a.&flag.BENE_ZIP_CD, 1, 5) = b.ZIP
%if &debug_mode %then where a.BENE_ID < &debug_threshold;;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output;

select "&output (ZIP = ZCTA)" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output
where SUBSTR(&flag.BENE_ZIP_CD, 1, 5) = &flag.ZCTA;
quit;
%mend;


%macro add_census_data(input, output);
/* Add the Census data based on the beneficiary ZCTA. */
proc sql;
create table ACD_A as
select a.*,
	HC03_EST_VC01 as &flag.ZCTA_BELOW_PL,
	HC01_EST_VC16 as &flag.ZCTA_HIGH_SCHOOL,
	HC01_EST_VC17 as &flag.ZCTA_COLLEGE
from &input a inner join &census_tbl b
on a.&flag.ZCTA = b.ZCTA
%if &debug_mode %then where a.BENE_ID < &debug_threshold;;

select "ACD_A" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from ACD_A;

/* Verify the integrity of the dataset. */
select "ACD_A (census data is null)" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from ACD_A
where &flag.ZCTA_BELOW_PL is null
	or &flag.ZCTA_HIGH_SCHOOL is null
	or &flag.ZCTA_COLLEGE is null;

create table &output as
select * from ACD_A
where &flag.ZCTA_BELOW_PL is not null
	and &flag.ZCTA_HIGH_SCHOOL is not null
	and &flag.ZCTA_COLLEGE is not null;

drop table ACD_A;

create unique index BENE_ID
on &output (BENE_ID);

select "&output (no null census data)" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output;
quit;
%mend;


%macro produce_cohorts;
%if &debug_mode %then
	%do;
		%let outputlib = &userlib;
		%let di = D; /* Debug mode identifier. */
	%end;
%else
	%do;
		%let outputlib = &sharedlib;
		%let di =; /* Debug mode identifier. */
	%end;
	
%let elig_beneficiaries_table = &userlib..&proj_cn.&di._ELIG_BENEF;
%let itv_cohort_tbl = &outputlib..&proj_cn.&di._ITV_CHRT;
%let ctr_cohort_tbl = &outputlib..&proj_cn.&di._CTR_CHRT;

%list_eligible_beneficiaries(&elig_beneficiaries_table);

/* Produce the data for the ACO cohort. */
%extract_aco_cohort(MK_A);
%add_match_data(MK_A, MK_B);
%add_zcta(MK_B, MK_C);
%add_census_data(MK_C, &itv_cohort_tbl);
proc sql;
drop table MK_A, MK_B, MK_C;
quit;

/* Produce the data for the Non-ACO cohort. */
%extract_non_aco_cohort(MK_D);
%add_match_data(MK_D, MK_E);
%add_zcta(MK_E, MK_F);
%add_census_data(MK_F, &ctr_cohort_tbl);
proc sql;
drop table MK_D, MK_E, MK_F;
quit;
%mend;

%create_census_indexes;

%produce_cohorts;


GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;

