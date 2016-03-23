%LET _CLIENTTASKLABEL='Compile beneficiary data for matching';
%LET _CLIENTPROJECTPATH='\\ccwdata.org\Profiles\fku838\Documents\Projects\cisa\cisa.egp';
%LET _CLIENTPROJECTNAME='cisa.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;
%let debug_mode = 0;
%let debug_threshold = 10000;

%let proj_cn = CISA;
%let userlib = FKU838SL;
%let sharedlib = SH026250;
%let aco_start = %str(MDY(1, 1, 2013));

%let claim_tables = INPATIENT OUTPATIENT SNF BCARRIER;

%let eligible_beneficiaries_table = &userlib..&proj_cn._ELIG_BENEF;

%let itv_cohort_tbl = &sharedlib..&proj_cn._ITV_CHRT;
%let ctr_cohort_tbl = &sharedlib..&proj_cn._CTR_CHRT;

/* All variables from 2011 are prefixed with "B", meaning "Before".
 All variables from 2013 are prefixed with "A", meaning "After". */
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
proc sql;
create table &output as
select a.BENE_ID from
(select BENE_ID, STATE_CODE from BENE_CC.MBSF_AB_2011
where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_SMI_CVRAGE_TOT_MONS = 12
	and BENE_HMO_CVRAGE_TOT_MONS = 0
	and BENE_DEATH_DT is null /* The beneficiary needs to be alive until the end of 2013. */
	%if &debug_mode %then and BENE_ID < &debug_threshold;) a
inner join /* The beneficiary must exist in both years. */
(select BENE_ID, STATE_CODE from BENE_CC.MBSF_AB_2013
where BENE_HI_CVRAGE_TOT_MONS = 12
	and BENE_SMI_CVRAGE_TOT_MONS = 12
	and BENE_HMO_CVRAGE_TOT_MONS = 0
	and BENE_DEATH_DT is null /* The beneficiary needs to be alive until the end of 2013. */
	%if &debug_mode %then and BENE_ID < &debug_threshold;) b
on a.BENE_ID = b.BENE_ID
	and a.STATE_CODE = b.STATE_CODE;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &output;
quit;
%mend;


%macro extract_intervention_cohort(output);
proc sql;
create table EIC_A as
select a.BENE_ID, ACO_NUM
from ACO_BENE.BENEFICIARY_ACO_2013 a
inner join &eligible_beneficiaries_table b
on a.BENE_ID = b.BENE_ID
where START_DATE <= &aco_start
	and Q4_ASSIGN = 1
%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

create index BENE_ID
on EIC_A (BENE_ID);

select "EIC_A" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from EIC_A;

create table EIC_B as
select BENE_ID, count(*) as ROWS
from EIC_A
group by BENE_ID
having ROWS > 1;

/* There are 3 beneficiaries that appear more than once in the table. Remove them. */
create table &output as
select * from EIC_A
where BENE_ID not in (select BENE_ID from EIC_B);

drop table EIC_A, EIC_B;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &output;
quit;
%mend;


%macro extract_control_cohort(output);
/* The control cohort is anyone eligible that were never in an SSP ACO at any time. */
proc sql;
create table &output as
select a.BENE_ID
from &eligible_beneficiaries_table a
left join ACO_BENE.BENEFICIARY_ACO_2013 b
on a.BENE_ID = b.BENE_ID
where b.BENE_ID is null
%if &debug_mode %then where a.BENE_ID < &debug_threshold;;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
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

proc sql;
create table AMD_A as
select a.*,
	%do VA=1 %to %sysfunc(countw(&ab_variables));
		%let var = %scan(&ab_variables, &VA);
		&var as &flag.&var,
	%end;	
	%do VA=1 %to %sysfunc(countw(&cc_variables));
		%let var = %scan(&cc_variables, &VA);
		case when &var._EVER is null then 0 else 1 end as &flag.&var._FLAG,
	%end;
	%do VA=1 %to %sysfunc(countw(&cu_variables));
		%let var = %scan(&cu_variables, &VA);
		coalesce(&var, 0) as &flag.&var,
	%end;
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
	%do VA=1 %to %sysfunc(countw(&beneficiary_pmt_vars));
		%let var = %scan(&beneficiary_pmt_vars, &VA);
		%if &VA > 1 %then +; coalesce(&var, 0)
	%end; as &flag.BENEFICIARY_PMTS,
	coalesce(HOP_ER_VISITS, 0) + coalesce(IP_ER_VISITS, 0) as &flag.ER_VISITS,
	coalesce(ACUTE_COV_DAYS, 0) + coalesce(OIP_COV_DAYS, 0) as &flag.INPAT_DAYS,
	PLAN_CVRG_MOS_NUM as &flag.PLAN_CVRG_MOS_NUM
from &input a
inner join BENE_CC.MBSF_AB_2011 b on a.BENE_ID = b.BENE_ID
inner join BENE_CC.MBSF_CC_2011 c on a.BENE_ID = c.BENE_ID
inner join BENE_CC.MBSF_CU_2011 d on a.BENE_ID = d.BENE_ID
inner join BENE_CC.MBSF_D_2011 e on a.BENE_ID = e.BENE_ID
%if &debug_mode %then where a.BENE_ID < &debug_threshold;;
quit;

data &output;
	set AMD_A;
	NUM_CC = %do VA=1 %to %sysfunc(countw(&cc_variables));
			%if &VA>1 %then %str(+); &flag.%scan(&cc_variables, &VA)_FLAG
		%end;;
run;

proc sql;
drop table AMD_A;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &output;
quit;
%mend;


%macro add_zcta(input, output);
proc sql;
create table &output as
select a.*, ZCTA as &flag.ZCTA
from &input a inner join &zcta_tbl b
on SUBSTR(a.&flag.BENE_ZIP_CD, 1, 5) = b.ZIP
%if &debug_mode %then where a.BENE_ID < &debug_threshold;;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &output;

select "&output (ZIP = ZCTA)" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &output
where SUBSTR(&flag.BENE_ZIP_CD, 1, 5) = &flag.ZCTA;
quit;
%mend;


%macro add_census_data(input, output);
proc sql;
create table ACD_A as
select a.*,
	HC02_EST_VC01*100/HC01_EST_VC01 as &flag.ZCTA_BELOW_PL2,
	/* We decided to remove High School attainment from the analysis.
	(HC01_EST_VC28+HC01_EST_VC30)*100/HC01_EST_VC01_25_PLUS as &flag.ZCTA_HIGH_SCHOOL2, */
	HC01_EST_VC30*100/HC01_EST_VC01_25_PLUS as &flag.ZCTA_COLLEGE2
from &input a inner join &census_tbl b
on a.&flag.ZCTA = b.ZCTA
%if &debug_mode %then where a.BENE_ID < &debug_threshold;;

select "ACD_A" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from ACD_A;

/* Verify the integrity of the dataset. */
select "ACD_A (census data is null)" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from ACD_A
where &flag.ZCTA_BELOW_PL is null
	/*or &flag.ZCTA_HIGH_SCHOOL is null*/
	or &flag.ZCTA_COLLEGE is null;

create table &output as
select * from ACD_A
where &flag.ZCTA_BELOW_PL is not null
	/* and &flag.ZCTA_HIGH_SCHOOL is not null */
	and &flag.ZCTA_COLLEGE is not null;

drop table ACD_A;

create unique index BENE_ID
on &output (BENE_ID);

select "&output (no null census data)" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &output;
quit;
%mend;


%macro produce_cohorts;
%create_census_indexes;

%list_eligible_beneficiaries(&eligible_beneficiaries_table);

%extract_intervention_cohort(MK_A);
%add_match_data(MK_A, MK_B);
%add_zcta(MK_B, MK_C);
%add_census_data(MK_C, &itv_cohort_tbl);
proc sql;
drop table MK_A, MK_B, MK_C;
quit;

%extract_control_cohort(MK_D);
%add_match_data(MK_D, MK_E);
%add_zcta(MK_E, MK_F);
%add_census_data(MK_F, &ctr_cohort_tbl);
proc sql;
drop table MK_D, MK_E, MK_F;
quit;
%mend;

%produce_cohorts;

GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;

