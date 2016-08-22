%LET _CLIENTTASKLABEL='Compile beneficiary data from the after period';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='\\ccwdata.org\Profiles\fku838\Documents\Projects\cisat\cisat.egp';
%LET _CLIENTPROJECTNAME='cisat.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;
/* If debug_mode=1, only a small subset of the beneficiaries will be processed.
  This is useful for debugging, because the code runs very fast. */
%let debug_mode = 0;
%let debug_threshold = 20000;

%let proj_cn = CISAT;
%let userlib = FKU838SL;
%let sharedlib = SH026250;

%let outlier_claims_thrsh = 0.0001; /* 0.01th percentile, not to be confused with 1th percentile. */
%let death_outcome_obs_end = %str(MDY(6, 30, 2015));

%let medpar_vars = TRNSPLNT RDLGY_DGNSTC RDLGY_THRPTC RDLGY_NUCLR_MDCN RDLGY_CT_SCAN RDLGY_OTHR_IMGNG
	OP_O_SRVC OP_A_SRVC OP_OA_SRVC ORGN_ACQSTN DGNS_CD SRGCL_PRCDR SRGCL_PRCDR_COUNT PHRMCY_ONE PHRMCY_NOT_ONE
	OBSRVTN	CRNRY_MYOCD CRNRY_PULMN CRNRY_HATRN CRNRY_INTRM CRNRY_OTHER ICU_GENRL ICU_SURGC ICU_MEDCL ICU_PEDTR
	ICU_PSYTR ICU_INTRM ICU_BURNC ICU_TRAUM ICU_OTHER INTNSV_CARE_DAY CRNRY_CARE_DAY INTNSV_CARE_CHRG
	CRNRY_CARE_CHRG OTHR_SRVC_CHRG PHRMCY_CHRG LAB_CHRG RDLGY_CHRG MRI_CHRG OP_SRVC_CHRG ER_CHRG
	PROFNL_FEES_CHRG ORGN_ACQSTN_CHRG ESRD_REV_SETG_CHRG CLNC_VISIT_CHRG POA_DGNS_CD DGNS_E_CD
	INFRMTL_ENCTR_IND TOT_CHRG;

%let code_super_groups = _00_00 _01_05 _06_07 _08_16 _17_17 _18_20 _21_29 _30_34 _35_39 _40_41
	_42_54 _55_59 _60_64 _65_71 _72_75 _76_84 _85_86 _87_99 _X;
%let code_super_groups_prefix = ICD9L3;

%let itv_cohort_tbl = &sharedlib..&proj_cn._ITV_CHRT;
%let ctr_cohort_tbl = &sharedlib..&proj_cn._CTR_CHRT;
%let itv_outcome_tbl_suffix = ITV_OUT;
%let ctr_outcome_tbl_suffix = CTR_OUT;
%let claim_tables = INPATIENT SNF OUTPATIENT BCARRIER;
%let year_list = 10 11 13 14;


%macro extract_beneficiary_cohorts(intervention, control);
proc sql;
create table &intervention as
select BENE_ID
from &itv_cohort_tbl
%if &debug_mode %then where BENE_ID < &debug_threshold;;

create unique index BENE_ID
on &intervention (BENE_ID);

select "&intervention" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &intervention;
quit;

proc sql;
create table &control as
select BENE_ID
from &ctr_cohort_tbl
%if &debug_mode %then where BENE_ID < &debug_threshold;;

create unique index BENE_ID
on &control (BENE_ID);

select "&control" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &control;
quit;
%mend;


%macro retrieve_outcome_claims(claim_table, cohort_table, output_table);
%if &claim_table = BCARRIER
	%then %let tot_chrg_var = CLM_PMT_AMT;
	%else %let tot_chrg_var = CLM_TOT_CHRG_AMT;

/* # Determine what is the value that corresponds to the outlier threshold. */
/* Determine the total number of rows and therefore the percentile of outliers. */
proc sql;
select round(sum(CLAIMS)*&outlier_claims_thrsh)
	as "&claim_table outl. clms."n into: outlier_claims
from (%do tn=1 %to 12;
	%if &tn>1 %then union all;
	select count(*) as CLAIMS
	from RIF%eval(2000+&year).&claim_table._CLAIMS_%sysfunc(putn(&tn, z2)) a
	inner join &cohort_table b
	on a.BENE_ID = b.BENE_ID
	%if &debug_mode %then where a.BENE_ID < &debug_threshold;
%end;);
quit;

/* Pick the *potential* outliers from each month of the RIF tables.
 By "potential outliers" I mean the top group of claims (according to
 the variable used to identify outliers) from each month. This group
 can potentially contain 100% of the actual outliers, but most likely
 the true top-ranking outliers will come from different months.
 Therefore, we order them by the variable used to identify outliers.
 This ordering will later on allow the identification of the real
 outliers from the list of potential outliers. */
%do tn=1 %to 12;
	proc sql outobs=&outlier_claims;
	create table ROC_A_&tn as
	select &tot_chrg_var
	from RIF%eval(2000+&year).&claim_table._CLAIMS_%sysfunc(putn(&tn, z2)) a
	inner join &cohort_table b
	on a.BENE_ID = b.BENE_ID
	%if &debug_mode %then where a.BENE_ID < &debug_threshold;
	order by &tot_chrg_var desc;
	quit;
%end;

/* Combine the potential outliers into one table limited again to the
 outlier threshold. The minimum value in this table will be the actual
 outlier threshold according to the variable used to determine outliers. */
proc sql outobs=&outlier_claims;
create table ROC_B as
select * from
(%do tn=1 %to 12;
	%if &tn > 1 %then union all;
	select * from ROC_A_&tn
%end;)
order by &tot_chrg_var desc;
quit;

/* Select the smallest value that is contained in the outlier threshold. */
proc sql;
/* ...but first drop the temporary tables created. */
%do tn=1 %to 12;
	drop table ROC_A_&tn;
%end;

select min(&tot_chrg_var)
	as "&claim_table outl. chrg."n into: outlier_tot_chrg
from ROC_B;

drop table ROC_B;
quit;

/* # Finally, retrieve the data, grouping by beneficiary, ignoring outlier claims. */
proc sql;
create table &output_table as
select BENE_ID, sum(A_CLAIMS) as CLAIMS, sum(A_TOT_CHRG) as TOT_CHRG
from (%do tn=1 %to 12;
	%if &tn>1 %then union all;
	select a.BENE_ID, count(*) as A_CLAIMS, sum(&tot_chrg_var) as A_TOT_CHRG
	from RIF%eval(2000+&year).&claim_table._CLAIMS_%sysfunc(putn(&tn, z2)) a
	inner join &cohort_table b
	on a.BENE_ID = b.BENE_ID
	where &tot_chrg_var <= &outlier_tot_chrg /* Value must not be an outlier. */
	%if &debug_mode %then and a.BENE_ID < &debug_threshold;
	group by a.BENE_ID
%end;)
group by BENE_ID;

create unique index BENE_ID
on &output_table (BENE_ID);

select "&output_table" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output_table;
quit;
%mend;


%macro retrieve_outcome_medpar(cohort_table, output_table);
/* Determine what are the outliers, using the same technique as in
 the macro %retrieve_outcome_claims. */
proc sql;
select round(count(*)*&outlier_claims_thrsh)
	as "MedPAR outl. clms"n into: outlier_claims
from MEDPAR.MEDPAR_%eval(2000+&year)
%if &debug_mode %then where BENE_ID < &debug_threshold;;
quit;

proc sql outobs=&outlier_claims;
create table ROM_A as
select TOT_CHRG_AMT
from MEDPAR.MEDPAR_%eval(2000+&year)
%if &debug_mode %then where BENE_ID < &debug_threshold;
order by TOT_CHRG_AMT desc;
quit;

proc sql;
select min(TOT_CHRG_AMT)
	as "MedPAR outl. chrg."n into: outlier_tot_chrg
from ROM_A;

drop table ROM_A;
quit;

/* Retrieve the data, grouped by beneficiary. */
proc sql;
create table &output_table as
select a.BENE_ID,
	sum(case when TRNSPLNT_IND_CD <> '0' then 1 else 0 end) as TRNSPLNT,
	sum(case when RDLGY_ONCLGY_IND_SW = '1' then 1 else 0 end) as RDLGY_DGNSTC, 
	sum(case when RDLGY_THRPTC_IND_SW = '1' then 1 else 0 end) as RDLGY_THRPTC,
	sum(case when RDLGY_NUCLR_MDCN_IND_SW = '1' then 1 else 0 end) as RDLGY_NUCLR_MDCN, 
	sum(case when RDLGY_CT_SCAN_IND_SW = '1' then 1 else 0 end) as RDLGY_CT_SCAN,
	sum(case when RDLGY_OTHR_IMGNG_IND_SW = '1' then 1 else 0 end) as RDLGY_OTHR_IMGNG, 
	sum(case when OP_SRVC_IND_CD = '1' then 1 else 0 end) as OP_O_SRVC,
	sum(case when OP_SRVC_IND_CD = '2' then 1 else 0 end) as OP_A_SRVC,
	sum(case when OP_SRVC_IND_CD = '3' then 1 else 0 end) as OP_OA_SRVC,
	sum(case when ORGN_ACQSTN_IND_CD is null then 0 else 1 end) as ORGN_ACQSTN,
	sum(DGNS_CD_CNT) as DGNS_CD,
	sum(case when SRGCL_PRCDR_IND_SW = '1' then 1 else 0 end) as SRGCL_PRCDR,
	sum(SRGCL_PRCDR_CD_CNT) as SRGCL_PRCDR_COUNT,
	sum(case when PHRMCY_IND_CD = '1' then 1 else 0 end) as PHRMCY_ONE,
	sum(case when PHRMCY_IND_CD <> '1' and PHRMCY_IND_CD <> '0' then 1 else 0 end) as PHRMCY_NOT_ONE,
	sum(case when OBSRVTN_SW = 'Y' then 1 else 0 end) as OBSRVTN,
	sum(case when CRNRY_CARE_IND_CD = '1' then 1 else 0 end) as CRNRY_MYOCD,
	sum(case when CRNRY_CARE_IND_CD = '2' then 1 else 0 end) as CRNRY_PULMN,
	sum(case when CRNRY_CARE_IND_CD = '3' then 1 else 0 end) as CRNRY_HATRN,
	sum(case when CRNRY_CARE_IND_CD = '4' then 1 else 0 end) as CRNRY_INTRM,
	sum(case when CRNRY_CARE_IND_CD = '9' then 1 else 0 end) as CRNRY_OTHER,
	sum(case when ICU_IND_CD = '0' then 1 else 0 end) as ICU_GENRL,
	sum(case when ICU_IND_CD = '1' then 1 else 0 end) as ICU_SURGC,
	sum(case when ICU_IND_CD = '2' then 1 else 0 end) as ICU_MEDCL,
	sum(case when ICU_IND_CD = '3' then 1 else 0 end) as ICU_PEDTR,
	sum(case when ICU_IND_CD = '4' then 1 else 0 end) as ICU_PSYTR,
	sum(case when ICU_IND_CD = '6' then 1 else 0 end) as ICU_INTRM,
	sum(case when ICU_IND_CD = '7' then 1 else 0 end) as ICU_BURNC,
	sum(case when ICU_IND_CD = '8' then 1 else 0 end) as ICU_TRAUM,
	sum(case when ICU_IND_CD = '9' then 1 else 0 end) as ICU_OTHER,
	sum(INTNSV_CARE_DAY_CNT) as INTNSV_CARE_DAY,
	sum(CRNRY_CARE_DAY_CNT) as CRNRY_CARE_DAY,
	sum(INTNSV_CARE_CHRG_AMT) as INTNSV_CARE_CHRG,
	sum(CRNRY_CARE_CHRG_AMT) as CRNRY_CARE_CHRG,
	sum(OTHR_SRVC_CHRG_AMT) as OTHR_SRVC_CHRG,
	sum(PHRMCY_CHRG_AMT) as PHRMCY_CHRG,
	sum(LAB_CHRG_AMT) as LAB_CHRG,
	sum(RDLGY_CHRG_AMT) as RDLGY_CHRG, 
	sum(MRI_CHRG_AMT) as MRI_CHRG,
	sum(OP_SRVC_CHRG_AMT) as OP_SRVC_CHRG,
	sum(ER_CHRG_AMT) as ER_CHRG,
	sum(PROFNL_FEES_CHRG_AMT) as PROFNL_FEES_CHRG,
	sum(ORGN_ACQSTN_CHRG_AMT) as ORGN_ACQSTN_CHRG,
	sum(ESRD_REV_SETG_CHRG_AMT) as ESRD_REV_SETG_CHRG,
	sum(CLNC_VISIT_CHRG_AMT) as CLNC_VISIT_CHRG,
	sum(POA_DGNS_CD_CNT) as POA_DGNS_CD,
	sum(DGNS_E_CD_CNT) as DGNS_E_CD,
	sum(case when INFRMTL_ENCTR_IND_SW = 'Y' then 1 else 0 end) as INFRMTL_ENCTR_IND,
	sum(TOT_CHRG_AMT) as TOT_CHRG
from MEDPAR.MEDPAR_%eval(2000+&year) a
inner join &cohort_table b
on a.BENE_ID = b.BENE_ID
where TOT_CHRG_AMT <= &outlier_tot_chrg
%if &debug_mode %then and a.BENE_ID < &debug_threshold;
group by a.BENE_ID;

create unique index BENE_ID
on &output_table (BENE_ID);

select "&output_table" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output_table;
quit;
%mend;


%macro retrieve_icd_procedures(claims_table, prefix, cohort, output);
%let case_code_super_group = %str(case
	when CODE_GROUP <= 0 then '_00_00'
	when CODE_GROUP <= 5 then '_01_05'
	when CODE_GROUP <= 7 then '_06_07'
	when CODE_GROUP <= 16 then '_08_16'
	when CODE_GROUP <= 17 then '_17_17'
	when CODE_GROUP <= 20 then '_18_20'
	when CODE_GROUP <= 29 then '_21_29'
	when CODE_GROUP <= 34 then '_30_34'
	when CODE_GROUP <= 39 then '_35_39'
	when CODE_GROUP <= 41 then '_40_41'
	when CODE_GROUP <= 54 then '_42_54'
	when CODE_GROUP <= 59 then '_55_59'
	when CODE_GROUP <= 64 then '_60_64'
	when CODE_GROUP <= 71 then '_65_71'
	when CODE_GROUP <= 75 then '_72_75'
	when CODE_GROUP <= 84 then '_76_84'
	when CODE_GROUP <= 86 then '_85_86'
	when CODE_GROUP <= 99 then '_85_86'
	else '_X' end);

/* SQL has a hard limit on 256 tables per query, so we need to slice the computation. */
/* The slice starts and ends must be in the same order in their macro variables. */
%let slice_starts = 1 4 7 10; /* 1 must match 3, 4 must match 6, and so on. */
%let slice_ends = 3 6 9 12;
%let sql_slices = %sysfunc(countw(&slice_starts));

%do s=1 %to %sysfunc(countw(&sql_slices));
	proc sql;
	create table RIP_&s as
	select BENE_ID, &case_code_super_group as CODE_SUPER_GROUP, sum(A_ROWS) as ROWS
	from (select BENE_ID, CODE_GROUP, sum(B_ROWS) as A_ROWS
		from (%do t=%sysfunc(scan(&slice_starts, &s)) %to %sysfunc(scan(&slice_ends, &s));
			%if &t > %sysfunc(scan(&slice_starts, &s)) %then union all;
			select BENE_ID, CODE_GROUP, sum(C_ROWS) as B_ROWS
			from (%do c=1 %to 25;
				%if &c > 1 %then union all;
				select a.BENE_ID, INPUT(SUBSTR(ICD_PRCDR_CD&c, 1, 2), z.) as CODE_GROUP,
					count(*) as C_ROWS
				from RIF%eval(2000+&year).&claims_table._CLAIMS_%sysfunc(putn(&t, z2)) a
				inner join &cohort b on a.BENE_ID = b.BENE_ID
				where ICD_PRCDR_CD&c is not null
				%if &debug_mode %then and a.BENE_ID < &debug_threshold;
				group by a.BENE_ID, CODE_GROUP
				%end;)
			group by BENE_ID, CODE_GROUP
			%end;)
		group by BENE_ID, CODE_GROUP)
	group by BENE_ID, CODE_SUPER_GROUP;
	quit;
%end;

proc sql;
create table RIP_C as
select BENE_ID, CODE_SUPER_GROUP, sum(ROWS) as ROWS
from
(%do s=1 %to %sysfunc(countw(&sql_slices));
	%if &s > 1 %then union all;
	select * from RIP_&s
%end;)
group by BENE_ID, CODE_SUPER_GROUP;

%do s=1 %to %sysfunc(countw(&sql_slices));
drop table RIP_&s;
%end;

/* Make sure all columns will exist when the table gets transposed. */
%do csg=1 %to %sysfunc(countw(&code_super_groups));
insert into RIP_C (BENE_ID, CODE_SUPER_GROUP, ROWS)
values (0, "%sysfunc(scan(&code_super_groups, &csg))", 0);
%end;
quit;

proc sort data=RIP_C;
	by bene_id;
run;

proc transpose data=RIP_C
	out=&output
	name=transposed_column
	prefix=&prefix;
	by BENE_ID;
	id CODE_SUPER_GROUP;
run;

proc sql;
alter table &output
drop column transposed_column;

drop table RIP_C;
quit;
%mend;


%macro add_icd_procedures(claims_table, prefix, input, output);

%retrieve_icd_procedures(&claims_table, &prefix.&code_super_groups_prefix, &input, AIP_A);

proc sql;
create table AIP_B as
select a.*
%do v=1 %to %sysfunc(countw(&code_super_groups));
	, &prefix.&code_super_groups_prefix.%sysfunc(scan(&code_super_groups, &v))
		as &flag.&prefix.&code_super_groups_prefix.%sysfunc(scan(&code_super_groups, &v))
%end;
from &input a left join AIP_A b
on a.BENE_ID = b.BENE_ID
%if &debug_mode %then where a.BENE_ID < &debug_threshold;;

drop table AIP_A;
quit;

data &output;
	set AIP_B;
	%do v=1 %to %sysfunc(countw(&code_super_groups));
		%let var = &flag.&prefix.&code_super_groups_prefix.%sysfunc(scan(&code_super_groups, &v));
		if &var=. then &var = 0;
	%end;
run;

proc sql;
drop table AIP_B;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output;
quit;

proc sql;
create table &output._SV as
select "&output._SV" as TABLE
	%do v=1 %to %sysfunc(countw(&code_super_groups));
		%let var = &flag.&prefix.&code_super_groups_prefix.%sysfunc(scan(&code_super_groups, &v));
		, sum(&var) as SUM_&var
	%end;
from &output;
quit;
%mend;


%macro add_outcome_mbsf(input, output);
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
%let primary_payer_vars = ACUTE_PRMRY_PMT ANES_PRMRY_PMT ASC_PRMRY_PMT DIALYS_PRMRY_PMT DME_PRMRY_PMT
	EM_PRMRY_PMT HH_PRMRY_PMT HOP_PRMRY_PMT HOS_PRMRY_PMT IMG_PRMRY_PMT OIP_PRMRY_PMT OPROC_PRMRY_PMT
	OTHC_PRMRY_PMT PHYS_PRMRY_PMT PTB_DRUG_PRMRY_PMT SNF_PRMRY_PMT TEST_PRMRY_PMT;
*/ 
%let beneficiary_pmt_vars = ACUTE_BENE_PMT ANES_BENE_PMT ASC_BENE_PMT DIALYS_BENE_PMT DME_BENE_PMT EM_BENE_PMT
	HOP_BENE_PMT IMG_BENE_PMT OIP_BENE_PMT OPROC_BENE_PMT OTHC_BENE_PMT PHYS_BENE_PMT PTB_DRUG_BENE_PMT
	SNF_BENE_PMT TEST_BENE_PMT PTD_BENE_PMT;

/* Outlier exclusion was deactivated for the Master Beneficiary Summary File because that
  file contains beneficiaries, not claims, therefore removing outliers would require
  removing entire beneficiaries. */
/*
proc sql;
select round(count(*)*&outlier_claims_thrsh)
	into: outlier_claims
from from &input a
left join BENE_CC.MBSF_CU_%eval(2000+&year) d on a.BENE_ID = d.BENE_ID;
quit;

proc sql outobs=&outlier_claims;
create table ROMB_A as
select
	%do VA=1 %to %sysfunc(countw(&medicare_pmt_vars));
		%let var = %scan(&medicare_pmt_vars, &VA);
		%if &VA > 1 %then +;
		coalesce(&var, 0)
	%end;
	%do VA=1 %to %sysfunc(countw(&beneficiary_pmt_vars));
		%let var = %scan(&beneficiary_pmt_vars, &VA);
		+ coalesce(&var, 0)
	%end;
		as TOTAL_PMTS
from &input a
left join BENE_CC.MBSF_CU_%eval(2000+&year) d on a.BENE_ID = d.BENE_ID
%if &debug_mode %then where a.BENE_ID < &debug_threshold;
order by TOTAL_PMTS desc;
quit;

proc sql;
select min(TOTAL_PMTS)
	into: outlier_tot_chrg
from ROMB_A;

drop table ROMB_A;
quit;
*/
/* Retrieve the data, grouped by beneficiary. Notice that the code below is very similar
  to the code used to retrieve data for beneficiary matching. */
proc sql;
create table AOM_A as
select a.*,
	%do VA=1 %to %sysfunc(countw(&ab_variables));
		%let var = %scan(&ab_variables, &VA);
		/* "b." is needed to disambiguate from Vital Status file. */
		b.&var as &flag.&var,
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
	%do VA=1 %to %sysfunc(countw(&medicare_pmt_vars));
		%let var = %scan(&medicare_pmt_vars, &VA);
		coalesce(&var, 0) as &flag.&var,
	%end; 
	%do VA=1 %to %sysfunc(countw(&beneficiary_pmt_vars));
		%let var = %scan(&beneficiary_pmt_vars, &VA);
		coalesce(&var, 0) as &flag.&var,
	%end; 
	coalesce(HOP_ER_VISITS, 0) + coalesce(IP_ER_VISITS, 0) as &flag.ER_VISITS,
	coalesce(ACUTE_COV_DAYS, 0) + coalesce(OIP_COV_DAYS, 0) as &flag.INPAT_DAYS,
	PLAN_CVRG_MOS_NUM as &flag.PLAN_CVRG_MOS_NUM,
	case when f.BENE_DEATH_DT is not null and f.BENE_DEATH_DT <= &death_outcome_obs_end
		then 1 else 0 end as &flag.DEATH_FLAG
from &input a
left join BENE_CC.MBSF_AB_%eval(2000+&year) b on a.BENE_ID = b.BENE_ID
left join BENE_CC.MBSF_CC_%eval(2000+&year) c on a.BENE_ID = c.BENE_ID
left join BENE_CC.MBSF_CU_%eval(2000+&year) d on a.BENE_ID = d.BENE_ID
left join BENE_CC.MBSF_D_%eval(2000+&year) e on a.BENE_ID = e.BENE_ID
left join EDB.VITAL_STATUS_W_NAMES_FILE f on a.BENE_ID = f.BENE_ID
%if &debug_mode %then where a.BENE_ID < &debug_threshold;;
quit;

data &output;
set AOM_A;
&flag.NUM_CC = %do VA=1 %to %sysfunc(countw(&cc_variables));
		%if &VA>1 %then %str(+); &flag.%scan(&cc_variables, &VA)_FLAG
	%end;;
run;

proc sql;
drop table AOM_A;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output;
quit;
%mend;

%macro add_outcome_claims(input, output);
%do t=1 %to %sysfunc(countw(&claim_tables));
	%retrieve_outcome_claims(%scan(&claim_tables, &t), &input,
		AOC_A_%sysfunc(scan(&claim_tables, &t)));
%end;

proc sql;
create table &output as
select a.*
%do t=1 %to %sysfunc(countw(&claim_tables));
	%let table = %sysfunc(scan(&claim_tables, &t));
	, coalesce(&table..CLAIMS, 0) as &flag.&table._CLAIMS,
		coalesce(&table..TOT_CHRG, 0) as &flag.&table._TOT_CHRG
%end;
from &input a
%do t=1 %to %sysfunc(countw(&claim_tables));
	%let table = %sysfunc(scan(&claim_tables, &t));
	left join AOC_A_&table &table
	on a.BENE_ID = &table..BENE_ID
%end;
%if &debug_mode %then where a.BENE_ID < &debug_threshold;;

%do t=1 %to %sysfunc(countw(&claim_tables));
	drop table AOC_A_%sysfunc(scan(&claim_tables, &t));
%end;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output;
quit;

proc sql;
create table &output._SV as
select "&output._SV" as TABLE
	%do t=1 %to %sysfunc(countw(&claim_tables));
		%let var = &flag.%sysfunc(scan(&claim_tables, &t));
		, sum(&var._CLAIMS) as SUM_&var._CLAIMS,
			sum(&var._TOT_CHRG) as SUM_&var._TOT_CHRG
	%end;
from &output;
quit;
%mend;


%macro add_outcome_medpar(input, output);
%retrieve_outcome_medpar(&input, AOM_A);

proc sql;
create table AOM_B as
select a.*
%do v=1 %to %sysfunc(countw(&medpar_vars));
	, %sysfunc(scan(&medpar_vars, &v)) as &flag.%sysfunc(scan(&medpar_vars, &v))
%end;
from &input a left join AOM_A b
on a.BENE_ID = b.BENE_ID
%if &debug_mode %then where a.BENE_ID < &debug_threshold;;

drop table AOM_A;
quit;

data &output;
	set AOM_B;
	%do VA=1 %to %sysfunc(countw(&medpar_vars));
		%let var = &flag.%scan(&medpar_vars, &VA);
		if &var=. then &var = 0;
	%end;
run;

proc sql;
drop table AOM_B;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output;
quit;

proc sql;
create table &output._SV as
select "&output._SV" as TABLE
	%do v=1 %to %sysfunc(countw(&medpar_vars));
		%let var = &flag.%sysfunc(scan(&medpar_vars, &v));
	, sum(&var) as SUM_&var
%end;
from &output;
quit;
%mend;


%macro combine_outcome_medpar(input, out1, out2, out_combined);
proc sql;
create table COM_A as
select a.*
%do v=1 %to %sysfunc(countw(&medpar_vars));
	%let var = &flag.%sysfunc(scan(&medpar_vars, &v));
	, b.&var + c.&var as &var
%end;
from &input a, &out1 b, &out2 c
where a.BENE_ID = b.BENE_ID and a.BENE_ID = c.BENE_ID
%if &debug_mode %then and a.BENE_ID < &debug_threshold;;
quit;

data &out_combined;
	set COM_A;
	%do VA=1 %to %sysfunc(countw(&medpar_vars));
		%let var = &flag.%scan(&medpar_vars, &VA);
		if &var=. then &var = 0;
	%end;
run;

proc sql;
drop table COM_A;

create unique index BENE_ID
on &out_combined (BENE_ID);

select "&out_combined" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &out_combined;
quit;

proc sql;
create table &out_combined._SV as
select "&out_combined._SV" as TABLE
	%do v=1 %to %sysfunc(countw(&medpar_vars));
		%let var = &flag.%sysfunc(scan(&medpar_vars, &v));
	, sum(&var) as SUM_&var
%end;
from &out_combined;
quit;
%mend;


%macro combine_outcome_mbsf(input, out1, out2, out_combined);
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
%let primary_payer_vars = ACUTE_PRMRY_PMT ANES_PRMRY_PMT ASC_PRMRY_PMT DIALYS_PRMRY_PMT DME_PRMRY_PMT
	EM_PRMRY_PMT HH_PRMRY_PMT HOP_PRMRY_PMT HOS_PRMRY_PMT IMG_PRMRY_PMT OIP_PRMRY_PMT OPROC_PRMRY_PMT
	OTHC_PRMRY_PMT PHYS_PRMRY_PMT PTB_DRUG_PRMRY_PMT SNF_PRMRY_PMT TEST_PRMRY_PMT;
*/ 
%let beneficiary_pmt_vars = ACUTE_BENE_PMT ANES_BENE_PMT ASC_BENE_PMT DIALYS_BENE_PMT DME_BENE_PMT EM_BENE_PMT
	HOP_BENE_PMT IMG_BENE_PMT OIP_BENE_PMT OPROC_BENE_PMT OTHC_BENE_PMT PHYS_BENE_PMT PTB_DRUG_BENE_PMT
	SNF_BENE_PMT TEST_BENE_PMT PTD_BENE_PMT;

/* Combine the outcomes from out1 and out2. */
proc sql;
create table COM_A as
select a.*,
	%do VA=1 %to %sysfunc(countw(&ab_variables));
		%let var = %scan(&ab_variables, &VA);
		/* For this group, prefer the variables from 2014 (out2), so a similar
		 approach (use the later year) is used for matching and outcomes. */
		c.&flag.&var as &flag.&var,
	%end;
	%do VA=1 %to %sysfunc(countw(&cc_variables));
		%let var = %scan(&cc_variables, &VA);
		/* For this group, assign 1 if the flag was 1 at any year. */
		case when b.&flag.&var._FLAG = 1 or c.&flag.&var._FLAG = 1
			then 1 else 0 end as &flag.&var._FLAG,
	%end;

	/* For the variables below, sum the values from both years. */
	%do VA=1 %to %sysfunc(countw(&cu_variables));
		%let var = %scan(&cu_variables, &VA);
		b.&flag.&var + c.&flag.&var as &flag.&var,
	%end;
	%do VA=1 %to %sysfunc(countw(&medicare_pmt_vars));
		%let var = %scan(&medicare_pmt_vars, &VA);
		b.&flag.&var + c.&flag.&var as &flag.&var,
	%end;
	/*
	%do VA=1 %to %sysfunc(countw(&primary_payer_vars));
		%let var = %scan(&primary_payer_vars, &VA);
		%if &VA > 1 %then +; coalesce(&var, 0)
	%end; as &flag.PRIM_PAYER_PMTS,
	*/ 
	%do VA=1 %to %sysfunc(countw(&beneficiary_pmt_vars));
		%let var = %scan(&beneficiary_pmt_vars, &VA);
		b.&flag.&var + c.&flag.&var as &flag.&var,
	%end;
	b.&flag.MEDICARE_PMTS + c.&flag.MEDICARE_PMTS as &flag.MEDICARE_PMTS,
	b.&flag.BENEFICIARY_PMTS + c.&flag.BENEFICIARY_PMTS as &flag.BENEFICIARY_PMTS,
	b.&flag.ER_VISITS + c.&flag.ER_VISITS as &flag.ER_VISITS,
	b.&flag.INPAT_DAYS + c.&flag.INPAT_DAYS as &flag.INPAT_DAYS,
	input(b.&flag.PLAN_CVRG_MOS_NUM, z2.) + input(c.&flag.PLAN_CVRG_MOS_NUM, z2.)
		as &flag.PLAN_CVRG_MOS_NUM,
	case when d.BENE_DEATH_DT is not null and d.BENE_DEATH_DT <= &death_outcome_obs_end
		then 1 else 0 end as &flag.DEATH_FLAG
from &input a, &out1 b, &out2 c, EDB.VITAL_STATUS_W_NAMES_FILE d
where a.BENE_ID = b.BENE_ID and a.BENE_ID = c.BENE_ID and a.BENE_ID = d.BENE_ID
%if &debug_mode %then and a.BENE_ID < &debug_threshold;;
quit;

/* Count the number of chronic conditions. */
data &out_combined;
set COM_A;
&flag.NUM_CC = %do VA=1 %to %sysfunc(countw(&cc_variables));
		%if &VA>1 %then %str(+); &flag.%scan(&cc_variables, &VA)_FLAG
	%end;;
run;

proc sql;
drop table COM_A;

create unique index BENE_ID
on &out_combined (BENE_ID);

select "&out_combined" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &out_combined;
quit;
%mend;


%macro combine_outcome_claims(input, out1, out2, out_combined);
proc sql;
create table &out_combined as
select a.*
%do t=1 %to %sysfunc(countw(&claim_tables));
	%let var = &flag.%sysfunc(scan(&claim_tables, &t));
	, b.&var._CLAIMS + c.&var._CLAIMS as &var._CLAIMS,
		b.&var._TOT_CHRG + c.&var._TOT_CHRG as &var._TOT_CHRG
%end;
from &input a, &out1 b, &out2 c
where a.BENE_ID = b.BENE_ID and a.BENE_ID = c.BENE_ID
%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

create unique index BENE_ID
on &out_combined (BENE_ID);

select "&out_combined" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &out_combined;
quit;

proc sql;
create table &out_combined._SV as
select "&out_combined._SV" as TABLE
	%do t=1 %to %sysfunc(countw(&claim_tables));
		%let var = &flag.%sysfunc(scan(&claim_tables, &t));
		, sum(&var._CLAIMS) as SUM_&var._CLAIMS, sum(&var._TOT_CHRG) as SUM_&var._TOT_CHRG
	%end;
from &out_combined;
quit;
%mend;


%macro combine_icd_procedures(claims_table, prefix, input, out1, out2, out_combined);
proc sql;
create table &out_combined as
select a.*
%do v=1 %to %sysfunc(countw(&code_super_groups));
	%let var = &flag.&prefix.&code_super_groups_prefix.%sysfunc(scan(&code_super_groups, &v));
	, b.&var + c.&var as &var
%end;
from &input a, &out1 b, &out2 c
where a.BENE_ID = b.BENE_ID and a.BENE_ID = c.BENE_ID
%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

create unique index BENE_ID
on &out_combined (BENE_ID);

select "&out_combined" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &out_combined;
quit;

proc sql;
create table &out_combined._SV as
select "&out_combined._SV" as TABLE
	%do v=1 %to %sysfunc(countw(&code_super_groups));
		%let var = &flag.&prefix.&code_super_groups_prefix.%sysfunc(scan(&code_super_groups, &v));
		, sum(&var) as SUM_&var
	%end;
from &out_combined;
quit;
%mend;

%macro make_tables;
/* Quick explanation of how the code works:
 1. Retrieve the beneficiaries of each cohort.
 2. For each cohorto, retrieve the data of each beneficiary, from each file, from each
    year separately.
 3. For each year, add the retrieved data to a growing table containing variables for
    different files.
 4. Combine the values from the two years of each period (before period/after period)
    into one value. The combination algorithm varies according tot he variable. */

%if &debug_mode %then
	%do;
		%let outputlib = &userlib;
		%let di = D; /* Debug mode identifier */
	%end;
%else
	%do;
		%let outputlib = &sharedlib;
		%let di =; /* Debug mode identifier */
	%end;

%let itv_cohort = &userlib..&proj_cn._ITV_BENE_IDS;
%let ctr_cohort = &userlib..&proj_cn._CTR_BENE_IDS;
%let itv_outcome_tbl = &outputlib..&proj_cn.&di._&itv_outcome_tbl_suffix;
%let ctr_outcome_tbl = &outputlib..&proj_cn.&di._&ctr_outcome_tbl_suffix;

%extract_beneficiary_cohorts(&itv_cohort, &ctr_cohort);

/* Retrieve the data from each year. */
%do Y = 1 %to %sysfunc(countw(&year_list));
	%let year = %sysfunc(scan(&year_list, &Y));
	%if &year = 10 | &year = 11
		%then %let flag = B; %else %let flag = A;

	%add_outcome_medpar(&itv_cohort, &itv_outcome_tbl._MEDP_&year);
	%add_outcome_medpar(&ctr_cohort, &ctr_outcome_tbl._MEDP_&year);
	
	%add_outcome_mbsf(&itv_cohort, &itv_outcome_tbl._MBSF_&year);
	%add_outcome_mbsf(&ctr_cohort, &ctr_outcome_tbl._MBSF_&year);

	%add_outcome_claims(&itv_cohort, &itv_outcome_tbl._CLMS_&year);
	%add_outcome_claims(&ctr_cohort, &ctr_outcome_tbl._CLMS_&year);

	%add_icd_procedures(INPATIENT, I, &itv_cohort, &itv_outcome_tbl._INP_&year);
	%add_icd_procedures(SNF, S, &itv_cohort, &itv_outcome_tbl._SNF_&year);

	%add_icd_procedures(INPATIENT, I, &ctr_cohort, &ctr_outcome_tbl._INP_&year);
	%add_icd_procedures(SNF, S, &ctr_cohort, &ctr_outcome_tbl._SNF_&year);
%end;

/* Combine years 2010-2011 and 2013-2014.
 TO DO: Organize the code below into loops. */
%let flag = B;
%combine_outcome_mbsf(&itv_cohort, &itv_outcome_tbl._MBSF_10,
	&itv_outcome_tbl._MBSF_11, &itv_outcome_tbl._MBSF_&flag);
%combine_outcome_medpar(&itv_cohort, &itv_outcome_tbl._MEDP_10,
	&itv_outcome_tbl._MEDP_11, &itv_outcome_tbl._MEDP_&flag);
%combine_outcome_claims(&itv_cohort, &itv_outcome_tbl._CLMS_10,
	&itv_outcome_tbl._CLMS_11, &itv_outcome_tbl._CLMS_&flag);
%combine_icd_procedures(INPATIENT, I, &itv_cohort, &itv_outcome_tbl._INP_10,
	&itv_outcome_tbl._INP_11, &itv_outcome_tbl._INP_&flag);
%combine_icd_procedures(SNF, S, &itv_cohort, &itv_outcome_tbl._SNF_10,
	&itv_outcome_tbl._SNF_11, &itv_outcome_tbl._SNF_&flag);

%combine_outcome_mbsf(&ctr_cohort, &ctr_outcome_tbl._MBSF_10,
	&ctr_outcome_tbl._MBSF_11, &ctr_outcome_tbl._MBSF_&flag);
%combine_outcome_medpar(&ctr_cohort, &ctr_outcome_tbl._MEDP_10,
	&ctr_outcome_tbl._MEDP_11, &ctr_outcome_tbl._MEDP_&flag);
%combine_outcome_claims(&ctr_cohort, &ctr_outcome_tbl._CLMS_10,
	&ctr_outcome_tbl._CLMS_11, &ctr_outcome_tbl._CLMS_&flag);
%combine_icd_procedures(INPATIENT, I, &ctr_cohort, &ctr_outcome_tbl._INP_10,
	&ctr_outcome_tbl._INP_11, &ctr_outcome_tbl._INP_&flag);
%combine_icd_procedures(SNF, S, &ctr_cohort, &ctr_outcome_tbl._SNF_10,
	&ctr_outcome_tbl._SNF_11, &ctr_outcome_tbl._SNF_&flag);

%let flag = A;
%combine_outcome_mbsf(&itv_cohort, &itv_outcome_tbl._MBSF_13,
	&itv_outcome_tbl._MBSF_14, &itv_outcome_tbl._MBSF_&flag);
%combine_outcome_medpar(&itv_cohort, &itv_outcome_tbl._MEDP_13,
	&itv_outcome_tbl._MEDP_14, &itv_outcome_tbl._MEDP_&flag);
%combine_outcome_claims(&itv_cohort, &itv_outcome_tbl._CLMS_13,
	&itv_outcome_tbl._CLMS_14, &itv_outcome_tbl._CLMS_&flag);
%combine_icd_procedures(INPATIENT, I, &itv_cohort, &itv_outcome_tbl._INP_13,
	&itv_outcome_tbl._INP_14, &itv_outcome_tbl._INP_&flag);
%combine_icd_procedures(SNF, S, &itv_cohort, &itv_outcome_tbl._SNF_13,
	&itv_outcome_tbl._SNF_14, &itv_outcome_tbl._SNF_&flag);

%combine_outcome_mbsf(&ctr_cohort, &ctr_outcome_tbl._MBSF_13,
	&ctr_outcome_tbl._MBSF_14, &ctr_outcome_tbl._MBSF_&flag);
%combine_outcome_medpar(&ctr_cohort, &ctr_outcome_tbl._MEDP_13,
	&ctr_outcome_tbl._MEDP_14, &ctr_outcome_tbl._MEDP_&flag);
%combine_outcome_claims(&ctr_cohort, &ctr_outcome_tbl._CLMS_13,
	&ctr_outcome_tbl._CLMS_14, &ctr_outcome_tbl._CLMS_&flag);
%combine_icd_procedures(INPATIENT, I, &ctr_cohort, &ctr_outcome_tbl._INP_13,
	&ctr_outcome_tbl._INP_14, &ctr_outcome_tbl._INP_&flag);
%combine_icd_procedures(SNF, S, &ctr_cohort, &ctr_outcome_tbl._SNF_13,
	&ctr_outcome_tbl._SNF_14, &ctr_outcome_tbl._SNF_&flag);
%mend;

%make_tables;

GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;

