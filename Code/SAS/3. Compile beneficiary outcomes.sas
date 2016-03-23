%LET _CLIENTTASKLABEL='Compile beneficiary outcomes';
%LET _CLIENTPROJECTPATH='\\ccwdata.org\Profiles\fku838\Documents\Projects\cisa\cisa.egp';
%LET _CLIENTPROJECTNAME='cisa.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;
%let debug_mode = 1;
%let debug_threshold = 10000;

%let proj_cn = CISA;
%let userlib = FKU838SL;
%let sharedlib = SH026250;

%let death_outcome_obs_end = %str(MDY(6, 30, 2014));

%let year_list = 11 13;

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
%let itv_outcome_tbl_suffix = ITV_OUTU;
%let ctr_outcome_tbl_suffix = CTR_OUTU;
%let claim_tables = INPATIENT SNF OUTPATIENT BCARRIER;


%macro retrieve_outcome_claims(claim_table, cohort_table, output_table);
proc sql;
create table &output_table as
select BENE_ID, sum(A_CLAIMS) as CLAIMS, sum(A_TOT_CHRG) as TOT_CHRG
from (%do tn=1 %to 12;
	%if &tn>1 %then union all;
	select a.BENE_ID, count(*) as A_CLAIMS,
		sum(%if &claim_table = BCARRIER %then CLM_PMT_AMT; %else CLM_TOT_CHRG_AMT;) as A_TOT_CHRG
	from RIF%eval(2000+&year).&claim_table._CLAIMS_%sysfunc(putn(&tn, z2)) a
	inner join &cohort_table b
	on a.BENE_ID = b.BENE_ID
	%if &debug_mode %then and a.BENE_ID < &debug_threshold;
	group by a.BENE_ID
%end;)
group by BENE_ID;

create unique index BENE_ID
on &output_table (BENE_ID);

select "&output_table" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &output_table;
quit;
%mend;


%macro retrieve_outcome_medpar(cohort_table, output_table);
proc sql;
create table &output_table as
select BENE_ID
	%do v=1 %to %sysfunc(countw(&medpar_vars));
		, sum(A_%sysfunc(scan(&medpar_vars, &v))) as %sysfunc(scan(&medpar_vars, &v))
	%end;
from (%do tn=1 %to 12;
		%if &tn>1 %then union all;
		select a.BENE_ID,
			sum(case when TRNSPLNT_IND_CD = '1' then 1 else 0 end) as A_TRNSPLNT,
			sum(case when RDLGY_ONCLGY_IND_SW = '1' then 1 else 0 end) as A_RDLGY_DGNSTC, 
			sum(case when RDLGY_THRPTC_IND_SW = '1' then 1 else 0 end) as A_RDLGY_THRPTC,
			sum(case when RDLGY_NUCLR_MDCN_IND_SW = '1' then 1 else 0 end) as A_RDLGY_NUCLR_MDCN, 
			sum(case when RDLGY_CT_SCAN_IND_SW = '1' then 1 else 0 end) as A_RDLGY_CT_SCAN,
			sum(case when RDLGY_OTHR_IMGNG_IND_SW = '1' then 1 else 0 end) as A_RDLGY_OTHR_IMGNG, 
			sum(case when OP_SRVC_IND_CD = '1' then 1 else 0 end) as A_OP_O_SRVC,
			sum(case when OP_SRVC_IND_CD = '2' then 1 else 0 end) as A_OP_A_SRVC,
			sum(case when OP_SRVC_IND_CD = '3' then 1 else 0 end) as A_OP_OA_SRVC,
			sum(case when ORGN_ACQSTN_IND_CD is null then 0 else 1 end) as A_ORGN_ACQSTN,
			sum(DGNS_CD_CNT) as A_DGNS_CD,
			sum(case when SRGCL_PRCDR_IND_SW = '1' then 1 else 0 end) as A_SRGCL_PRCDR,
			sum(SRGCL_PRCDR_CD_CNT) as A_SRGCL_PRCDR_COUNT,
			sum(case when PHRMCY_IND_CD = '1' then 1 else 0 end) as A_PHRMCY_ONE,
			sum(case when PHRMCY_IND_CD <> '1' and PHRMCY_IND_CD <> '0' then 1 else 0 end) as A_PHRMCY_NOT_ONE,
			sum(case when OBSRVTN_SW = '1' then 1 else 0 end) as A_OBSRVTN,
			sum(case when CRNRY_CARE_IND_CD = '1' then 1 else 0 end) as A_CRNRY_MYOCD,
			sum(case when CRNRY_CARE_IND_CD = '2' then 1 else 0 end) as A_CRNRY_PULMN,
			sum(case when CRNRY_CARE_IND_CD = '3' then 1 else 0 end) as A_CRNRY_HATRN,
			sum(case when CRNRY_CARE_IND_CD = '4' then 1 else 0 end) as A_CRNRY_INTRM,
			sum(case when CRNRY_CARE_IND_CD = '9' then 1 else 0 end) as A_CRNRY_OTHER,
			sum(case when ICU_IND_CD = '0' then 1 else 0 end) as A_ICU_GENRL,
			sum(case when ICU_IND_CD = '1' then 1 else 0 end) as A_ICU_SURGC,
			sum(case when ICU_IND_CD = '2' then 1 else 0 end) as A_ICU_MEDCL,
			sum(case when ICU_IND_CD = '3' then 1 else 0 end) as A_ICU_PEDTR,
			sum(case when ICU_IND_CD = '4' then 1 else 0 end) as A_ICU_PSYTR,
			sum(case when ICU_IND_CD = '6' then 1 else 0 end) as A_ICU_INTRM,
			sum(case when ICU_IND_CD = '7' then 1 else 0 end) as A_ICU_BURNC,
			sum(case when ICU_IND_CD = '8' then 1 else 0 end) as A_ICU_TRAUM,
			sum(case when ICU_IND_CD = '9' then 1 else 0 end) as A_ICU_OTHER,
			sum(INTNSV_CARE_DAY_CNT) as A_INTNSV_CARE_DAY,
			sum(CRNRY_CARE_DAY_CNT) as A_CRNRY_CARE_DAY,
			sum(INTNSV_CARE_CHRG_AMT) as A_INTNSV_CARE_CHRG,
			sum(CRNRY_CARE_CHRG_AMT) as A_CRNRY_CARE_CHRG,
			sum(OTHR_SRVC_CHRG_AMT) as A_OTHR_SRVC_CHRG,
			sum(PHRMCY_CHRG_AMT) as A_PHRMCY_CHRG,
			sum(LAB_CHRG_AMT) as A_LAB_CHRG,
			sum(RDLGY_CHRG_AMT) as A_RDLGY_CHRG, 
			sum(MRI_CHRG_AMT) as A_MRI_CHRG,
			sum(OP_SRVC_CHRG_AMT) as A_OP_SRVC_CHRG,
			sum(ER_CHRG_AMT) as A_ER_CHRG,
			sum(PROFNL_FEES_CHRG_AMT) as A_PROFNL_FEES_CHRG,
			sum(ORGN_ACQSTN_CHRG_AMT) as A_ORGN_ACQSTN_CHRG,
			sum(ESRD_REV_SETG_CHRG_AMT) as A_ESRD_REV_SETG_CHRG,
			sum(CLNC_VISIT_CHRG_AMT) as A_CLNC_VISIT_CHRG,
			sum(POA_DGNS_CD_CNT) as A_POA_DGNS_CD,
			sum(DGNS_E_CD_CNT) as A_DGNS_E_CD,
			sum(case when INFRMTL_ENCTR_IND_SW = '1' then 1 else 0 end) as A_INFRMTL_ENCTR_IND,
			sum(TOT_CHRG_AMT) as A_TOT_CHRG
		from MEDPAR.MEDPAR_%eval(2000+&year) a
		inner join &cohort_table b
		on a.BENE_ID = b.BENE_ID
		%if &debug_mode %then and a.BENE_ID < &debug_threshold;
		group by a.BENE_ID
	%end;)
group by BENE_ID;

create unique index BENE_ID
on &output_table (BENE_ID);

select "&output_table" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &output_table;
quit;
%mend;


%macro retrieve_icd_procedures(claims_table, prefix, cohort, output);
%let case_code_super_group = %str(case
	when CODE_GROUP <= '0099' then '_00_00'
	when CODE_GROUP <= '0599' then '_01_05'
	when CODE_GROUP <= '0799' then '_06_07'
	when CODE_GROUP <= '1699' then '_08_16'
	when CODE_GROUP <= '1799' then '_17_17'
	when CODE_GROUP <= '2099' then '_18_20'
	when CODE_GROUP <= '2999' then '_21_29'
	when CODE_GROUP <= '3499' then '_30_34'
	when CODE_GROUP <= '3999' then '_35_39'
	when CODE_GROUP <= '4199' then '_40_41'
	when CODE_GROUP <= '5499' then '_42_54'
	when CODE_GROUP <= '5999' then '_55_59'
	when CODE_GROUP <= '6499' then '_60_64'
	when CODE_GROUP <= '7199' then '_65_71'
	when CODE_GROUP <= '7599' then '_72_75'
	when CODE_GROUP <= '8499' then '_76_84'
	when CODE_GROUP <= '8699' then '_85_86'
	when CODE_GROUP <= '9999' then '_85_86'
	else '_X' end);

/* SQL has a hard limit on 256 tables per query, so we need to slice the computation. */
/* The slice starts and ends must to be in the same order in the macro variable. */
%let slice_starts = 1 4 7 10;
%let slice_ends = 3 6 9 12;
%let sql_slices = %sysfunc(countw(&slice_starts));

%do s=1 %to %sysfunc(countw(&sql_slices));
	proc sql;
	create table RIP_&s as
	select BENE_ID, &case_code_super_group as CODE_SUPER_GROUP, sum(A_ROWS) as ROWS
	from (select BENE_ID, CODE_GROUP, sum(B_ROWS) as A_ROWS
		from (%do t=%sysfunc(scan(&slice_starts, &s)) %to %sysfunc(scan(&slice_ends, &s));
			%if &t > %sysfunc(scan(&slice_starts, &s)) %then union all;
			select BENE_ID, CODE_GROUP, sum(A_ROWS) as B_ROWS
			from (%do c=1 %to 25;
				%if &c > 1 %then union all;
				select a.BENE_ID, substring(ICD_PRCDR_CD&c from 1 for 2) as CODE_GROUP, count(*) as A_ROWS
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

/* Make sure all columns exist in the transposed table. */
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

select "&output" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &output;
quit;

/*
proc sql;
create table &output._SV as
select "&output._SV" as TABLE
	%do v=1 %to %sysfunc(countw(&code_super_groups));
		%let var = &flag.&prefix.&code_super_groups_prefix.%sysfunc(scan(&code_super_groups, &v));
		, sum(&var) as SUM_&var
	%end;
from &output;
quit;
*/
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

proc sql;
create table %if &year = 13 %then AOM_A; %else AOM_B; as
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
	PLAN_CVRG_MOS_NUM as &flag.PLAN_CVRG_MOS_NUM
from &input a
left join BENE_CC.MBSF_AB_%eval(2000+&year) b on a.BENE_ID = b.BENE_ID
left join BENE_CC.MBSF_CC_%eval(2000+&year) c on a.BENE_ID = c.BENE_ID
left join BENE_CC.MBSF_CU_%eval(2000+&year) d on a.BENE_ID = d.BENE_ID
left join BENE_CC.MBSF_D_%eval(2000+&year) e on a.BENE_ID = e.BENE_ID
%if &debug_mode %then where a.BENE_ID < &debug_threshold;;

create unique index BENE_ID
on %if &year = 13 %then AOM_A; %else AOM_B; (BENE_ID);
quit;

%if &year = 13 %then %do;
	proc sql;
	create table AOM_B as
	select a.*,
		case when BENE_DEATH_DT is not null and BENE_DEATH_DT <= &death_outcome_obs_end
			then 1 else 0 end as &flag.BENE_DEATH_FLAG
	from AOM_A a left join EDB.VITAL_STATUS_W_NAMES_FILE b
	on a.BENE_ID = b.BENE_ID
	%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

	drop table AOM_A;
	quit;
%end;

data &output;
	set AOM_B;
	NUM_CC = %do VA=1 %to %sysfunc(countw(&cc_variables));
			%if &VA>1 %then %str(+); &flag.%scan(&cc_variables, &VA)_FLAG
		%end;;
run;

proc sql;
drop table AOM_B;

create unique index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
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

select "&output" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &output;
quit;

/*
proc sql;
create table &output._SV as
select "&output._SV" as TABLE
	%do t=1 %to %sysfunc(countw(&claim_tables));
		%let var = &flag.%sysfunc(scan(&claim_tables, &t));
		, sum(&var._CLAIMS) as SUM_&var._CLAIMS, sum(&var._TOT_CHRG) as SUM_&var._TOT_CHRG
	%end;
from &output;
quit;
*/
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

select "&output" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &output;
quit;

/*
proc sql;
create table &output._SV as
select "&output._SV" as TABLE
	%do v=1 %to %sysfunc(countw(&medpar_vars));
		%let var = &flag.%sysfunc(scan(&medpar_vars, &v));
	, sum(&var) as SUM_&var
%end;
from &output;
quit;
*/
%mend;

%macro extract_beneficiary_cohorts(intervention, control);
proc sql;
create table &intervention as
select BENE_ID
from &itv_cohort_tbl;

create unique index BENE_ID
on &intervention (BENE_ID);

select "&intervention" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &intervention
%if &debug_mode %then where BENE_ID < &debug_threshold;;
quit;

proc sql;
create table &control as
select BENE_ID
from &ctr_cohort_tbl;

create unique index BENE_ID
on &control (BENE_ID);

select "&control" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &control
%if &debug_mode %then where BENE_ID < &debug_threshold;;
quit;
%mend;

%macro make_tables;
%if &debug_mode %then
	%let outputlib = &userlib;
%else
	%let outputlib = &sharedlib;

%let itv_cohort = &userlib..CISA_ITV_BENE_IDS;
%let ctr_cohort = &userlib..CISA_CTR_BENE_IDS;

%extract_beneficiary_cohorts(&itv_cohort, &ctr_cohort);

%do Y = 1 %to %sysfunc(countw(&year_list));
	%let year = %sysfunc(scan(&year_list, &Y));
	%if &year = 11
		%then %let flag = B;
		%else %let flag = A;
	%let itv_outcome_tbl = &outputlib..&proj_cn._&itv_outcome_tbl_suffix&year;
	%let ctr_outcome_tbl = &outputlib..&proj_cn._&ctr_outcome_tbl_suffix&year;

	%add_outcome_medpar(&itv_cohort, &itv_outcome_tbl._MEDP);
	%add_outcome_medpar(&ctr_cohort, &ctr_outcome_tbl._MEDP);

	%add_outcome_mbsf(&itv_cohort, &itv_outcome_tbl._MBSF);
	%add_outcome_mbsf(&ctr_cohort, &ctr_outcome_tbl._MBSF);

	%add_icd_procedures(INPATIENT, I, &itv_cohort, &itv_outcome_tbl._INP);
	%add_icd_procedures(SNF, S, &itv_cohort, &itv_outcome_tbl._SNF);

	%add_icd_procedures(INPATIENT, I, &ctr_cohort, &ctr_outcome_tbl._INP);
	%add_icd_procedures(SNF, S, &ctr_cohort, &ctr_outcome_tbl._SNF);

	%add_outcome_claims(&itv_cohort, &itv_outcome_tbl._CLMS);
	%add_outcome_claims(&ctr_cohort, &ctr_outcome_tbl._CLMS);
%end;
%mend;

%make_tables;

GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;

