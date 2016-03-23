%LET _CLIENTTASKLABEL='Make difference table';
%LET _CLIENTPROJECTPATH='\\ccwdata.org\Profiles\fku838\Documents\Projects\cisa\cisa.egp';
%LET _CLIENTPROJECTNAME='cisa.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;
%let debug_mode = 0;
%let debug_threshold = 10000;

%let proj_cn = CISA;
%let userlib = FKU838SL;
%let sharedlib = SH026250;

%let atc4_map = &userlib..&proj_cn._NDC_ATC4;

%let CLMS_VARS =
	INPATIENT_CLAIMS INPATIENT_TOT_CHRG
	SNF_CLAIMS SNF_TOT_CHRG
	OUTPATIENT_CLAIMS OUTPATIENT_TOT_CHRG
	BCARRIER_CLAIMS BCARRIER_TOT_CHRG;

%let MBSF_VARS =
	/* AB segment */
	BENE_AGE_AT_END_REF_YR

	/* CC segment */
	AMI_FLAG ALZH_DEMEN_FLAG ATRIAL_FIB_FLAG CATARACT_FLAG CHRONICKIDNEY_FLAG COPD_FLAG CHF_FLAG DIABETES_FLAG
	GLAUCOMA_FLAG HIP_FRACTURE_FLAG ISCHEMICHEART_FLAG DEPRESSION_FLAG OSTEOPOROSIS_FLAG RA_OA_FLAG
	STROKE_TIA_FLAG CANCER_BREAST_FLAG CANCER_COLORECTAL_FLAG CANCER_PROSTATE_FLAG CANCER_LUNG_FLAG
	CANCER_ENDOMETRIAL_FLAG ANEMIA_FLAG ASTHMA_FLAG HYPERL_FLAG HYPERP_FLAG HYPERT_FLAG HYPOTH_FLAG

	/* CU segment */
	PHYS_EVENTS PTB_DRUG_EVENTS HOP_VISITS SNF_COV_DAYS HOS_COV_DAYS HH_VISITS ASC_EVENTS
	EM_EVENTS ANES_EVENTS DIALYS_EVENTS OPROC_EVENTS IMG_EVENTS TEST_EVENTS DME_EVENTS OTHC_EVENTS
	READMISSIONS PTD_FILL_CNT

	/* CU segment, Medicare payments */
	ACUTE_MDCR_PMT ANES_MDCR_PMT ASC_MDCR_PMT DIALYS_MDCR_PMT DME_MDCR_PMT EM_MDCR_PMT
	HH_MDCR_PMT HOP_MDCR_PMT HOS_MDCR_PMT IMG_MDCR_PMT OPROC_MDCR_PMT OTHC_MDCR_PMT PHYS_MDCR_PMT
	PTB_DRUG_MDCR_PMT PTD_MDCR_PMT SNF_MDCR_PMT TEST_MDCR_PMT OIP_MDCR_PMT PTD_TOTAL_RX_CST

	/* CU segment, Beneficary payments */
	ACUTE_BENE_PMT ANES_BENE_PMT ASC_BENE_PMT DIALYS_BENE_PMT DME_BENE_PMT EM_BENE_PMT
	HOP_BENE_PMT IMG_BENE_PMT OIP_BENE_PMT OPROC_BENE_PMT OTHC_BENE_PMT PHYS_BENE_PMT PTB_DRUG_BENE_PMT
	SNF_BENE_PMT TEST_BENE_PMT PTD_BENE_PMT

	/* D segment and some other variables */
	MEDICARE_PMTS BENEFICIARY_PMTS 
	ER_VISITS INPAT_DAYS 
	/*PLAN_CVRG_MOS_NUM*/
	BENE_DEATH_FLAG
	NUM_CC;

%let MEDP_VARS = TRNSPLNT RDLGY_DGNSTC RDLGY_THRPTC RDLGY_NUCLR_MDCN RDLGY_CT_SCAN RDLGY_OTHR_IMGNG
	OP_O_SRVC OP_A_SRVC OP_OA_SRVC ORGN_ACQSTN DGNS_CD SRGCL_PRCDR SRGCL_PRCDR_COUNT PHRMCY_ONE PHRMCY_NOT_ONE
	OBSRVTN	CRNRY_MYOCD CRNRY_PULMN CRNRY_HATRN CRNRY_INTRM CRNRY_OTHER ICU_GENRL ICU_SURGC ICU_MEDCL ICU_PEDTR
	ICU_PSYTR ICU_INTRM ICU_BURNC ICU_TRAUM ICU_OTHER INTNSV_CARE_DAY CRNRY_CARE_DAY INTNSV_CARE_CHRG
	CRNRY_CARE_CHRG OTHR_SRVC_CHRG PHRMCY_CHRG LAB_CHRG RDLGY_CHRG MRI_CHRG OP_SRVC_CHRG ER_CHRG
	PROFNL_FEES_CHRG ORGN_ACQSTN_CHRG ESRD_REV_SETG_CHRG CLNC_VISIT_CHRG POA_DGNS_CD DGNS_E_CD
	INFRMTL_ENCTR_IND TOT_CHRG;

%let ICD9L3_GROUPS = _00_00 _01_05 _06_07 _08_16 _17_17 _18_20 _21_29 _30_34 _35_39 _40_41
	_42_54 _55_59 _60_64 _65_71 _72_75 _76_84 _85_86 _87_99 _X;

%let INP_prefix = IICD9L3;
%let SNF_prefix = SICD9L3;

%let after_flag = A;
%let before_flag = B;
%let table_list = MEDP INP SNF CLMS MBSF DRUG;
%let chrt_list = ITV CTR;


%macro make_diff_table(chrt, tbl);
%if &tbl^=INP & &tbl^=SNF %then
	%do;
		%if &tbl=DRUG %then
			%do;
				proc sql;
				select distinct ATC as ATC4
				into: vars_list separated by ' ' 	
				from &atc4_map
				order by ATC desc;
				quit;
			%end;
		%else
			%let vars_list = &&&tbl._VARS;
		
		proc sql;
		create table &outputlib..&proj_cn._&chrt._D_&tbl as
		select a.BENE_ID
		%do v=1 %to %sysfunc(countw(&vars_list));
			%let var = %sysfunc(scan(&vars_list, &v));
			, a.&after_flag.&var._CLAIMS - b.&before_flag.&var._CLAIMS as D&var._CLAIMS
			, a.&after_flag.&var._COST - b.&before_flag.&var._COST as D&var._COST
		%end;
		from &sharedlib..&proj_cn._&chrt._OUT13_&tbl a
		inner join &sharedlib..&proj_cn._&chrt._OUT11_&tbl b
		on a.BENE_ID = b.BENE_ID
		%if &debug_mode %then where a.BENE_ID < &debug_threshold;;
		quit;
	%end;
%else
	%do;
		%let vars_list = &ICD9L3_GROUPS;
		%let prefix = &&&tbl._prefix;
		proc sql;
		create table &outputlib..&proj_cn._&chrt._D_&tbl as
		select a.BENE_ID
		%do v=1 %to %sysfunc(countw(&vars_list));
			%let var = &prefix.%sysfunc(scan(&vars_list, &v));
			, a.&after_flag.&var - b.&before_flag.&var as D&var
		%end;
		from &sharedlib..&proj_cn._&chrt._OUT13_&tbl a
		inner join &sharedlib..&proj_cn._&chrt._OUT11_&tbl b
		on a.BENE_ID = b.BENE_ID
		%if &debug_mode %then where a.BENE_ID < &debug_threshold;;
		quit;
	%end;
%mend;


%macro make_tables;
%if &debug_mode %then
	%let outputlib = &userlib;
%else
	%let outputlib = &sharedlib;

%do t=1 %to %sysfunc(countw(&table_list));
	%do c=1 %to %sysfunc(countw(&chrt_list));
		%make_diff_table(%sysfunc(scan(&chrt_list, &c)),
			%sysfunc(scan(&table_list, &t)));
	%end;
%end;
%mend;

%make_tables;


GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;

