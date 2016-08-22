%LET _CLIENTTASKLABEL='Compile drug classes data from the after period';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='\\ccwdata.org\Profiles\fku838\Documents\Projects\cisat\cisat.egp';
%LET _CLIENTPROJECTNAME='cisat.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;
/* If debug_mode=1, only a small subset of the beneficiaries will be processed.
  This is useful for debugging, because the code runs very fast. */
%let debug_mode = 0;
%let debug_threshold = 100000;

%let proj_cn = CISAT;
%let userlib = FKU838SL;
%let sharedlib = SH026250;
%let pdelib = IN026250;
%let pdereq = 5094;
%let pdenewreq = 5546;

%let itv_cohort_tbl = &sharedlib..&proj_cn._ITV_CHRT;
%let ctr_cohort_tbl = &sharedlib..&proj_cn._CTR_CHRT;
%let itv_outcome_suffix = ITV_OUT;
%let ctr_outcome_suffix = CTR_OUT;
%let atc4_map = &userlib..&proj_cn._NDC_ATC4;

%let ndc_year_list = 6 7 8 9 10 11 12 13 14;
%let year_list = 10 11 13 14;


%macro create_indexes_on_map;
proc sql;
create index ATC4
on &atc4_map (ATC4);

create index YMN
on &atc4_map (YEAR, MONTH, NDC);
quit;
%mend;


%macro extract_ndc_info_for_mapping(year_list, output);
proc sql;
create table FKU838SL.&output as
%do Y=1 %to %sysfunc(countw(&year_list));
	%if &Y > 1 %then union all;
	%let year = %sysfunc(scan(&year_list, &Y));
	select %eval(2000+&year) as YEAR, MONTH(SRVC_DT) as MONTH,
		PROD_SRVC_ID, GNN, BN, count(*) as CLAIMS
	from %if &year > 11 %then IN026250.PDE&year._R5094;
		%else IN026250.PDESAF%sysfunc(putn(&year, z2))_R5094;
	group by YEAR, MONTH, PROD_SRVC_ID, GNN, BN
%end;;
quit;
%mend;


%macro retrieve_drug_claims(cohort, claims_output, cost_output);
proc sql noprint;
create table &userlib..RDC_A as
select a.BENE_ID, c.ATC4, count(*) as CLAIMS, sum(TOT_RX_CST_AMT) as COST
from %if &year = 14 %then &pdelib..PDE&year._R&pdenewreq;
	%else %if &year = 13 %then &pdelib..PDE&year._R&pdereq;
		%else &pdelib..PDESAF&year._R&pdereq; a
inner join &cohort b on a.BENE_ID = b.BENE_ID
inner join &atc4_map c on YEAR(a.SRVC_DT) = c.YEAR
	and MONTH(a.SRVC_DT) = c.MONTH and a.PROD_SRVC_ID = c.NDC
%if &debug_mode %then and a.BENE_ID < &debug_threshold;
group by a.BENE_ID, c.ATC4;

/* Make sure all columns will exist when the table gets transposed. */
select distinct ATC4
into: atc4_list separated by ' ' 	
from &atc4_map
order by ATC4 desc;

%do atc4=1 %to %sysfunc(countw(&atc4_list));
	insert into &userlib..RDC_A (BENE_ID, ATC4, CLAIMS, COST)
	values (0, "%sysfunc(scan(&atc4_list, &atc4))", 0, 0);
%end;
quit;

proc sort data=&userlib..RDC_A;
	by BENE_ID ATC4;
run;

proc transpose data=&userlib..RDC_A
	out=&userlib..RDC_B
	name=TRANSPOSED_VARIABLE;
	by BENE_ID;
	id ATC4;
	var CLAIMS COST;
run;

proc sql;
drop table &userlib..RDC_A;

create table &claims_output as
select * from &userlib..RDC_B
where TRANSPOSED_VARIABLE = 'CLAIMS';
alter table &claims_output
drop column TRANSPOSED_VARIABLE;

create table &cost_output as
select * from &userlib..RDC_B
where TRANSPOSED_VARIABLE = 'COST';
alter table &cost_output
drop column TRANSPOSED_VARIABLE;

drop table &userlib..RDC_B;
quit;
%mend;


%macro add_drug_claims(input, output);

%retrieve_drug_claims(&input, &userlib..ADC_A_CLAIMS, &userlib..ADC_A_COST);

proc sql noprint;
select distinct ATC4
into: atc4_list separated by ' '
from &atc4_map
order by ATC4;

create index BENE_ID
on &userlib..ADC_A_CLAIMS (BENE_ID);

create index BENE_ID
on &userlib..ADC_A_COST (BENE_ID);

create table &userlib..ADC_B as
select a.*
%do v=1 %to %sysfunc(countw(&atc4_list));
	%let var = %sysfunc(scan(&atc4_list, &v));
	, b.&var as &flag.&var._CLAIMS, c.&var as &flag.&var._COST
%end;
from &input a
left join &userlib..ADC_A_CLAIMS b on a.BENE_ID = b.BENE_ID
left join &userlib..ADC_A_COST c on a.BENE_ID = c.BENE_ID
%if &debug_mode %then where a.BENE_ID < &debug_threshold;;

drop table &userlib..ADC_A_CLAIMS, &userlib..ADC_A_COST;
quit;

data &output;
	set &userlib..ADC_B;
	%do v=1 %to %sysfunc(countw(&atc4_list));
		%let var = &flag.%sysfunc(scan(&atc4_list, &v));
		if &var._CLAIMS=. then &var._CLAIMS = 0;
		if &var._COST=. then &var._COST = 0;
	%end;
run;

proc sql;
drop table &userlib..ADC_B;

create index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &output;
quit;
%mend;


%macro combine_drug_claims(input, out1, out2, out_combined);
proc sql noprint;
select distinct ATC4
into: atc4_list separated by ' '
from &atc4_map
order by ATC4;
quit;

proc sql;
create table &out_combined as
select a.*
%do v=1 %to %sysfunc(countw(&atc4_list));
	%let var = &flag.%sysfunc(scan(&atc4_list, &v));
	, b.&var._CLAIMS + c.&var._CLAIMS as &var._CLAIMS,
	b.&var._COST + c.&var._COST as &var._COST
%end;
from &input a, &out1 b, &out2 c
where a.BENE_ID = b.BENE_ID and a.BENE_ID = c.BENE_ID
%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

create index BENE_ID
on &out_combined (BENE_ID);

select "&out_combined" as TABLE, count(*) as 'Rows'n,
	count(unique(BENE_ID)) as 'Unique BENE_IDs'n
from &out_combined;
quit;

proc sql;
create table &out_combined._SV as
select "&out_combined._SV" as TABLE
	%do v=1 %to %sysfunc(countw(&atc4_list));
		%let var = &flag.%sysfunc(scan(&atc4_list, &v));
	, sum(&var._CLAIMS) as SUM_&var._CLAIMS, sum(&var._COST) as SUM_&var._COST
%end;
from &out_combined;
quit;
%mend;


%macro make_tables;
%if &debug_mode %then
	%let outputlib = &userlib;
%else
	%let outputlib = &sharedlib;

/* Extract only beneficiaries that have 24 months of Part D enrollment
 in both before period and after period, i.e. full Part D enrollment
 during the entire study (both periods are 2 years long). */
proc sql;
create table &userlib..MT_A as
select a.BENE_ID
from &sharedlib..&proj_cn._&itv_outcome_suffix._MBSF_B a
inner join &sharedlib..&proj_cn._&itv_outcome_suffix._MBSF_A b
on a.BENE_ID = b.BENE_ID
inner join &pdelib..MFF_REQ&pdereq c
on a.BENE_ID = c.BENE_ID
where BPLAN_CVRG_MOS_NUM = 24 and APLAN_CVRG_MOS_NUM = 24
%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

create unique index BENE_ID
on &userlib..MT_A (BENE_ID);

create table &userlib..MT_B as
select a.BENE_ID
from &sharedlib..&proj_cn._&ctr_outcome_suffix._MBSF_B a
inner join &sharedlib..&proj_cn._&ctr_outcome_suffix._MBSF_A b
on a.BENE_ID = b.BENE_ID
inner join &pdelib..MFF_REQ&pdereq c
on a.BENE_ID = c.BENE_ID
where BPLAN_CVRG_MOS_NUM = 24 and APLAN_CVRG_MOS_NUM = 24
%if &debug_mode %then and a.BENE_ID < &debug_threshold;;

create unique index BENE_ID
on &userlib..MT_B (BENE_ID);
quit;

%do Y = 1 %to %sysfunc(countw(&year_list));
	%let year = %sysfunc(scan(&year_list, &Y));
	%if &year = 10 | &year = 11
		%then %let flag = B;
		%else %let flag = A;
	%let itv_outcome_tbl = &outputlib..&proj_cn._&itv_outcome_suffix;
	%let ctr_outcome_tbl = &outputlib..&proj_cn._&ctr_outcome_suffix;

	%add_drug_claims(&userlib..MT_A, &itv_outcome_tbl._DRUG_&year);
	%add_drug_claims(&userlib..MT_B, &ctr_outcome_tbl._DRUG_&year);
%end;

%let flag = B;
%combine_drug_claims(&userlib..MT_A, &itv_outcome_tbl._DRUG_10,
	&itv_outcome_tbl._DRUG_11, &itv_outcome_tbl._DRUG_&flag);
%combine_drug_claims(&userlib..MT_B, &ctr_outcome_tbl._DRUG_10,
	&ctr_outcome_tbl._DRUG_11, &ctr_outcome_tbl._DRUG_&flag);

%let flag = A;
%combine_drug_claims(&userlib..MT_A, &itv_outcome_tbl._DRUG_13,
	&itv_outcome_tbl._DRUG_14, &itv_outcome_tbl._DRUG_&flag);
%combine_drug_claims(&userlib..MT_B, &ctr_outcome_tbl._DRUG_13,
	&ctr_outcome_tbl._DRUG_14, &ctr_outcome_tbl._DRUG_&flag);

proc sql;
drop table &userlib..MT_A, &userlib..MT_B;
quit;
%mend;

%extract_ndc_info_for_mapping(&ndc_year_list, MASTER_NDC_INFO);

/* Between the above macro and the macros below, the MASTER_NDC_INFO table
 needs to be exported to outside the VRDC and the NDCs of the drugs need to
 be mapped to ATC-4 classes using the R script. Please see the R script in
 the GitHub repository. */

%create_indexes_on_map;

%make_tables;

GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;

