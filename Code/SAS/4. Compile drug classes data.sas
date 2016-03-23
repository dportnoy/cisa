%LET _CLIENTTASKLABEL='Compile drug classes data';
%LET _CLIENTPROJECTPATH='\\ccwdata.org\Profiles\fku838\Documents\Projects\cisa\cisa.egp';
%LET _CLIENTPROJECTNAME='cisa.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;
%let debug_mode = 0;
%let debug_threshold = 20000;

%let proj_cn = CISA;
%let userlib = FKU838SL;
%let sharedlib = SH026250;

%let itv_cohort_tbl = &sharedlib..&proj_cn._ITV_CHRT;
%let ctr_cohort_tbl = &sharedlib..&proj_cn._CTR_CHRT;
%let itv_outcome_tbl_suffix = ITV_OUT;
%let ctr_outcome_tbl_suffix = CTR_OUT;
%let atc4_map = &userlib..&proj_cn._NDC_ATC4;

%let ndc_year_list = 6 7 8 9 10 11 12 13;
%let year_list = 11 13;

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
	select "%eval(2000+&year)" as YEAR, MONTH(SRVC_DT) as MONTH, PROD_SRVC_ID, count(*) as CLAIMS
	from %if &year > 11 %then IN026250.PDE&year._R5094;
		%else IN026250.PDESAF%sysfunc(putn(&year, z2))_R5094;
	group by YEAR, MONTH, PROD_SRVC_ID
%end;;
quit;
%mend;


%macro retrieve_drug_claims(cohort, claims_output, cost_output);
proc sql noprint;
create table RDC_A as
select a.BENE_ID, c.ATC as ATC4, count(*) as CLAIMS, sum(TOT_RX_CST_AMT) as COST
from %if &year = 13 %then IN026250.PDE13_R5094; %else IN026250.PDESAF11_R5094; a
inner join &cohort b on a.BENE_ID = b.BENE_ID
inner join &atc4_map c on YEAR(a.SRVC_DT) = c.YEAR
	and MONTH(a.SRVC_DT) = c.MONTH and a.PROD_SRVC_ID = c.NDC
%if &debug_mode %then and a.BENE_ID < &debug_threshold;
group by a.BENE_ID, c.ATC;

/* Make sure all columns exist in the transposed table. */
select distinct ATC as ATC4
into: atc4_list separated by ' ' 	
from &atc4_map
order by ATC desc;

%do atc4=1 %to %sysfunc(countw(&atc4_list));
	insert into RDC_A (BENE_ID, ATC4, CLAIMS, COST)
	values (0, "%sysfunc(scan(&atc4_list, &atc4))", 0, 0);
%end;
quit;

proc sort data=RDC_A;
	by BENE_ID ATC4;
run;

proc transpose data=RDC_A
	out=RDC_B
	name=TRANSPOSED_VARIABLE;
	by BENE_ID;
	id ATC4;
	var CLAIMS COST;
run;

proc sql;
drop table RDC_A;

create table &claims_output as
select * from RDC_B
where TRANSPOSED_VARIABLE = 'CLAIMS';
alter table &claims_output
drop column TRANSPOSED_VARIABLE;

create table &cost_output as
select * from RDC_B
where TRANSPOSED_VARIABLE = 'COST';
alter table &cost_output
drop column TRANSPOSED_VARIABLE;

drop table RDC_B;
quit;
%mend;


%macro add_drug_claims(input, output);

%retrieve_drug_claims(&input, ADC_A_CLAIMS, ADC_A_COST);

proc sql noprint;
select distinct ATC
into: atc4_list separated by ' '
from &atc4_map
order by ATC;

create index BENE_ID
on ADC_A_CLAIMS (BENE_ID);

create index BENE_ID
on ADC_A_COST (BENE_ID);

create table ADC_B as
select a.*
%do v=1 %to %sysfunc(countw(&atc4_list));
	%let var = %sysfunc(scan(&atc4_list, &v));
	, b.&var as &flag.&var._CLAIMS, c.&var as &flag.&var._COST
%end;
from &input a
left join ADC_A_CLAIMS b
on a.BENE_ID = b.BENE_ID
left join ADC_A_COST c
on a.BENE_ID = c.BENE_ID
%if &debug_mode %then where a.BENE_ID < &debug_threshold;;

drop table ADC_A_CLAIMS, ADC_A_COST;
quit;

data &output;
	set ADC_B;
	%do v=1 %to %sysfunc(countw(&atc4_list));
		%let var = &flag.%sysfunc(scan(&atc4_list, &v));
		if &var._CLAIMS=. then &var._CLAIMS = 0;
		if &var._COST=. then &var._COST = 0;
	%end;
run;

proc sql;
drop table ADC_B;

create index BENE_ID
on &output (BENE_ID);

select "&output" as TABLE, count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &output;
quit;
%mend;


%macro make_tables;
%if &debug_mode %then
	%let outputlib = &userlib;
%else
	%let outputlib = &sharedlib;

%create_indexes_on_map;

%do &Y = 1 %to %sysfunc(countw(&year_list));
	%let year = %sysfunc(scan(&year_list, &Y));
	%if &year = 11
		%then %let flag = B;
		%else %let flag = A;
	%let itv_outcome_tbl = &outputlib..&proj_cn._&itv_outcome_tbl_suffix&year;
	%let ctr_outcome_tbl = &outputlib..&proj_cn._&ctr_outcome_tbl_suffix&year;

	proc sql;
	create table MT_A as
	select BENE_ID
	from &itv_cohort_tbl
	where &flag.PLAN_CVRG_MOS_NUM = '12'
	%if &debug_mode %then and BENE_ID < &debug_threshold;;

	create index BENE_ID
	on MT_A (BENE_ID);

	create table MT_B as
	select BENE_ID
	from &ctr_cohort_tbl
	where &flag.PLAN_CVRG_MOS_NUM = '12'
	%if &debug_mode %then and BENE_ID < &debug_threshold;;

	create index BENE_ID
	on MT_B (BENE_ID);
	quit;

	%add_drug_claims(MT_A, &itv_outcome_tbl._DRUG);
	%add_drug_claims(MT_B, &ctr_outcome_tbl._DRUG);

	proc sql;
	drop table MT_A, MT_B;
	quit;
%end;
%mend;

%extract_ndc_info_for_mapping(&ndc_year_list, MASTER_NDC_INFO);
%make_tables;

GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;

