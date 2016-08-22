%LET _CLIENTTASKLABEL='Post-process results';
%LET _CLIENTPROCESSFLOWNAME='Process Flow';
%LET _CLIENTPROJECTPATH='\\ccwdata.org\Profiles\fku838\Documents\Projects\cisat\cisat.egp';
%LET _CLIENTPROJECTNAME='cisat.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;
%let proj_cn = CISAT;
%let userlib = FKU838SL;
%let sharedlib = SH026250;
%let table_list = D_INP D_MEDP D_SNF D_CLMS D_MBSF D_DRUG;
%let atc4_map = &userlib..&proj_cn._NDC_ATC4;
%let atc4_name_map = &sharedlib..ATC4_NAME;
%let variable_description_map = &sharedlib..CISA_VAR_DESC;
%let output_xlsx = "&myfiles_root./dua_026250/&proj_cn./&proj_cn. Results 3";


%macro post_process_tab1(tbl);
%let output_table = &sharedlib..&proj_cn._&tbl._PP;

proc sql;
create table &output_table as
select DESCRIPTION as 'Description'n, a.*
from &sharedlib..&proj_cn._&tbl a
left join &variable_description_map b
on a.VARIABLE = b.VARIABLE or a.VARIABLE = CATX('', b.VARIABLE, '_FLAG');
quit;

proc export
data=&output_table
dbms=xlsx replace
outfile=&output_xlsx;
sheet="PS covariates";
run;
%mend;


%macro post_process_tab2(tbl);
%let output_table = &proj_cn._TAB2_&tbl._PP;
proc sql;
create table &sharedlib..&output_table as
select %if &tbl^=D_DRUG & &tbl^=DA_DRUG %then DESCRIPTION;
	%else ATC4_NAME; as 'Description'n,
	SUBSTR(a.VARIABLE, 2) as 'Variable'n,
	NACO_M as 'Non-ACO mean'n, NACO_S as 'Non-ACO SD'n,
	YACO_M as 'ACO mean'n, YACO_S as 'ACO SD'n,
	P_VALUE as 'P-value'n,
	NACO_M - YACO_M as 'Difference'n,
	case when (NACO_M - YACO_M) < 0 then ABS(PCHNG)*-1
		else ABS(PCHNG) end as 'Percent diff.'n
from &sharedlib..&proj_cn._TAB2_&tbl a
left join
%if &tbl^=D_DRUG & &tbl^=DA_DRUG %then
	&variable_description_map b on a.VARIABLE = b.VARIABLE;
%else
	&atc4_name_map b on SUBSTR(a.VARIABLE, 2, 5) = b.ATC4;
order by %if &tbl=D_INP | &tbl=D_SNF %then 'Variable'n;
	%else 'Percent diff.'n asc;;
quit;

proc export
data=&sharedlib..&output_table
dbms=xlsx replace
outfile=&output_xlsx;
sheet="&tbl";
run;
%mend;


%macro make_tables;
/* The post-processing consists of giving descriptive names to the variables,
  computing the percent difference, renaming the columns, and exporting the
  tables to one Excel file. */
%put %sysfunc(fdelete(&output_xlsx));
%post_process_tab1(TAB1);
%do T=1 %to %sysfunc(countw(&table_list));
	%post_process_tab2(%sysfunc(scan(&table_list, &T)));
%end;
%mend;

%make_tables;


GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROCESSFLOWNAME=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;

