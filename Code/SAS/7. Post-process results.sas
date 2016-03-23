%LET _CLIENTTASKLABEL='Post-process results';
%LET _CLIENTPROJECTPATH='\\ccwdata.org\Profiles\fku838\Documents\Projects\cisa\cisa.egp';
%LET _CLIENTPROJECTNAME='cisa.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;
%let proj_cn = CISA;
%let userlib = FKU838SL;
%let sharedlib = SH026250;
%let table_list =
	OUT11_MEDP OUT11_INP OUT11_MBSF OUT11_SNF OUT11_CLMS
	OUT13_MEDP OUT13_INP OUT13_MBSF OUT13_SNF OUT13_CLMS
	D_INP D_MEDP D_SNF D_CLMS D_MBSF D_DRUG;
%let atc4_map = &userlib..&proj_cn._NDC_ATC4;
%let atc4_name_map = &userlib..&proj_cn._ATC4_NAME;
%let variable_description_map = &sharedlib..&proj_cn._VAR_DESC;

%macro post_process_tab1(tab_tbl);
proc sql;
create table &sharedlib..&proj_cn._&tab_tbl._PP as
select DESCRIPTION, a.*
from &sharedlib..&proj_cn._&tab_tbl a
left join &variable_description_map b
on a.VARIABLE = b.VARIABLE or a.VARIABLE = CATX('', b.VARIABLE, '_FLAG');
quit;
%mend;

%macro post_process_tab2(tbl);
proc sql;
create table &sharedlib..&proj_cn._TAB2_&tbl._PP as
select a.VARIABLE as 'Variable'n,
	%if &tbl^=D_DRUG & &tbl^=DA_DRUG %then DESCRIPTION;
		%else ATC_NAME; as 'Description'n,
	NO as 'Non-ACO'n, YES as ACO, DIFF as 'Diff.'n,
	DIFF/NO as 'Percent diff.'n, P_VALUE as 'P-value'n
from &sharedlib..&proj_cn._TAB2_&tbl a
left join
%if &tbl^=D_DRUG & &tbl^=DA_DRUG %then
	&variable_description_map b on a.VARIABLE = b.VARIABLE;
%else
	&atc4_name_map b on SUBSTR(a.VARIABLE, 2, 5) = b.ATC;
order by 'Percent diff.'n asc;
quit;
%mend;

%macro make_tables;
%post_process_tab1(TAB1);
%do T=1 %to %sysfunc(countw(&table_list));
	%post_process_tab2(%sysfunc(scan(&table_list, &T)));
%end;
%mend;

%make_tables;


GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;

