%LET _CLIENTTASKLABEL='Exploratory analyses';
%LET _CLIENTPROJECTPATH='\\ccwdata.org\Profiles\fku838\Documents\Projects\cisa\cisa.egp';
%LET _CLIENTPROJECTNAME='cisa.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;
%let debug_mode = 1;
%let debug_threshold = 20000;

%let proj_cn = CISA;
%let userlib = FKU838SL;
%let sharedlib = SH026250;

%let eligible_beneficiaries_table = &userlib..&proj_cn._ELIG_BENEF;

%let itv_cohort_tbl = &sharedlib..&proj_cn._ITV_CHRT;
%let ctr_cohort_tbl = &sharedlib..&proj_cn._CTR_CHRT;

%macro retrieve_icd_version(claims_table, output);
/* SQL has a hard limit on 256 tables per query, so we need to slice the computation. */
/* The slice starts and ends must to be in the same order in the macro variables. */
%let slice_starts = 1 4 7 10;
%let slice_ends = 3 6 9 12;
%let sql_slices = %sysfunc(countw(&slice_starts));

%do s=1 %to %sysfunc(countw(&sql_slices));
	proc sql;
	create table RIP_&s as
	%do t=%sysfunc(scan(&slice_starts, &s)) %to %sysfunc(scan(&slice_ends, &s));
		%if &t > %sysfunc(scan(&slice_starts, &s)) %then union;
		select distinct ICD_DGNS_VRSN_CD
		from (%do c=1 %to 25;
			%if &c > 1 %then union;
			select distinct ICD_DGNS_VRSN_CD&c as ICD_DGNS_VRSN_CD
			from RIF2011.&claims_table._CLAIMS_%sysfunc(putn(&t, z2))
			%if &debug_mode %then where BENE_ID < &debug_threshold;
			%end;)
	%end;;
	quit;
%end;

proc sql;
create table &output as
select distinct ICD_DGNS_VRSN_CD
from
(%do s=1 %to %sysfunc(countw(&sql_slices));
	%if &s > 1 %then union;
	select * from RIP_&s
%end;);

%do s=1 %to %sysfunc(countw(&sql_slices));
drop table RIP_&s;
%end;
%mend;


%macro list_ATC4_by_popularity(year, cohort);
proc sql;
create table &cohort._ATC_&year as
select c.ATC as ATC4, count(unique(b.BENE_ID)) as BENEFICIARIES, count(*) as CLAIMS, sum(TOT_RX_CST_AMT) as COST
from %if &year = 13 %then IN026250.PDE13_R5094; %else IN026250.PDESAF11_R5094; a
inner join &cohort b on a.BENE_ID = b.BENE_ID
inner join &atc4_map c on YEAR(a.SRVC_DT) = c.YEAR
	and MONTH(a.SRVC_DT) = c.MONTH and a.PROD_SRVC_ID = c.NDC
%if &debug_mode %then where a.BENE_ID < &debug_threshold;
group by c.ATC
order by BENEFICIARIES desc, COST desc, CLAIMS desc;
quit;
%mend;

%macro make_tables;
proc sql;
select count(unique(BENE_ID)) as TotaBenef
from ACO_BENE.BENEFICIARY_ACO_2013
where Q4_ASSIGN = 1;
quit;

proc sql;
select count(unique(BENE_ID)) as TotaBenef
from (select BENE_ID from BENE_CC.MBSF_AB_2011
	union select BENE_ID from BENE_CC.MBSF_AB_2013);
quit;

%retrieve_icd_version(INPATIENT, INPATIENT_ICD_VERSIONS);
%retrieve_icd_version(SNF, SNF_ICD_VERSIONS);

proc sql;
select "&eligible_beneficiaries_table" as TABLE,
	count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &eligible_beneficiaries_table;
quit;

proc sql;
create table &eligible_beneficiaries_table._explr as
select a.BENE_ID from
(select BENE_ID, STATE_CODE from BENE_CC.MBSF_AB_2011
	where BENE_HI_CVRAGE_TOT_MONS = 12
		and BENE_SMI_CVRAGE_TOT_MONS = 12
		and BENE_HMO_CVRAGE_TOT_MONS = 0
		and BENE_DEATH_DT is null
		%if &debug_mode %then and BENE_ID < &debug_threshold;) a
inner join
(select BENE_ID, STATE_CODE from BENE_CC.MBSF_AB_2013
	where BENE_HMO_CVRAGE_TOT_MONS = 0
		and
			((BENE_DEATH_DT is null and BENE_HI_CVRAGE_TOT_MONS = 12
			and BENE_SMI_CVRAGE_TOT_MONS = 12)
			or
			(BENE_HI_CVRAGE_TOT_MONS >= MONTH(BENE_DEATH_DT)
			and BENE_SMI_CVRAGE_TOT_MONS >= MONTH(BENE_DEATH_DT)))
		%if &debug_mode %then and BENE_ID < &debug_threshold;) b
on a.BENE_ID = b.BENE_ID
	and a.STATE_CODE = b.STATE_CODE;

create index BENE_ID
on &eligible_beneficiaries_table._explr (BENE_ID);

select "&eligible_beneficiaries_table (state requirement)" as TABLE,
	count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &eligible_beneficiaries_table._explr;
quit;

proc sql;
select "&itv_cohort_tbl (zip matches)" as TABLE,
	count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &itv_cohort_tbl a inner join FKU838SL.CISA_ZIP_TO_ZCTA b
on SUBSTR(a.BBENE_ZIP_CD, 1, 5) = b.ZIP;
quit;

proc sql;
select "&ctr_cohort_tbl (zip matches)" as TABLE,
	count(unique(BENE_ID)) as UNIQUE_BENE_IDS
from &ctr_cohort_tbl a inner join FKU838SL.CISA_ZIP_TO_ZCTA b
on SUBSTR(a.BBENE_ZIP_CD, 1, 5) = b.ZIP;
quit;

%list_ATC4_by_popularity(11, &itv_cohort_tbl);
%list_ATC4_by_popularity(11, &ctr_cohort_tbl);
%list_ATC4_by_popularity(13, &itv_cohort_tbl);
%list_ATC4_by_popularity(13, &ctr_cohort_tbl);
%mend;

%make_tables;


GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;

