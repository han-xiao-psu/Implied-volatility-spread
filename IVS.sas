%let path=D:\Dropbox\Prediction_IVS;

libname data "&path.\Data";

/***** Part I connecting with WRDS *************/

/*log into wrds via PC SAS connect*/
%let wrds=wrds-cloud.wharton.upenn.edu 4016;
options comamid=TCP remote=WRDS;
signon username=_prompt_;

 /*assign libnames to wrds datasets*/
rsubmit;
libname optionm "/wrds/optionm/sasdata/";
libname crsp "/wrds/crsp/sasdata/a_stock";
endrsubmit;

/*assign libnames to wrds datasets*/
libname wwork (work) server=wrds;
libname optionm (optionm) server=wrds;
libname crsp (crsp) server=wrds;

/***** Part II data ****************************/

/*******************************************************************************/
/*******************************************************************************/
/**********SECTION 1: Constructing the predictor********************************/
/*******************************************************************************/
/********************************************************************************/

/************** Download Implied Volatility **************************/

rsubmit;
data sample01;
set optionm.vsurfd1996(where=(days=30 and abs(delta)=50));
run;
endrsubmit;

rsubmit;
proc append base=sample01 data=optionm.vsurfd1997(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd1998(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd1999(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2000(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2001(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2002(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2003(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2004(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2005(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2006(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2007(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2008(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2009(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2010(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2011(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2012(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2013(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2014(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2015(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2016(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2017(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2018(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2019(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2020(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2021(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2022(where=(days=30 and abs(delta)=50));run;
endrsubmit;
rsubmit;
proc append base=sample01 data=optionm.vsurfd2023(where=(days=30 and abs(delta)=50));run;
endrsubmit;

/************** Creates OptionMetrics-CRSP Link Table **************************/
rsubmit;
%oclink;
endrsubmit;

/*merge permno with secid*/
rsubmit;
proc sql;
create table sample02 as
select distinct b.permno, a.*, intnx('month',a.date,0,'E') as mdate format date9., year(a.date) as year
from sample01 as a left join oclink as b
on a.secid=b.secid
where not missing(permno) and not missing(impl_volatility)
order by mdate, permno;
quit;
proc sort data=sample02; by permno cp_flag mdate date; run;
data sample03;
set sample02;
by permno cp_flag mdate;
run;
endrsubmit;

/**************** Generate Aggregate Equal-weighted IVS **************************/
rsubmit;
proc sort data=sample03; by mdate cp_flag; run;
proc means data=sample03 N mean noprint;
by mdate cp_flag;
var impl_volatility;
output out=sample04(drop=_TYPE_ _FREQ_) N=NUM mean=IV;
run;
endrsubmit;

/* Separate Call and Put Implied Volatility */
rsubmit;
data sample04C(drop=cp_flag);
set sample04;
if cp_flag='P' then delete;
rename IV=IV_CALL;
rename NUM=NUM_CALL;
label IV='CALL option IV';
label NUM='Option number';
run;
data sample04P(drop=cp_flag);
set sample04;
if cp_flag='C' then delete;
rename IV=IV_PUT;
label IV='Put option IV';
run;
endrsubmit;

/* Equal-weighted IVS */
rsubmit;
proc sql;
create table IVS_EW_MONTH
as select distinct a.mdate,a.NUM_CALL,a.IV_CALL-b.IV_PUT as IVS_EW
from sample04C as a, sample04P as b
where a.mdate=b.mdate;
quit;
endrsubmit;

/******************************** Data output ***********************************/
rsubmit;
proc download data=IVS_EW_MONTH out=IVS_EW_MONTH;run;
endrsubmit;

proc export data = IVS_EW_MONTH  outfile = "&path.\Data\IVS_EW_MONTH_2023.csv"
dbms = csv replace;
run;

signoff;

ods pdf file = "&path.\Figure\IVS2023.pdf" pdftoc = 1 startpage=no style = printer dpi = 2500;
options nodate nonumber orientation=landscape;
ods graphics / reset=all width=10.5in border=off;
proc sgplot data=IVS_EW_MONTH;
format IVS_EW percentn8.3;
format mdate year4.;
band y=IVS_EW lower="01MAR2001"d upper= "30NOV2001"d / name='Recession' legendlabel = 'Recession' fillattrs=(color = gray transparency=0.6) ;
band y=IVS_EW lower="01DEC2007"d upper= "30JUN2009"d / fillattrs=(color = gray transparency=0.6) ;
band y=IVS_EW lower="01FEB2020"d upper= "30APR2020"d / fillattrs=(color = gray transparency=0.6) ;
refline 0 / axis=y lineattrs=(thickness=3 color=red pattern=dash);
series x=mdate y=IVS_EW/ name="IVS" legendlabel = 'IVS' lineattrs=(thickness=3 color=blue);
keylegend "IVS" "Recession" / location=inside position=bottomright across=1 noborder valueattrs=(color=black size=20pt family="Times New Roman");
xaxis label="Year" min='31JAN1996'd max='31DEC2023'd values=('31JAN1996'd to '31DEC2023'd by 200) 
	valueattrs=(color=black size=20pt family="Times New Roman") labelattrs=(color=black size=20pt family="Times New Roman");
yaxis label='IVS (%)' values=(-0.06 to 0.04 by 0.02)  valueattrs=(color=black size=20pt family="Times New Roman") labelattrs=(color=black size=20pt family="Times New Roman");
run;
ods pdf close;
ods graphics;
