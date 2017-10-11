libname lending '/prd/risk/retail/ifrs9/UAE_Mortgage/new_data';
libname pd "/prd/risk/retail/ifrs9/UAE_Mortgage/PD";
libname LGD "/prd/risk/retail/ifrs9/UAE_Mortgage/LGD";
libname final '/prd/risk/retail/ifrs9/UAE_Mortgage/Final_Data';
libname UAE_RAW '/prd/risk/retail/ifrs9/UAE_Mortgage/RAW_DATA';
libname ecl "/prd/risk/retail/ifrs9/UAE_Mortgage/ECL";
libname mev "/prd/risk/retail/ifrs9/UAE_Mortgage/MEV";



%let yyyymm=201506;
%let EIR=0.0419;

/*******Raw data****/
Data raw_201506;
set uae_raw.lendingbase_201506;
if (ASSETCODE in (50)) OR (ASSETCODE in (10) and ASSETCATEGORYCODE in (2,3,4,5,6,7));
if Chargeoff ne "Y" AND pos1 >0 ;
run;


data final_201506;
set raw_201506;
tenor=intck('Month',RECEIPTDATE,finalinstldate);
loanacno1 = compress(put(loanacno,$11.));
drop loanacno;
rename loanacno1 = loanacno;
N_YEAR = year(misdate);
N_MONTH = month(misdate);
if N_MONTH >= 10 then curr_mth = cats(N_YEAR,N_MONTH);
else curr_mth= cats (N_YEAR,'0',N_MONTH);
snapshot = curr_mth*1;
run;


Data drp_file;
set uae_raw.mortgage_DRP_CASE;
run;

proc sort data=drp_file;
by loanacno ;
run;

proc sort data=final_201506;
by loanacno;
run;


Data final_w_drp;
merge final_201506 (in=a) drp_file (in=b );
by loanacno ;
if a  ;
if a and b then drp =1;
else drp =0;
run;

proc sort data=final_w_drp;
by loanacno misdate;
run;


data mktvlv_cltv;
set uae_raw.mktvlv_cltv_ext;
run; 

proc sort data=mktvlv_cltv;
by loanacno misdate;
run;


Data final_w_cltv;
merge final_w_drp (in=a) mktvlv_cltv (in =b);
by loanacno misdate ;
if a ;
if a and b then cltv_merge = 1;
else cltv_merge = 0;
run;

data under_cons;
set uae_raw.under_constmon ;
snapshot =cons_date*1;
run;
proc sort data=under_cons;
by loanacno snapshot ;
run;


proc sort data=final_w_cltv;
by loanacno snapshot ;
run;


proc sort data=uae_raw.undraw out=undraw;
by loanacno snapshot ;
run;

Data final_w_cons ;
merge final_w_cltv (in=a) undraw(in=b) under_cons (in=c);
by loanacno snapshot ;
if  a ;
if a and b then un =1;
else un =0;
if a and c then cons =1;
else cons =0;
run;




/* The below code could be used for getting xdp and 30 + dpd in last 12 months*/

data snap_1;
set _null_;
run;

%macro previous(m= , m1=);
data snap_1;
set  final_w_cons;
/*rename  BKTYPE = BKTYPE_snap  ;*/
run;

data ecl.snap_1_&m.;
set snap_1;
run;

%LET BEG_YY = %EVAL(%SUBSTR(&m,1,4) * 1);
%LET L=12;

%let m2=%eval(&m1);
%let mon = %eval(&m);

%do i=1 %to &l;
     %if &m2 = 01 %then %do;
           %let mon = %eval(&mon-89);
           %let m2 = 12;
           %put &m2 ; %put &mon ;
     %end;
     %else %do;
           %let mon = %eval(&mon-1);
           %let m2 = %eval(&m2-1);
           %put &m2 ; %put &mon ;
     %end;

     proc sql noprint;
     create table ecl.snap_1_&m as
     select a.*,  b.BKTYPE as BKTYPE_last_&i. 
     from ecl.snap_1_&m as a
     left join pd.valid_&mon as b
     on a.loanacno = b.loanacno;
     quit; 
%end;
%mend;

%previous(m=201506, m1=06);


%macro DF_Tag(Beg_YYMM,END_YYMM);

%LET BEG_YY = %EVAL(%SUBSTR(&BEG_YYMM,1,4) * 1);
%LET BEG_MM = %EVAL(%SUBSTR(&BEG_YYMM,5,2) * 1);
%LET END_YY = %EVAL(%SUBSTR(&END_YYMM,1,4) * 1);
%LET END_MM = %EVAL(%SUBSTR(&END_YYMM,5,2) * 1);

*GET ROUNDS OF THE LOOP;
%LET L=%EVAL((&END_YY-&BEG_YY)*12 + (&END_MM - &BEG_MM)+1 );

%DO I = 1 %TO &L;
Data _NULL_;
Call Symput('YYYYMM',Put(intnx('month',mdy(&BEG_MM,1,&BEG_YY),&I-1,'B'),YYMMN6.));
Call Symput('BYYYYMM',Put(intnx('month',mdy(&BEG_MM,1,&BEG_YY),&I-1,'B'),date9.));
run;

Data ecl_final201506;
set ecl.snap_1_&YYYYMM. ;
lst12del = 0;
ARRAY lst12mondel{12}  BKTYPE_last_1-BKTYPE_last_12;

if BKTYPE < 4 and drp =0 then Exst_Def=0;
else Exst_Def =1;

if BKTYPE < 4 and drp = 0 then do;
J=1;
DO WHILE (J<=12);

     
	 IF (lst12mondel{J} = 1) AND lst12del=0   THEN DO;
           lst12del=1; 
     END;
	 

   J+1; 
END;
drop J ;
END;
run;
%end;
%MEND;
%DF_Tag(201506,201506);


/***unbiased pd***/
data ecl_final201506_pd;
set ecl_final201506;

if Exst_Def=1 then do;
lgd_seg="default";
		
		cure_rate=0.216;
		fsd=0.425;
		time_def_pdp=30;
		cost_rate=0.020;
		rec_sttl_ds=0.219;
	end;
	else do;
lgd_seg="nondef";
		
		cure_rate=0.524;
		fsd=0.515;
		time_def_pdp=47;
		cost_rate=0.020;
		rec_sttl_ds=0.344;
	end;



run;

/***merging current ppi data*****/

data ecl.hist_ppi;
set mev.mev_historic;
if country ="AE" and Concept ="Residential property index";
run;

data ecl.for_ppi;
set mev.mev_forecast;
if country ="AE" and Concept ="Residential property index";
run;

data ecl.ppi_all;
set ecl.hist_ppi ecl.for_ppi;
run;

proc sql;
		create table prep1_ecl_&yyyymm. as select
			a.*
			,b.factor_value as ppi_latest
				
	/*=Market value at sale=*/
		from ecl_final201506_pd a
			
			left join ecl.hist_ppi b
				on intnx('quarter', mdy(a.n_month,1,a.n_year), 0, 'e')=b.time_period
			
		;
quit;

data prep1_ecl_&yyyymm.;
set prep1_ecl_&yyyymm.;
fppi_curr1 = ppi_latest;
alpha = 0.23;
run;


/*===YEAR 1 - LTV at current, LTV at default, market value at sale for non-default an default segment===*/
	proc sql;
		create table prep2_ecl_&yyyymm. as select
			a.*
			
			,d.factor_value as fppi_nd_sale1
			,e.factor_value as fppi_d_sale1
	
	/*=Market value at sale=*/
			,case when Exst_Def=1 then fppi_d_sale1/a.ppi_latest*a.mkt
			      else fppi_nd_sale1/a.ppi_latest*a.mkt
				  end as cmv_sale1
		from prep1_ecl_&yyyymm. a
		
			left join ecl.for_ppi d
				on intnx('quarter',intnx('month',intnx('quarter', mdy(a.n_month,1,a.n_year), 2, 'e'), 47, 'e'),0,'e')=d.time_period
			left join ecl.for_ppi e
				on intnx('quarter',intnx('month',intnx('quarter', mdy(a.n_month,1,a.n_year), 0, 'e'), 30, 'e'),0,'e')=e.time_period
		;
quit;



*=============================;
*	STEP 4: ECL Computation;
*=============================;

	data prep3_ecl_&yyyymm.;
	set prep2_ecl_&yyyymm.;
	length pd_seg $50. ead_seg $20.;

/*=====UNBIASED PD=====*/
if Exst_Def=1 then do;
	pd_seg="00 Existing default";
	pd_unbiased=1;
end;

else do;
if BKTYPE in (3,2) then pd_seg =1;
else if BKTYPE = 1 then pd_seg =2;
else if lst12del =1 then pd_seg =3;
else if cltv >=1.07 then pd_seg =4;
else pd_seg =5;

if BKTYPE in (3,2) then pd_unbiased =0.55;
else if BKTYPE = 1 then pd_unbiased =0.076;
else if lst12del =1 then pd_unbiased =0.013;
else if cltv >=1.07 then pd_unbiased =0.034;
else pd_unbiased =0.0012;

end;


/*=====UNBIASED EAD=====*/
		if Exst_Def=1 then ead_seg="00 Existing default"; else ead_seg="01 Non Default";
		if cons = 1 then  ead_unbiased=pos1+undrawn_amt; else ead_unbiased = pos1;

/*=====UNBIASED LGD=====*/
	
			recovery=max((1-fsd)*cmv_sale1-cost_rate*cmv_sale1+rec_sttl_ds*ead_unbiased,0);
            disc_loss1=max(ead_unbiased-recovery/(1+&EIR./12)**time_def_pdp,0);
            if ead_unbiased>0 then lgd_unbiased=min((1-cure_rate)*disc_loss1/ead_unbiased,1); 
			else lgd_unbiased=0;

/*=====UNBIASED ECL=====*/
		mtd_0 = 6; 
		disc_factor_0 = (1+&EIR/12)**(-mtd_0);
		ecl_unbiased = pd_unbiased*ead_unbiased*lgd_unbiased*disc_factor_0;


*===============================================;
*	STEP 5: Lifetime EAD and Forward Looking LGD;
*===============================================;


        rem_tenor = max(tenor- mob);
		if rem_tenor<=0 then a_schd_instl_adj=0;
		else if rem_tenor>0 then a_schd_instl_adj= mort(POS1, ., INTERESTRATE/1200,rem_tenor);

		length ostd_1 - ostd_300 rem_tenor_1 - rem_tenor_300  ead_lifetime2 - ead_lifetime25 8.;

		array ostd[*] ostd_1 - ostd_300;
		array remtenor[*] rem_tenor_1 - rem_tenor_300;
		array ead[*] ead_lifetime2 - ead_lifetime25;
       
		do m = 1 to 300;
			remtenor[m] = tenor-(mob+m);
			if remtenor[m]<=0 then ostd[m]=0;
			else if remtenor[m]>0 then do;
			         if cons = 1 then do;
                         ostd[m] = mort(.,a_schd_instl_adj,INTERESTRATE/1200,remtenor[m])+undrawn_amt;
				     end;
				     else do;
				         ostd[m] = mort(.,a_schd_instl_adj,INTERESTRATE/1200,remtenor[m]);
				     end;
		    end;
        end;
	/**year 2 onwards**/
		do y = 1 to 24;
			ead[y] = ostd[y*12];
		end;

	do yy = 1 to 24;
	if ead_seg="00 Existing default" then do;
		ead[yy] = POS1; *assign balance at snapshot for existing defaults;
		end;
	end;
	drop ostd_1 - ostd_300 rem_tenor_1 - rem_tenor_300 m y yy;

run;


/*=====FORWARD LOOKING LGD=====*/
/*===YEAR2 onwards:- merge forecast PPI at snapshot, default and sale up to year 30th
                   - LTV at current, LTV at default, market value at sale for non-default an default segment===*/

data prep4_ecl_&yyyymm.;
set prep3_ecl_&yyyymm.;
proc sort; by loanacno;
run;


	%macro lifetime_ppi();
		%do yr=1 %to 24; /*year 2 to year 25*/
		%let z=%eval(&yr+1);
		proc sql;
		create table lifetime_ppi as select
			a.*			
			,c.factor_value as fppi_nd_sale&z.
            ,d.factor_value as fppi_curr&z.	
			,e.factor_value as fppi_d_sale&z.
		
		/*=Market value at sale=*/
			,case when Exst_Def=1 then fppi_d_sale&z./a.ppi_latest*a.mkt
			 else fppi_nd_sale&z./a.ppi_latest*a.mkt
			 end as cmv_sale&z.
			from prep3_ecl_&yyyymm. as a 
			
			left join ecl.for_ppi as c
	        on intnx('quarter', intnx('month', intnx('month',intnx('quarter', mdy(a.n_month,1,a.n_year), 2, 'e'), 47, 'e'), 12*&yr., 'e'), 0, 'e')=c.time_period
	       	left join ecl.ppi_all as d
			on intnx('quarter', intnx('month', intnx('quarter', mdy(a.n_month,1,a.n_year), 0, 'e'), 12*&yr., 'e'), 0, 'e')=d.time_period
	       	left join ecl.for_ppi as e
			on intnx('quarter', intnx('month', intnx('month',intnx('quarter', mdy(a.n_month,1,a.n_year), 0, 'e'), 30, 'e'), 12*&yr., 'e'), 0, 'e')=e.time_period 
			/***NOTE: forecast PPI provided by QPO is insufficient for PPI sale***/
			order loanacno;
		quit;

		

		/*=YEAR2:- merge lgd parameters*/
/*				proc sql;*/
/*				create table lifetime_ppi as select*/
/*				a.**/
/*				,b.sales_cost as sales_cost&z.*/
/*				,b.alpha as alpha&z.*/
/*				from lifetime_ppi as a*/
/*				left join lgd_para as b*/
/*				on a.f_acct_default_0=b.default_0 and compress(a.ltv_def&z._band)=compress(b.ltv_def)*/
/*				order by id_acct;*/
/*				quit;*/
/**/
		data prep4_ecl_&yyyymm.;
		merge prep4_ecl_&yyyymm. lifetime_ppi;
		by loanacno;
		run;
	%end;
	%mend;
    %lifetime_ppi();
		

/*===YEAR2:- forward looking cure rate and LGD===*/
		data prep5_ecl_&yyyymm.;
		set prep4_ecl_&yyyymm.;
		

		If Exst_Def=0 then do;
			if (fppi_curr1 - fppi_curr2)/fppi_curr1 <= 0 then fwl_cure2 = cure_rate;
			else fwl_cure2 = max(0, (cure_rate - alpha * ((fppi_curr1-fppi_curr2)/fppi_curr1)));
		end;
        else do;
            fwl_cure2 = cure_rate;
		end;
	
			recovery2=max((1-FSD)*cmv_sale2 - cost_rate*cmv_sale2 + rec_sttl_ds*ead_lifetime2,0);
			disc_loss2=max(ead_lifetime2-recovery2/(1+&EIR./12)**time_def_pdp,0);
			if ead_lifetime2>0 then lgd_fwl2=min((1-fwl_cure2)*disc_loss2/ead_lifetime2,1);
			else lgd_fwl2=0;
		run;

		

/*===YEAR3 onwards:- forward looking cure rate and LGD===*/
%macro fwlcure();
	%do m = 2 %to 24; 
	%let n = %eval(&m+1);
		
		data cure;
		set prep5_ecl_&yyyymm.;
		If Exst_Def=0 then do;

			array max_ppi {*} fppi_curr1 - fppi_curr&m.;
				if (max(of max_ppi{*}) - fppi_curr&n.) / max(of max_ppi{*}) <= 0 then fwl_cure&n. = cure_rate;
					else fwl_cure&n. = max(0, (cure_rate - alpha * (max(of max_ppi{*}) - fppi_curr&n.) / max(of max_ppi{*})));
        end;
        else do;
		fwl_cure&n. = cure_rate;
		end;

		

			recovery&n.=max((1-FSD)*cmv_sale&n. - cost_rate*cmv_sale&n. + rec_sttl_ds*ead_lifetime&n.,0);
			disc_loss&n.=max(ead_lifetime&n.-recovery&n./(1+&EIR./12)**time_def_pdp,0);
			if ead_lifetime&n.>0 then lgd_fwl&n.=min((1-fwl_cure&n.)*disc_loss&n./ead_lifetime&n.,1);
            else lgd_fwl&n.=0;

		proc sort; by loanacno;

		data prep5_ecl_&yyyymm.;
		merge prep5_ecl_&yyyymm. cure;
		by loanacno;
		run;
%end;
%mend;
%fwlcure();

data ecl.stg03_ecl_&yyyymm.;
set prep5_ecl_&yyyymm.;
run;

