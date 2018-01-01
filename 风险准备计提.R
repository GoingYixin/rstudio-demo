###指定参数
date = '2017-12-01'
mth=201710

###指定要使用的库
library(RODBC)
library(rJava)
library(xlsxjars)
library(xlsx)
library(proto)
library(gsubfn)
library(RSQLite)
library(tcltk)
library(sqldf)
library(lubridate)

###关联impla
ods_hive<-odbcConnect("impala") 

###逾期金额分类
str_sql1 = "
						select t1.application_id
						      ,t1.prod_type
						      ,case when t2.application_id is null then 'M0'
						            when datediff('sta_date',t2.min_due_date)<=30 then 'M1'
						            when datediff('sta_date',t2.min_due_date)<=60 then 'M2'
						            when datediff('sta_date',t2.min_due_date)<=90 then 'M3'
						            when datediff('sta_date',t2.min_due_date)<=120 then 'M4'
						            else 'M4+'
						       end as status
						      ,sum(t1.bf_amt) as bf_amt
						      ,sum(t1.int_amt) as int_amt
						      ,sum(t1.int_bf) as int_bf
						from (
						      select a.application_id
						            ,ucase(a.type) as prod_type
						            ,sum(case when a.due_type='本金' then a.undue_amount else 0 end) as bf_amt
						            ,sum(case when a.due_type='利息' then a.undue_amount else 0 end) as int_amt
						            ,sum(a.undue_amount) as int_bf
						      from dm_finance.rpt_dues as a
						      where a.due_type in ('本金','利息') and a.writeoff not in (1,2) and a.undue_amount>0
						      group by a.application_id
						            ,ucase(a.type)) as t1   
						left join (
						           select a.application_id
						                 ,min(a.due_date) as min_due_date
						           from dm_finance.rpt_dues as a
						           where a.due_date<'sta_date' and a.due_type='本金' and a.undue_amount>0 and a.writeoff not in (1,2)
						           group by a.application_id) as t2
						on t1.application_id=t2.application_id
						group by  t1.application_id
						      ,t1.prod_type
						      ,case when t2.application_id is null then 'M0'
						            when datediff('sta_date',t2.min_due_date)<=30 then 'M1'
						            when datediff('sta_date',t2.min_due_date)<=60 then 'M2'
						            when datediff('sta_date',t2.min_due_date)<=90 then 'M3'
						            when datediff('sta_date',t2.min_due_date)<=120 then 'M4'
						            else 'M4+'
						       end
                "
                
str_sql1 = gsub('sta_date',date,str_sql1)
str_sql1 = gsub('sta_mth',mth,str_sql1)
str_sql1 = gsub('\t'," ",str_sql1)
str_sql1 = gsub('\n'," ",str_sql1)
temp1<-sqlQuery(ods_hive,str_sql1,max=0,as.is=TRUE) 

temp1$bf_amt <- as.numeric(temp1$bf_amt)
temp1$bf_cnt <- 1
temp1$int_amt <- as.numeric(temp1$int_amt)
temp1$int_bf <- as.numeric(temp1$int_bf)

temp2<-read.csv("F:/文件目录/dim_prod.csv",header=T,sep=",",colClasses="character")
temp3<-merge(temp1, temp2,by = c("prod_type"),all.x=T)

str_sql2 = "
						select distinct application_id
            from dw_std.loan_applications 
            where reason_code1 in ('A999','A996','A991') and to_date(approved_at)<'sta_date'
                "

str_sql2 = gsub('sta_date',date,str_sql2)
str_sql2 = gsub('\t'," ",str_sql2)
str_sql2 = gsub('\n'," ",str_sql2)
temp_3<-sqlQuery(ods_hive,str_sql2,max=0,as.is=TRUE) 

temp4<-sqldf("
select case when b.application_id is not null then '测试'
            else a.prod_cls
       end as prod_cls1
      ,a.prod_cls
      ,a.prod_lvl
      ,a.status
      ,sum(a.bf_amt) as bf_amt
      ,sum(a.bf_cnt) as bf_cnt
      ,sum(a.int_amt) as int_amt
      ,sum(a.int_bf) as int_bf
from temp3 as a
left join temp_3 as b
on a.application_id=b.application_id
group by case when b.application_id is not null then '测试'
              else a.prod_cls
         end 
        ,a.prod_cls
        ,a.prod_lvl
        ,a.status
") 

###报表输出
names(temp4) <- c("产品分类1","产品分类","产品等级","逾期状态","本金余额","本金笔数","利息余额","本息余额")
file_name = paste("F:/常规工作/月报/风险准备计提/报表/风险准备计提",date,".csv",sep="")
write.csv(temp4,file=file_name,row.names = FALSE)