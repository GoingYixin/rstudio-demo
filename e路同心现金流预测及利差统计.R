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

###导出disburse_repay数据
disburse_repay<-sqlQuery(ods_hive,"
select application_id
      ,due_type
      ,cast(substr(index,1,2) as int) as pay_seq
      ,transaction_code
      ,to_date(created_at) as pay_date
      ,settle_amount
      ,pay_code
      ,row_number() over(partition by application_id,due_type,index order by created_at) as obs_seq
from dm_finance.rpt_disburse_repay
",max=0,as.is=TRUE)

###导入e路同心债权数据
temp_1<-sqldf("
select distinct application_id
      ,pay_code
from disburse_repay
where due_type='disburse'
") 

temp1<-read.csv("F:/资金方对帐/e路同心/文件-e路同心/e路同心结算放款模板.csv",header=T,sep=",",colClasses="character")
names(temp1) <- c("application_id","loan_id","institute_name ","institute_type","user_name","loan_amount","settle_amount","fee_amt","amount","amount_ll","amount_kq","tenor","rate","pay_chl","pay_time","lender","state","duration")
temp1$loan_amount <- as.numeric(gsub(',','',temp1$loan_amount))
temp1$pay_cnt <- as.numeric(gsub('月','',temp1$tenor))
temp1$disr_date <- as.Date(as_datetime(temp1$pay_time))
temp1<-temp1[,c("application_id","loan_amount","pay_cnt","disr_date","pay_cnt")]
temp1<-merge(temp1, temp_1,by = c("application_id"))

temp2_1<-read.csv("F:/资金方对帐/e路同心/文件-我来贷/e路同心债权转让20161014之前.csv",header=F,sep=",",colClasses="character")
names(temp2_1) <- c("application_id")
temp2_1<-merge(temp1,temp2_1,by = c("application_id"))
temp2_1$int_rate[temp2_1$pay_cnt<=3]<-0.1
temp2_1$int_rate[temp2_1$pay_cnt>3 & temp2_1$pay_cnt<=6]<-0.11
temp2_1$int_rate[temp2_1$pay_cnt>6 & temp2_1$pay_cnt<=9]<-0.12
temp2_1$int_rate[temp2_1$pay_cnt>9 & temp2_1$pay_cnt<=12]<-0.13
temp2_1$int_rate[temp2_1$pay_cnt>12]<-0.14

temp2_2<-read.csv("F:/资金方对帐/e路同心/文件-我来贷/e路同心债权转让20161014之后.csv",header=F,sep=",",colClasses="character")
names(temp2_2) <- c("application_id")
temp2_3<-read.csv("F:/资金方对帐/e路同心/文件-我来贷/e路同心债权转让20161014之后-12为11.csv",header=F,sep=",",colClasses="character")
names(temp2_3) <- c("application_id")
temp2_2<-rbind(temp2_2,temp2_3)
temp2_2<-merge(temp1,temp2_2,by = c("application_id"))
temp2_2$int_rate[temp2_2$pay_cnt<=3]<-0.105
temp2_2$int_rate[temp2_2$pay_cnt>3 & temp2_2$pay_cnt<=6]<-0.115
temp2_2$int_rate[temp2_2$pay_cnt>6 & temp2_2$pay_cnt<=9]<-0.125
temp2_2$int_rate[temp2_2$pay_cnt>9 & temp2_2$pay_cnt<=12]<-0.135
temp2_2$int_rate[temp2_2$pay_cnt>12]<-0.145

temp2_2<-rbind(temp2_2,temp2_1)

temp2<-sqlQuery(ods_hive,"
select tenor as pay_cnt
      ,pay_seq
from dw_udm.dim_tenor
",max=0,as.is=TRUE)

temp2$pay_cnt <- as.numeric(temp2$pay_cnt)
temp2$pay_seq <- as.numeric(temp2$pay_seq)
temp1<-merge(temp2_2,temp2,by = c("pay_cnt"))
temp1$settled_date <- as.Date(temp1$disr_date)%m+% months(temp1$pay_seq) 
temp1$eprincipal <- temp1$loan_amount*temp1$int_rate/12*((1+temp1$int_rate/12)^temp1$pay_cnt)/((1+temp1$int_rate/12)^temp1$pay_cnt-1)

temp2<-temp1
temp2$due_type<-'利息'

interest_fun = function(loan_amount,rate,psinstmamt,pay_seq){
  retain_prin = loan_amount
  if (pay_seq > 0){
    for(i in  1:pay_seq){
      interest = retain_prin * rate/12
      m_prin = psinstmamt - interest
      retain_prin = retain_prin - m_prin
    }
  }
  return(interest)
}

temp2$due_amount1 <- mapply(interest_fun,temp2$loan_amount,temp2$int_rate,temp2$eprincipal,temp2$pay_seq)

temp3<-temp1
temp3$due_type<-'本金'

principal_fun = function(loan_amount,rate,psinstmamt,pay_seq){
  retain_prin = loan_amount
  if (pay_seq > 0){
    for(i in  1:pay_seq){
      interest = retain_prin * rate/12
      m_prin = psinstmamt - interest
      retain_prin = retain_prin - m_prin
    }
  }
  return(m_prin)
}

temp3$due_amount1 <- mapply(principal_fun,temp3$loan_amount,temp3$int_rate,temp3$eprincipal,temp3$pay_seq)

temp1<-rbind(temp2,temp3)

###考虑提前结清影响
temp2<-sqldf("
select application_id
      ,max(settled_date) as settled_date
from temp1
where due_type='本金'
group by application_id
") 

temp3<-sqldf("
select a.application_id
      ,max(a.settled_date) as settled_date
from temp1 as a
left join temp2 as b
on a.application_id=b.application_id and a.settled_date=b.settled_date
where b.application_id is null and a.due_type='本金'
group by a.application_id
") 

temp2<-sqlQuery(ods_hive,"
select application_id
      ,state
      ,to_date(closed_at) as early_date
from dw_std.loans
where state in ('early_settled','closed')
",max=0,as.is=TRUE,stringsAsFactors=TRUE) 
                
temp2$early_date <- as.Date(temp2$early_date)
temp3$settled_date <- as.Date(temp3$settled_date)

temp4<-sqldf("
select b.application_id
      ,b.early_date
from temp3 as a
join temp2 as b
on a.application_id=b.application_id
where b.state='early_settled'
union all
select b.application_id
      ,b.early_date
from temp3 as a
join temp2 as b
on a.application_id=b.application_id
where b.state='closed' and a.settled_date>=b.early_date
") 

temp2<-sqldf("
select a.application_id
      ,min(a.settled_date) as settled_date1
      ,sum(case when a.due_type='本金' then a.due_amount1 else 0 end) as due_bf
      ,sum(case when a.due_type='利息' then a.due_amount1 else 0 end) as due_int
      ,min(a.pay_seq) as pay_seq1
from temp1 as a
join temp4 as b
on a.application_id=b.application_id
where a.settled_date>b.early_date and a.due_type in ('本金','利息')
group by a.application_id
") 

temp1<-sqldf("
select a.application_id
      ,a.due_type
      ,a.pay_seq
      ,a.pay_code
      ,case when b.application_id is not null and a.pay_seq>=b.pay_seq1 then b.settled_date1
            else a.settled_date
       end as settled_date
      ,case when b.application_id is not null and a.pay_seq=b.pay_seq1 and a.due_type='本金' then b.due_bf
            when b.application_id is not null and a.pay_seq>b.pay_seq1 then 0
            else a.due_amount1
       end as due_amount1
from temp1 as a
left join temp2 as b
on a.application_id=b.application_id
union all 
select a.application_id
      ,'提前结清罚金' as due_type
      ,a.pay_seq
      ,a.pay_code
      ,b.settled_date1 as settled_date
      ,(b.due_int-a.due_amount1)*0.2 as due_amount1
from temp1 as a
join temp2 as b
on a.application_id=b.application_id and a.pay_seq=b.pay_seq1 and a.due_type='利息'
") 

temp4<-data.frame(application_id=temp1[temp1$pay_seq==1 & temp1$due_type=="本金",c("application_id")])

###导出disburse_repay数据
disburse_repay<-sqldf("
select *
from disburse_repay
where due_type<>'disburse'
") 

temp2<-merge(disburse_repay,temp4,by = c("application_id"))
temp2$settle_amount <- as.numeric(temp2$settle_amount)
temp2$pay_date <- as.Date(temp2$pay_date)
temp5<-temp2[temp2$obs_seq==1,c("application_id","due_type","pay_seq","transaction_code")]
temp1<-merge(temp1,temp5,by = c("application_id","due_type","pay_seq"),all.x=T)
temp1<-merge(temp1,temp2,by = c("application_id","due_type","pay_seq","transaction_code","pay_code"),all=T)

###导出dues数据
dues<-sqlQuery(ods_hive,"
select a.application_id
      ,a.due_type
      ,cast(substr(a.index,1,2) as int) as pay_seq
      ,a.due_amount
      ,a.due_date
      ,b.pay_code
from dm_finance.rpt_dues as a
join (select distinct application_id
            ,pay_code
      from dm_finance.rpt_disburse_repay
where due_type='disburse') as b
on a.application_id=b.application_id
",max=0,as.is=TRUE)

temp3<-merge(dues,temp4,by=c("application_id"))
temp3$due_amount <- as.numeric(temp3$due_amount)
temp3$due_date <- as.Date(temp3$due_date)

###提前结清贷款清单导出
temp2<-sqldf("
select application_id
      ,max(due_date) as due_date
from temp3
where due_type='本金'
group by application_id
") 

temp4<-sqldf("
select a.application_id
      ,max(a.due_date) as due_date
from temp3 as a
left join temp2 as b
on a.application_id=b.application_id and a.due_date=b.due_date
where b.application_id is null and a.due_type='本金'
group by a.application_id
") 

temp2<-sqlQuery(ods_hive,"
select application_id
      ,state
      ,to_date(closed_at) as early_date
from dw_std.loans
where state in ('early_settled','closed')
",max=0,as.is=TRUE,stringsAsFactors=TRUE) 
                
temp2$early_date <- as.Date(temp2$early_date)
temp4$due_date <- as.Date(temp4$due_date)

temp5<-sqldf("
select b.application_id
      ,b.early_date
from temp4 as a
join temp2 as b
on a.application_id=b.application_id
where b.state='early_settled'
union all
select b.application_id
      ,b.early_date
from temp4 as a
join temp2 as b
on a.application_id=b.application_id
where b.state='closed' and a.due_date>=b.early_date
") 

temp3$due_date <- as.Date(temp3$due_date)
temp4<-sqldf("
select a.application_id
      ,b.early_date
      ,sum(a.due_amount) as due_amount
      ,min(a.pay_seq) as pay_seq1
from temp3 as a
join temp5 as b
on a.application_id=b.application_id
where a.due_date>=b.early_date and a.due_type='本金'
group by a.application_id
        ,b.early_date
") 

temp2<-sqldf("
select a.application_id
      ,a.due_type
      ,a.pay_seq
      ,a.pay_code
      ,case when b.application_id is not null and a.pay_seq>=b.pay_seq1 then b.early_date
            else a.due_date
       end as due_date
      ,case when b.application_id is not null and a.pay_seq=b.pay_seq1 and a.due_type='本金' then b.due_amount
            when b.application_id is not null and a.pay_seq>b.pay_seq1 then 0
            else a.due_amount
       end as due_amount
from temp3 as a
left join temp4 as b
on a.application_id=b.application_id
") 

temp3<-merge(temp1,temp2,by = c("application_id","due_type","pay_seq","pay_code"),all=T)
temp3$due_amount[temp3$obs_seq!=1 & temp3$transaction_code!='' & !is.na(temp3$obs_seq)]<-0

temp4_1<-sqldf("
select pay_code
      ,due_type
      ,due_date
      ,sum(due_amount) as due_amount
from temp3
group by pay_code
        ,due_type
        ,due_date
") 

temp4_2<-sqldf("
select pay_code
      ,due_type
      ,settled_date as due_date
      ,sum(due_amount1) as due_amount1
from temp3
group by pay_code
        ,due_type
        ,settled_date
") 

temp4_3<-sqldf("
select pay_code
      ,due_type
      ,pay_date as due_date
      ,sum(settle_amount) as settle_amount
from temp3
group by pay_code
        ,due_type
        ,pay_date
") 

temp4_4<-sqldf("
select pay_code
      ,due_type
      ,settled_date as due_date
      ,sum(due_amount1) as settle_amount1
from temp3
group by pay_code
        ,due_type
        ,settled_date
") 

temp4<-merge(temp4_1,temp4_2,by = c("pay_code","due_type","due_date"),all=T)
temp4<-merge(temp4,temp4_3,by = c("pay_code","due_type","due_date"),all=T)
temp4<-merge(temp4,temp4_4,by = c("pay_code","due_type","due_date"),all=T)
temp4<-temp4[!is.na(temp4$due_amount) | !is.na(temp4$due_amount1) | !is.na(temp4$settle_amount) | !is.na(temp4$settle_amount1),]
temp4$due_date <- format(temp4$due_date, "%Y-%m-%d")

names(temp4) <- c("支付渠道","费用类型","统计日期","应收金额-我来贷","应收金额-e路同心","已收金额-我来贷","已收金额-e路同心")               
tod_date<-as.Date(Sys.Date()-1,"%Y%m%%d")
file_name = paste("F:/报表/e路同心/e路同心统计报表",tod_date,".xlsx",sep="")
write.xlsx(temp4,file_name, sheetName="现金流",col.names=TRUE, row.names=FALSE, append=TRUE, showNA=F)

temp5<-sqldf("
select pay_code
      ,due_date
       ,sum(due_amount) as due_amount
      ,sum(due_amount1) as due_amount1
from temp3
where due_type='利息'
group by pay_code
        ,due_date
") 

temp5<-temp5[!is.na(temp5$due_amount) | !is.na(temp5$due_amount1),]
temp5$due_date <- format(temp5$due_date, "%Y-%m-%d")

names(temp5) <- c("支付渠道","统计日期","应收金额-我来贷","应收金额-e路同心")               
file_name = paste("F:/报表/e路同心/e路同心统计报表",tod_date,".xlsx",sep="")
write.xlsx(temp5,file_name, sheetName="利差",col.names=TRUE, row.names=FALSE, append=TRUE, showNA=F)