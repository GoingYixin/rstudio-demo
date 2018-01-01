###指定要使用的库
library(RODBC)
library(rJava)
library(xlsxjars)
library(xlsx)
library(proto)
library(gsubfn)
library(RSQLite)
library(lubridate)
library(FinCal)
library(dplyr)
library(reshape)
library(reshape2)
library(sqldf)
library(knitr)
library(rmarkdown)
library(data.table)
library(tcltk)
###关联impla
ods_hive<-odbcConnect("impala") 
###跑数日期
end_date = as.character(floor_date(Sys.Date(),"month")-1)
end_date_str = gsub('-',"",end_date)
end_date_str = substr(end_date_str,1,6)
data_source_1 = paste("dm_finance.rpt_dues_lender",end_date_str,sep="_")
data_source_2 = paste("dm_finance.rpt_disburse_repay",end_date_str,sep="_")

#取出贷款
sql_str = "SELECT s.application_id,
                  s.loan_amount,
                  to_date(s.created_at)  AS disb_date,
                  CAST(regexp_replace(s.tenor,'D|M','') AS INT) tenor
           FROM   source_2  s 
          WHERE   s.lender_id = 238 
            AND   s.due_type = 'disburse'
                  ;"

sql_str = gsub('\n'," ",sql_str)
sql_str = gsub('source_2',data_source_2,sql_str)
disb_list =sqlQuery(ods_hive,sql_str,stringsAsFactors = F,as.is = T)
disb_list$loan_amount = as.numeric(disb_list$loan_amount)
disb_list$disb_date   = as.Date(disb_list$disb_date)
##服务费率
chang_date = as.Date('2017-08-11')
#8.11前
disb_list$sever_fee_rate[disb_list$disb_date < chang_date & disb_list$tenor == 3 ] = 0.0074
disb_list$sever_fee_rate[disb_list$disb_date < chang_date & disb_list$tenor == 6 ] = 0.0129
disb_list$sever_fee_rate[disb_list$disb_date < chang_date & disb_list$tenor == 9 ] = 0.0183
disb_list$sever_fee_rate[disb_list$disb_date < chang_date & disb_list$tenor == 12] = 0.0236
#8.11后
disb_list$sever_fee_rate[disb_list$disb_date >=chang_date & disb_list$tenor == 3 ] = 0.0055
disb_list$sever_fee_rate[disb_list$disb_date >=chang_date & disb_list$tenor == 6 ] = 0.0095
disb_list$sever_fee_rate[disb_list$disb_date >=chang_date & disb_list$tenor == 9 ] = 0.0134
disb_list$sever_fee_rate[disb_list$disb_date >=chang_date & disb_list$tenor == 12] = 0.0173
#计算服务费c
disb_list$loan_amount = as.numeric(disb_list$loan_amount)
disb_list$ser_fee = disb_list$loan_amount * disb_list$sever_fee_rate
##导出还款总额
sql_str = "SELECT    s.application_id,
                 SUM(s.due_amount)     AS amount
         FROM    source_1   s
        WHERE    s.lender_id = 238
          AND    s.due_type  IN ('本金','利息')
      GROUP BY   s.application_id,
                 s.loan_amount,
                 s.tenor;"
sql_str = gsub('\n'," ",sql_str)
sql_str = gsub('source_1',data_source_1,sql_str)
dues_tot_list =sqlQuery(ods_hive,sql_str,stringsAsFactors = F,as.is = T)
dues_tot_list$amount = as.numeric(dues_tot_list$amount)
##合并表
disb_list = merge(disb_list,dues_tot_list,by = c("application_id"),all.x = T)
disb_list$psinstmamt2 = (disb_list$amount + disb_list$ser_fee)/disb_list$tenor
disb_list$psinstmamt2 <- (disb_list$amount + disb_list$ser_fee)/disb_list$tenor
disb_list$tenor = as.numeric(disb_list$tenor)
##计算月利率
disb_list$rate_2 = apply(disb_list[c("tenor","loan_amount","psinstmamt2")],1, function(row) discount.rate(n=row["tenor"],pv=-row["loan_amount"],fv=0,pmt=row["psinstmamt2"],type=0))
##取期数维表
dim_tenor = sqlQuery(ods_hive,"select * from dw_udm.dim_tenor",stringsAsFactors=TRUE) 
##按期数发散
disb_list = merge(disb_list, dim_tenor,by = c("tenor"))
## 计算期初本金
retain_prin_fun = function(loan_amount,rate,psinstmamt,pay_seq){
  retain_prin = loan_amount
  if (pay_seq > 1){
    for(i in  2:pay_seq){
      interest = round(retain_prin * rate,2)
      m_prin = psinstmamt - interest
      retain_prin = retain_prin - m_prin
    }
  }
  return(retain_prin)
}

##计算每期期初本金
disb_list$retain_prin = mapply(retain_prin_fun,disb_list$loan_amount,disb_list$rate_2,disb_list$psinstmamt2,disb_list$pay_seq)
disb_list$retain_prin = round(disb_list$retain_prin,2)
##计算每期利息
disb_list$interest_2    = round(disb_list$retain_prin * disb_list$rate_2,2)
##计算每期本金
disb_list$psinstmamt2   = round(disb_list$psinstmamt2,2)
disb_list$rep_prin_2    = disb_list$psinstmamt2 - disb_list$interest_2
##数据修正
disb_list   = sqldf("select   application_id,
                              disb_date,
                              loan_amount,
                              pay_seq,
                              case when pay_seq = 1 then ser_fee else 0 end AS ser_fee,
                              interest_2,
                              case when tenor = pay_seq then retain_prin + interest_2 else psinstmamt2 end AS psinstmamt2
                       from   disb_list  s;")

##导出实际还款
sql_str = "SELECT   s.application_id,
                    s.due_date,
                    s.tenor,
                    TRIM(regexp_replace(regexp_extract(s.`index`,'.*/',0),'/',''))       AS  pay_seq,
                    SUM(CASE WHEN s.due_type  IN ('利息') THEN s.due_amount ELSE 0 END)    AS interest_1,
                    SUM(s.due_amount)                                                    AS psinstmamt1
            FROM    source_1   s
           WHERE    s.lender_id = 238
             AND    s.due_type  IN ('本金','利息')  
         GROUP BY   s.application_id,
                    s.due_date,
                    s.tenor,
                    TRIM(regexp_replace(regexp_extract(s.`index`,'.*/',0),'/',''));
          "
sql_str = gsub('\n'," ",sql_str)
sql_str = gsub('source_1',data_source_1,sql_str)
rpt_dues =sqlQuery(ods_hive,sql_str,stringsAsFactors = F,as.is = T)
rpt_dues$due_date  = as.Date(rpt_dues$due_date)
rpt_dues$pay_seq  = as.integer(rpt_dues$pay_seq)
rpt_dues$interest_1 = as.numeric(rpt_dues$interest_1)
rpt_dues$psinstmamt1 = as.numeric(rpt_dues$psinstmamt1)
##数据整合
rep_var = sqldf("select s1.application_id,
                          s1.disb_date,
                          s2.due_date,
                          s1.loan_amount,
                          s2.tenor,
                          s1.ser_fee,
                          s1.interest_2 - s2.interest_1  AS per_ser_fee,
                          s2.psinstmamt1                 AS amount_1,
                          s1.psinstmamt2                 AS amount_2
                     from disb_list  s1
                left join rpt_dues   s2 on s1.application_id = s2.application_id and s1.pay_seq = s2.pay_seq;")

var_tot =sqldf("SELECT  s.disb_date,
               s.due_date,
               SUM(s.per_ser_fee)  AS ser_fee,
               sum(s.amount_1)     AS amount_1,
               sum(s.amount_2)     AS amount_2
               FROM    rep_var   s 
               GROUP BY    s.disb_date,
               s.due_date
               ORDER BY    s.disb_date,
               s.due_date;")
##导出报告
names(rep_var) = c('申请编号','放款日期','还款日期','放款金额','放款期限','服务费合计','服务费分摊','每期实际本息','每期分摊服务费本息')
names(var_tot) = c('放款日期','还款日期','服务费分摊','实际本息','分摊服务费本息')
file_name = paste("F:/报表/众安/利差清单",end_date,".csv",sep="")
write.csv(rep_var,file_name,row.names=FALSE)
file_name = paste("F:/报表/众安/利差汇总数据",end_date,".xlsx",sep="")
write.xlsx(var_tot,file_name, sheetName="利差汇总数据",col.names=TRUE, row.names=TRUE, append=TRUE, showNA=TRUE)
rm(list = ls())


