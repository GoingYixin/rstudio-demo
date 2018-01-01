###ָ��Ҫʹ�õĿ�
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
###����impla
ods_hive<-odbcConnect("impala") 
###��������
end_date = as.character(floor_date(Sys.Date(),"month")-1)
end_date_str = gsub('-',"",end_date)
end_date_str = substr(end_date_str,1,6)
data_source_1 = paste("dm_finance.rpt_dues_lender",end_date_str,sep="_")
data_source_2 = paste("dm_finance.rpt_disburse_repay",end_date_str,sep="_")

#ȡ������
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
##�������
chang_date = as.Date('2017-08-11')
#8.11ǰ
disb_list$sever_fee_rate[disb_list$disb_date < chang_date & disb_list$tenor == 3 ] = 0.0074
disb_list$sever_fee_rate[disb_list$disb_date < chang_date & disb_list$tenor == 6 ] = 0.0129
disb_list$sever_fee_rate[disb_list$disb_date < chang_date & disb_list$tenor == 9 ] = 0.0183
disb_list$sever_fee_rate[disb_list$disb_date < chang_date & disb_list$tenor == 12] = 0.0236
#8.11��
disb_list$sever_fee_rate[disb_list$disb_date >=chang_date & disb_list$tenor == 3 ] = 0.0055
disb_list$sever_fee_rate[disb_list$disb_date >=chang_date & disb_list$tenor == 6 ] = 0.0095
disb_list$sever_fee_rate[disb_list$disb_date >=chang_date & disb_list$tenor == 9 ] = 0.0134
disb_list$sever_fee_rate[disb_list$disb_date >=chang_date & disb_list$tenor == 12] = 0.0173
#��������c
disb_list$loan_amount = as.numeric(disb_list$loan_amount)
disb_list$ser_fee = disb_list$loan_amount * disb_list$sever_fee_rate
##���������ܶ�
sql_str = "SELECT    s.application_id,
                 SUM(s.due_amount)     AS amount
         FROM    source_1   s
        WHERE    s.lender_id = 238
          AND    s.due_type  IN ('����','��Ϣ')
      GROUP BY   s.application_id,
                 s.loan_amount,
                 s.tenor;"
sql_str = gsub('\n'," ",sql_str)
sql_str = gsub('source_1',data_source_1,sql_str)
dues_tot_list =sqlQuery(ods_hive,sql_str,stringsAsFactors = F,as.is = T)
dues_tot_list$amount = as.numeric(dues_tot_list$amount)
##�ϲ���
disb_list = merge(disb_list,dues_tot_list,by = c("application_id"),all.x = T)
disb_list$psinstmamt2 = (disb_list$amount + disb_list$ser_fee)/disb_list$tenor
disb_list$psinstmamt2 <- (disb_list$amount + disb_list$ser_fee)/disb_list$tenor
disb_list$tenor = as.numeric(disb_list$tenor)
##����������
disb_list$rate_2 = apply(disb_list[c("tenor","loan_amount","psinstmamt2")],1, function(row) discount.rate(n=row["tenor"],pv=-row["loan_amount"],fv=0,pmt=row["psinstmamt2"],type=0))
##ȡ����ά��
dim_tenor = sqlQuery(ods_hive,"select * from dw_udm.dim_tenor",stringsAsFactors=TRUE) 
##��������ɢ
disb_list = merge(disb_list, dim_tenor,by = c("tenor"))
## �����ڳ�����
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

##����ÿ���ڳ�����
disb_list$retain_prin = mapply(retain_prin_fun,disb_list$loan_amount,disb_list$rate_2,disb_list$psinstmamt2,disb_list$pay_seq)
disb_list$retain_prin = round(disb_list$retain_prin,2)
##����ÿ����Ϣ
disb_list$interest_2    = round(disb_list$retain_prin * disb_list$rate_2,2)
##����ÿ�ڱ���
disb_list$psinstmamt2   = round(disb_list$psinstmamt2,2)
disb_list$rep_prin_2    = disb_list$psinstmamt2 - disb_list$interest_2
##��������
disb_list   = sqldf("select   application_id,
                              disb_date,
                              loan_amount,
                              pay_seq,
                              case when pay_seq = 1 then ser_fee else 0 end AS ser_fee,
                              interest_2,
                              case when tenor = pay_seq then retain_prin + interest_2 else psinstmamt2 end AS psinstmamt2
                       from   disb_list  s;")

##����ʵ�ʻ���
sql_str = "SELECT   s.application_id,
                    s.due_date,
                    s.tenor,
                    TRIM(regexp_replace(regexp_extract(s.`index`,'.*/',0),'/',''))       AS  pay_seq,
                    SUM(CASE WHEN s.due_type  IN ('��Ϣ') THEN s.due_amount ELSE 0 END)    AS interest_1,
                    SUM(s.due_amount)                                                    AS psinstmamt1
            FROM    source_1   s
           WHERE    s.lender_id = 238
             AND    s.due_type  IN ('����','��Ϣ')  
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
##��������
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
##��������
names(rep_var) = c('������','�ſ�����','��������','�ſ���','�ſ�����','����Ѻϼ�','����ѷ�̯','ÿ��ʵ�ʱ�Ϣ','ÿ�ڷ�̯����ѱ�Ϣ')
names(var_tot) = c('�ſ�����','��������','����ѷ�̯','ʵ�ʱ�Ϣ','��̯����ѱ�Ϣ')
file_name = paste("F:/����/�ڰ�/�����嵥",end_date,".csv",sep="")
write.csv(rep_var,file_name,row.names=FALSE)
file_name = paste("F:/����/�ڰ�/�����������",end_date,".xlsx",sep="")
write.xlsx(var_tot,file_name, sheetName="�����������",col.names=TRUE, row.names=TRUE, append=TRUE, showNA=TRUE)
rm(list = ls())


