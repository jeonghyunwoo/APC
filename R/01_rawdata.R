library(odbc)
library(tidyverse)
# db connection ####
con = dbConnect(...)
# rawdata sql #### 
sql = "
select 마감년월
,to_char(대출일자,'yyyymm') 대출년월
,months_between(to_date(마감년월||'01'),to_date(to_char(대출일자,'yyyymm')||'01')) mob
,sum(case when 대출잔액>0 then 1 else 0 end) 유지n
,sum(case when ppd >=1 and 대출잔액 >0 then 1 else 0 end) 연체1pn
,sum(case when ppd >=2 and 대출잔액 >0 then 1 else 0 end) 연체2pn
,count(*) 유지상각n
,sum(case when ppd >=1 or 상각액>0 then 1 else 0 end) 연체상각1pn
,sum(case when ppd >=2 or 상각액>0 then 1 else 0 end) 연체상각2pn
,sum(ln_bal)/100000000 유지b
,sum(case when ppd>=1 then 대출잔액 else 0 end)/100000000 연체1pb
,sum(case when ppd>=2 then 대출잔액 else 0 end)/100000000 연체2pb
,sum(대출잔액+상각액)/100000000 유지상각b
,sum(case when ppd>=1 or 상각액>0 then 대출잔액+상각액 else 0 end)/100000000 연세상각1
,sum(case when ppd>=2 or 상각액>0 then 대출잔액+상각액 else 0 end)/100000000 연체상각2
from 대출월마감tbl
where 마감년월 >='200701'
 and 대출소분류 in ('11','12')
 and (대출잔액>0 or 상각액>0)
 and to_char(대출일자,'yyyymmdd') >='200601'
group by  마감년월
,to_char(대출일자,'yyyymm')
,months_between(to_date(마감년월||'01'),to_date(to_char(대출일자,'yyyymm')||'01'))
"
# data 산출 ####
dfraw = dbGetQuery(con,sql) %>% as_tibble() %>% select_all(tolower)
saveRDS(dfraw,'data/dfraw.rds')
print('1.APC rawdata done')