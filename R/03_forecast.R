# period effect 예측모형 
library(tidyverse)
library(lubridate)
library(clipr)
library(scales)
windowsFonts(ng=windowsFont('NanumGothic'))
# 1.예측모형 후보항목 생성 ####
# (1) dq1: 신용대출 연체율 ####
# dfraw = read_rds('data/dfraw.rds')
dq1 = group_by(dfraw,마감년월) %>% 
  summarise(건연체율 = sum(연체2pn)/sum(유지n),
            액연체율 = sum(연체2pb)/sum(유지b)) %>% 
  mutate(date = ymd(str_c(마감년월,'01'))) %>% 
  select(-마감년월) %>% 
  data.table()
saveRDS(dq1,'data/dq1.rds')
# (2) dfcbr: cb 8~10비중,다중채무자비중,저신용업권비중,잠재부실률 ####
mm = 202002
sql0 = "
with cb as(
 select to_char(기준일자,'yyyymm') mon
 ,고객id
 ,max(cb등급) keep(dense_rank last order by bs_fsr_dt) nice
 ,max(미상환대출건수) keep(dense_rank last order by 기준일자) 미상환대출건수
 ,max(미해제연체일수) keep(dense_rank last order by 기준일자) 미해제연체일수
 ,max(저신용대출금액) keep(dense_rank last order by 기준일자) 저신용대출금액
 from cb마감테이블
 where to_char(기준일자,'yyyymm') between '201703' and '{mm}'
 group by to_char(기준일자,'yyyymm'), 고객id
)

select 마감년월
,case when b.nice is null then d.nice else b.nice end nice
,case when b.미상환대출건수>=3 then 1 else 0 end 다중채무
,case when b.미해제연체일수>=30 then 1 else 0 end 잠재부실
,case when b.저신용대출금액 >0 then 1 else 0 end 저신용업권
,sum(a.대출잔액)/100000000 잔액
,count(*) 건수 
from 마감테이블 a
 left join cb b 
  on a.마감년월 = b.mon
  and a.고객id = b.고객id
 left join 접수테이블 c
  on a.접수번호 = c.접수번호
  and c.업무구분 = '01' --신규
 left join 신용정보테이블 d
  on c.신청id = d.신청id
where a.마감년월 between '201703' and '{mm}'
 and a.상품분류 in ('신용')
 and a.대출잔액 >0
group by 마감년월
,case when b.nice is null then d.nice else b.nice end nice
,case when b.미상환대출건수>=3 then 1 else 0 end 다중채무
,case when b.미해제연체일수>=30 then 1 else 0 end 잠재부실
,case when b.저신용대출금액 >0 then 1 else 0 end 저신용업권
"
sql = str_glue(sql0)

source('d:/r/conn.r') # db connection 프로그램
dfcb_add = dbGetQuery(con,sql) %>% select_all(tolower) %>% as_tibble()
dfcb_add = dfcb_add %>% 
  mutate_at(vars(마감년월,nice,다중채무,잠재부실,저신용업권),as.integer)
# 기존 데이터에 추가 
dfcb = read_rds('data/dfcb.rds')
addm = c(first(dfcb_add$마감년월),setdiff(dfcb_add$마감년월,dfcb$마감년월))
dfcb_add = filter(dfcb_add,마감년월 %in% addm)
dfcb = bind_rows(filter(dfcb,!(마감년월 %in% addm)),dfcb_add)
dfcbr = filter(dfcb, !is.na(nice)) %>% 
  group_by(마감년월) %>% 
  summarise(cb810b = sum(ifelse(nice==0L|between(nice,8,10),잔액,0)),
            다중채무b = sum(ifelse(다중채무==1L,잔액,0)),
            저신용b = sum(ifelse(저신용업권==1L,잔액,0)),
            잠재부실b = sum(ifelse(잠재부실==1L,잔액,0)),
            bal = sum(잔액),
            cb810r = cb810b/bal,
            다중채무r = 다중채무b/bal,
            저신용r = 저신용b/bal,
            잠재부실r = 잠재부실b/bal) %>% 
  mutate(date = ymd(str_c(마감년월,'01')))
saveRDS(dfcbr,'data/dfcbr.rds')

# (3) dqtr: 전이율01, 전이율02 ####
sql = "
select 마감년월
,sum(case when ppd=0 then 1 else 0 end) 정상
,sum(case when ppd=1 then 1 else 0 end) p1
,sum(case when ppd=2 then 1 else 0 end) p2
,sum(case when ppd=3 then 1 else 0 end) p3
,sum(case when ppd=4 then 1 else 0 end) p4
from 마감테이블
where 마감년월 between '200601' and '{mm}' --전이율01 기준 최종월(최종마감월)
 and to_char(대출일자,'yyyymm') >='200601'
 and 상품분류 in ('신용')
 and 대출잔액 >0
group by 마감년월 
"
t = dbGetQuery(con,sql) %>% select_all(tolower) %>% as_tibble() %>% 
  arrange(마감년월)
dqtr = transmute(t,date=ymd(str_c(마감년월,'01')),
                 전이율01 = 100*p1/lag(정상),
                 전이율02 = 100*p2/lag(정상,2),
                 전이율03 = 100*p3/lag(정상,3),
                 전이율04 = 100*p4/lag(정상,4)) %>% 
  arrange(date)
saveRDS(dqtr,'data/전이율.rds')

# (4) kbjs: kb아파트전세지수(전국) ####
# 원천: (월간)kb주택가격동향_시계열 
# 전세APT 시트: 전국-항목명 빼고 복사 
kbjs = read_clip_tbl(header=F)
kbjs = kbjs %>% 
  rename(kb아파트전세지수_전국 = V1) %>% 
  add_column(date = seq(19860101,by='month',length.out=nrow(.)),.before=1) %>% 
  filter(year(date) >=2005) %>% 
  as_tibble()
saveRDS('data/kbjs.rds')

# (5) 한국은행 지표 ####
# 지표 불러오기 프로그램: 인터넷망 z:/proj/apc_bokdf.ipynb
bok0 = read_csv('data/bokdf.csv',col_type=cols(.default = col_character())) %>% 
  mutate_at(vars(item_name1,item_name2,item_name3,stat_name),
            ~str_conv(.,'cp949'))
bok = bok3 %>% filter(item_code1 %in% c('I16E','MO3AC','FMB')) %>% 
  select(time,item_name1,data_value) %>% 
  pivot_wider(names_from=item_name1,values_from=data_value,
              values_fn=list(data_value=max)) %>% 
  set_names(c('mm','기대인플레이션율','은행신용카드대연체율','선행지수cc')) %>% 
  mutate(date = ymd(str_c(mm,'01'))) %>% 
  arrange(mm) %>% 
  fill(기대인플레이션율:선행지수cc,.direction='down') %>% 
  mutate_at(vars(-mm,-date),as.numeric)
saveRDS(bok,'data/bok.rds')

# 2.모형화 데이터 생성 ####
# age1, per1, coh1 
if(!exists('age1')|!exists('per1')|!exists('coh1')){
  load('data/apc1.rda')
}
# dq1: 건연체율, 액연체율 
if(!exists('dq1')) dq1 = read_rds('data/dq1.rds')
# dfcbr: 다중채무비중(다중채무r), 저신용비중(cb810r), 잠재부실률 등 
if(!exists('dfcbr')) dfcbr = read_rds('data/dfcbr.rds')
# kbjs: kb아파트전세지수_전국
if(!exists('kbjs')) kbjs = read_rds('data/kbjs.rds')
# bok: 기대인플레이션율, 선행지수cc(순환변동치), 은행신용카드대연체율
if(!exists('bok')) bok = read_rds('data/bok.rds')
# df0: 변수종합 및 가공, 기간이 가장 긴 bok 기준으로 left join
ma3var = setNames(c('prr','선행지수cc','다중채무r','은행신용카드대연체율'), # 항목레이블
                  c('prr3','ma3_2','ma3_3','ma3_4')) # 항목명 
ma6var = setNames(c('은행신용카드대연체율','전이율01'),
                  c('ma6_4','ma6_5'))
vivar = setNames(c('전이율01','전이율02','선행지수cc','ma3_4','다중채무r'),
                 c('vi7','vi8','vi14','vi19','via4'))
xivar = setNames(c('기대인플레이션율','kb아파트전세지수_전국'),
                 c('xii31','xii28'))
# 최종예측월: 예측치 산출을 위해 전 2개월치가 업데이트 안되는 경우 직전 데이터값으로 imputing
library(zoo)
df0 = date %>% 
  left_join(bok,by='date') %>% 
  left_join(kbjs,by='date') %>% 
  left_join(per1,by='date') %>% 
  left_join(dfcbr,by='date') %>% 
  left_join(dqtr,by='date') %>% 
  left_join(dq1,by='date') %>% 
  mutate_at(ma3var,funs(rollapply(.,3,mean,align='right',fill=NA))) %>% # 3개월이동평균
  mutate_at(ma6var,funs(rollapply(.,6,mean,align='right',fill=NA))) %>% # 6개월이동평균
  mutate_at(vivar,funs(rollapply(.,3,mean,align='right',fill=NA)/
                         rollapply(.,6,mean,align='right',fill=NA))) %>% # 6개월 추세변수
  mutate_at(xivar,funs(rollapply(.,6,mean,align='right',fill=NA)/
                         rollapply(.,12,mean,align='right',fill=NA))) %>% # 12개월추세변수 
  select(date,prr3,xii28,xii31,vi7:via4,ma3_2,ma3_4,ma6_4:ma6_5)
nam.lag6 = setNames(names(df0)[-c(1,2)],paste(names(df0)[-c(1,2)],'lag6',sep='.')) # lag6 변수명
nam.lag7 = setNames(names(df0)[-c(1,2)],paste(names(df0)[-c(1,2)],'lag7',sep='.')) # lag7 변수명
nam.lag8 = setNames(names(df0)[-c(1,2)],paste(names(df0)[-c(1,2)],'lag8',sep='.')) # lag8 변수명
nam.lag9 = setNames(names(df0)[-c(1,2)],paste(names(df0)[-c(1,2)],'lag9',sep='.')) # lag9 변수명
df0lag = df0 %>% 
  # 모형예측변수 6개월~9개월 lagged변수화 
  mutate_at(nam.lag6,~lag(.,6)) %>% 
  mutate_at(nam.lag7,~lag(.,7)) %>% 
  mutate_at(nam.lag8,~lag(.,8)) %>% 
  mutate_at(nam.lag9,~lag(.,9)) %>% 
  filter(date >= ymd(20070301)) # prr3 최초값 있는 200703 이후만 남김 
# df0: 기울기(slope) 및 상승/보합/하락 판단변수(updn) 생성 
slopes = numeric()
deters = integer()
ups = numeric()
lws = numeric()
dfs = select(df0lag,date,prr3) %>% 
  filter(!is.na(prr3))
for(i in 1:(nrow(dfs)-5)){
  x = i:(i+5)
  y = dfs$prr3[x]
  fit = lm(y~x)
  slope = coef(fit)[[2]]
  up = confint(fit,level=.9)[2,2]
  lw = confint(fit,level=.9)[2,1]
  deter = ifelse(sign(up*lw)<0,0L,sign(slope))
  # slope의 90%신뢰구간이 0을 포함하면 보합(즉, 상한[up]과 하한[lw]의 부호가 다른경우)
  deter = as.integer(deter)
  slopes = c(slopes,slope)
  deters = c(deters,deter)
  ups = c(ups,up) # 상승 판단기준 
  lws = c(lws,lw) # 하락 판단기준 
}
dfs = mutate(dfs,slope=c(rep(NA,5),slopes),
             sup = c(rep(NA,5),ups),
             slw = c(rep(NA,5),lws),
             deter = c(rep(NA,5),deters)) # lag6변수로 예측했으므로 
dfs1 = left_join(df0lag,select(dfs,-prr3),by='date') %>% 
  select(date,prr3,slope,sup,slw,deter,everything()) %>% 
  mutate(updn = ifelse(deter==1L,'up',
                       ifelse(deter==-1L,'dn','flat')),
         updn = factor(updn),
         updn1 = ifelse(deter==0L,sign(slope)*0.5,deter)) %>% 
  filter(year(date)>=2008)
  # 예측기간의 항목값이 NA인 feature들은 제거함
  # 제거대상: xii28:ma6_5.lag9
fnam = select(dfs1,xii28:ma6_5.lag9)
exvar = names(fnam)[sapply(fnam,function(x) is.na(last(x)))]
# 모형 설명변수중 값이 없는 변수 체크 ####
df1 = select(dfs1,-one_of(exvar))
omit_vars = setdiff(exvar,names(df1))
if(length(omit_vars)==0){
  print('빠진변수 없음')
} else {
  print(omit_vars)
}

# updn: 1/0.5/-1
# updn1: up/flat/dn
# updn2: 1/0.5/-0.5/-1
saveRDS(df1,'data/df1.rds')
print('모형화 데이터 생성 done')

# mdf0: 모형예측용 데이터 ####
mdf0 = df1 %>% 
  mutate(target = ifelse(updn=='up',1,-1),
         target = factor(target)) %>% 
  select(date,xii28.lag6:ma6_5.lag9,prr3,target) %>% 
  select(-starts_with('ma3_2')) %>% # 선행지수cc 3개월 이동평균 제외
  select(-starts_with('vi14')) %>% # 선행지수cc 추세 제외
  filter_at(vars(xii28.lag6:ma6_5.lag9),all_vars(!is.na(.)))

library(caret)
# 주로 누락되는 변수 
# vi7.lag6 : 전이율01의 lag6
# vi8.lag6 : 전이율02의 lag6
# ma6_5.lag6: 전이율01 이동평균의 lag6 
# xii31.lag6: 기대인플레이션율
# vi19.lag6: 카드론연체3ma
# ma3_4.lag6: 은행신용카드대연체율
# ma6_4.lag6: 은행신용카드대연체율 

# fcst: 예측치산출 ####
rfm = read_rds('data/rfm_v2.rds') # 예측모델(randomforest)
fcst = predict(rfm, mdf0)
fcstall = select(mdf0, date,prr3) %>% 
  add_column(pred = fcst)
fpp = pivot_longer(fcstall,cols=-date,names_to='key',values_to='value') %>% 
  mutate(val = round(value*100,1))
# 예측치 저장
saveRDS(fpp,str_glue('data/apc_fcst/fpp{mm}.rds'))
fcstval = filter(fpp,key=='pred',date==last(date)) %>% pull(val)
fpp %>% 
  ggplot(aes(date,val,color=key,lty=key))+
  geom_line(size=.8)+
  ggrepel::geom_text_repel(aes(label=comma(val,accuracy=.1)),family='ng',
                               size=3,color='black',hjust=-.5,
                               data = . %>% filter(date==max(date)))+
  annotate('text',x=ymd(20201201),y=c(118,177,229),
           label=str_c('Level',c(1,2,3),':',c(114,173,225)),
           family='ng',size=3)+
  scale_linetype_manual(values=c(1,1))+
  scale_x_date(expand = expand_scale(mult=c(.1,.1)))+
  geom_hline(yintercept = c(114,173,225),color='firebrick',lty=2)