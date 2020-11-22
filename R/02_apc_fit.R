library(Epi)
library(tidyverse)
library(lubridate)
library(data.table)
# (1)모형적합용 데이터가공 ####
dfraw = read_rds('data/dfraw.rds')
# mob : months on books, 신규대출후 경과개월 
a = dfraw %>% 
  mutate(P = dense_rank(마감년월),
         C = dense_rank(대출년월),
         A = mob,
         pdate = ymd(str_c(마감년월,'01')),
         cdate = ymd(str_c(대출년월,'01'))) %>% 
  data.table()
pdate = select(a,마감년월,Per=P,date=pdate) %>% distinct() %>% data.table()
cdate = select(a,대출년월,C,date=cdate) %>% distinct() %>% data.table()

# (2)모형적합 (상각후 연체 기준) ####
# parm = 'ACP': period는 추세(trend) 없음 가정(cohort는 추세있음)
# parm = 'APC': cohort는 추세(trend) 없음 가정(period는 추세있음)
df1 = select(a,마감년월,유지n,P,A,D=연체2pn,Y=유지n) %>% 
  filter(A>=3) %>% data.table()
apc_mod = apc.fit(df1,dist='poisson',model='factor',dr.extr='Holford',
                  ref.p = 13,
                  # 2008년1월을 기준으로 함(2007.1=1, 2008.1=13)
                  parm = 'ACP', scale=100)
# (3)결과가공 ####
age1 = data.table(apc_mod$Age)
per1 = data.table(apc_mod$Per) %>% left_join(pdate,by='Per') %>% 
  rename(prr=`P-RR`,lo=`2.5%`,up=`97.5%`) %>% 
  data.table()
coh1 =data.table(apc_mod$Coh) %>% 
  mutate(C = dense_rank(Coh)) %>% 
  left_join(cdate,by='C') %>% 
  rename(crr=`C-RR`,lo=`2.5%`,up=`97.5%`) %>% 
  data.table()
# (4)결과저장 ####
save(age1,per1,coh1,file='data/pac1.rda') # 상각후 건기준(ACP):P기준=2008.1
saveRDS(apc_mod,'data/apc_mod.rds')
print('2.APC fitting done')
