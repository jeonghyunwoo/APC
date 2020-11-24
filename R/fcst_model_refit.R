# period효과 예측모형 재개발 프로그램 
# period효과는 경제전반의 리스크, 회사 정책변경(심사기준 등) 등을 반영함 
# caret::train에서 제외할 변수는 data= 에서 제외할것
library(caret)
library(doParallel)

# 모형 tuning or refit ####
devd = filter(mdf0,!is.na(prr3))
te = tail(devd,12) %>% data.frame()
tr = setdiff(devd,te) %>% data.frame()

ctrl = trainControl(method='cv',number=5)
registerDoParallel(4)
rf = train(prr3~.,data=select(tr,-date,-target),method='ranger',trControl=ctrl)
stopImplicitCluster()

pred = predict(rf, te)

saveRDS(rf,'data/apc_model/rf_v2.rds')

# 끝
