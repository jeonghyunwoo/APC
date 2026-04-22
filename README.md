# Retail Credit Risk Monitoring using Age-Period-Cohort (APC)

가계대출 연체율은 단순히 “경기가 나빠져서” 올라가는 것이 아니라,
포트폴리오의 경과월(MOB), 취급 시점의 빈티지 특성, 월별 거시환경 변화가 함께 섞여 나타납니다.

이 프로젝트는 Age-Period-Cohort(APC) 모델을 활용해 연체율을 세 가지 시간축으로 분해하고,
그중 거시환경 변화에 가까운 `Period effect`를 별도 모니터링 지표로 활용하기 위한 분석입니다.

---

## 1. Business Question

가계대출 연체율 상승이 발생했을 때, 아래를 구분하는 것이 핵심입니다.

- 포트폴리오 숙성(Seasoning) 영향인가?
- 특정 취급 시점(Vintage/Cohort)의 질적 차이인가?
- 시장 전반의 경기/신용환경 변화인가?

단순 연체율만 보면 이 세 요인이 섞여 있어,
실제 리스크가 커진 것인지 구조적 착시인지 판단하기 어렵습니다.

---

## 2. Why APC?

APC 모델은 연체율 변화를 다음 세 축으로 분해합니다.

- **Age**: 대출 실행 후 경과 개월 수(MOB)
- **Period**: 연체율이 관측된 시점
- **Cohort**: 대출이 취급된 시점

이렇게 분해하면,
- 시간이 지나면서 자연스럽게 나타나는 숙성 효과와
- 특정 빈티지의 특성,
- 그리고 시장 전반의 공통 충격

을 구분해서 볼 수 있습니다.

이 프로젝트의 핵심은 `Period effect`를 리스크 모니터링 지표로 해석하는 데 있습니다.

---

## 3. Data

분석 단위는 월별 집계 데이터이며, 주요 구조는 다음과 같습니다.

- 마감년월
- 대출년월
- MOB (months on books)
- 유지 건수 / 잔액
- 연체 건수 / 연체 잔액
- 상각 반영 건수 / 금액

추가적으로 period effect 예측을 위해 다음과 같은 외부/보조 지표를 결합했습니다.

- CB 기반 저신용/다중채무/잠재부실 관련 비중
- 연체 전이율
- KB 전세지수
- 한국은행 거시지표
- 이동평균 및 추세 파생변수

---

## 4. Methodology

### Step 1. Raw data aggregation
월마감 기준 포트폴리오를 `마감년월 × 대출년월 × MOB` 단위로 집계합니다.

### Step 2. APC model fitting
Poisson 기반 APC 모델을 적합해
- Age effect
- Period effect
- Cohort effect

를 분리합니다.

### Step 3. Period effect forecasting
분리된 Period effect를 타깃으로 두고,
거시/CB/전이율 변수를 이용해 선행 예측 모델을 구축합니다.

### Step 4. Monitoring use
예측된 period risk level을 통해
단순 연체율보다 앞서 시장성 리스크 신호를 점검할 수 있도록 설계했습니다.

---

## 5. Repo Structure

```text
R/
├─ 01_rawdata.R        # 월마감 대출 포트폴리오 집계
├─ 02_apc_fit.R        # APC 적합 및 Age/Period/Cohort 효과 산출
├─ 03_forecast.R       # Period effect 예측용 변수 생성 및 예측
└─ fcst_model_refit.R  # 예측모형 재학습
