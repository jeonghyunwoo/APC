# Retail Credit Risk Monitoring using Age-Period-Cohort (APC)

Retail delinquency rates do not move for a single reason.  
They are jointly shaped by portfolio seasoning, origination vintage quality, and changes in the broader economic environment.

This project applies an Age-Period-Cohort (APC) framework to decompose delinquency dynamics into three time dimensions and to isolate the **Period effect** as a practical monitoring signal for portfolio risk.

---

## 1. Business Context

When delinquency rises, the key question is not simply **whether** it increased, but **why**.

A higher delinquency rate may reflect:

- natural seasoning as accounts age
- weaker credit quality from specific origination vintages
- deterioration in the external macro or credit environment

If these effects are not separated, headline delinquency can be misleading.  
This makes it difficult to distinguish genuine risk deterioration from portfolio-composition effects.

---

## 2. Objective

The objective of this project is to build a monitoring framework that can:

- decompose delinquency movements into structural components
- isolate the part most closely associated with market-wide conditions
- support earlier and more interpretable risk diagnosis than a simple delinquency-rate trend

In practice, the focus was placed on using the **Period effect** as a monitoring indicator for external risk conditions.

---

## 3. Why APC?

The APC framework separates delinquency behavior into three dimensions:

- **Age**: months on books (MOB) after origination
- **Period**: calendar time when delinquency is observed
- **Cohort**: origination period (vintage)

This decomposition helps distinguish:

- seasoning effects from portfolio aging
- vintage effects from differences in underwriting or borrower mix
- common time effects that may reflect macroeconomic or market stress

For risk monitoring purposes, this structure is useful because it turns a single blended delinquency number into a more interpretable signal.

---

## 4. Data Structure

The core dataset is organized at a monthly aggregated level, including variables such as:

- observation month
- origination month
- months on books (MOB)
- active account / balance measures
- delinquent account / balance measures
- charge-off related counts and balances

To forecast the Period effect, the project also incorporates auxiliary indicators such as:

- CB-based risk composition measures
- delinquency transition metrics
- KB jeonse index
- Bank of Korea macroeconomic indicators
- moving-average and trend-derived variables

---

## 5. Methodology

### Step 1. Raw data aggregation
Portfolio performance data is aggregated by `observation month × origination month × MOB`.

### Step 2. APC model fitting
A Poisson-based APC model is fitted to separate:

- Age effect
- Period effect
- Cohort effect

### Step 3. Period effect forecasting
The extracted Period effect is used as a target variable, with macroeconomic and credit-related indicators incorporated as predictors.

### Step 4. Monitoring application
The resulting framework is intended to support ongoing monitoring by providing a risk signal that is more interpretable than the raw delinquency rate alone.

---

## 6. Repository Structure

```text
R/
├─ 01_rawdata.R        # Monthly portfolio aggregation
├─ 02_apc_fit.R        # APC fitting and effect extraction
├─ 03_forecast.R       # Period effect forecasting pipeline
└─ fcst_model_refit.R  # Forecast model refit
```

---

## 7. Key Takeaways

- Delinquency trends should not be interpreted at face value.
- APC decomposition is useful for separating seasoning, vintage, and macro-related influences.
- The **Period effect** can serve as a more interpretable monitoring signal for external risk conditions.
- This approach is especially helpful when portfolio growth, runoff, or mix shift distorts the headline delinquency rate.

---

## 8. Practical Relevance

This is not just a modeling exercise.  
It is closer to a practical risk-monitoring framework designed to answer a real business question:

**Is portfolio risk genuinely worsening, or is the observed delinquency change being driven by structural composition effects?**

That distinction matters in areas such as:

- monthly risk reporting
- early warning monitoring
- portfolio-quality diagnosis
- stress testing support
- management communication on the drivers of delinquency

---

## 9. Reproducibility Notes

This repository is a simplified public version of an internal risk-monitoring project.

Because the original work used internal portfolio data and internal connection logic, the public version does not include:

- source data
- internal database access logic
- some environment-specific execution details

The repository is therefore intended to communicate:

- the analytical structure
- the modeling logic
- the monitoring concept
- the practical use case

rather than to provide a fully reproducible end-to-end public pipeline.

---

## 10. Tech Stack

- R
- tidyverse
- data.table
- Epi
- caret / ranger

---

## 11. Future Improvements

- add synthetic sample data for public reproducibility
- include example charts for Age / Period / Cohort effects
- document the interpretation of Period effect in more detail
- clean environment-specific code for a more portable workflow
