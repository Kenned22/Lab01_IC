---
title: "Lab 3 Checklist"
---

**Goal:** Create a cleaned version of the dataset with a log documenting the work
**Deliverable:** 2. A short narrative (or table) explaining **what you fixed** and **how**.

1. Update Kanban board with new cards for lab
2. Identify what each row represents
3. assign data quality dims to at least two vbls

# Numeric and price issues
1. identify type issues - Completed (Neal)
2. identify outlier values (neg, large, etc...) - **NOT DONE**
3. Identify sentinel values (those that may signify missing values, e.g. NA, -1, etc) - **NOT DONE**
4. written discription - **NOT DONE**
5. Define and apply rules for impossible values - **NOT DONE (other than leading "O" --> 0)**

# Cleaning Categorical & String Fields
1. DEVICE_GEO_REGION: standardizing Oregon codes - **NOT DONE**
2. DEVICE_GEO_ZIP: formats and sentinels - **NOT DONE**
3. RESPONSE_TIME: prefixes and extra characters
    - convert to numeric - **DONE**
    - check for NA's - **DONE** (none found)
4. Date–Time Cleaning: TIMESTAMP (20–30 minutes) - DONE(ish).
    - Fixed the field but did not do all the things the lab asks for. Added some code showing how I explored the timestamp data issues and a brief explination on my ultimate fix. Still need to provide some written answers to questions
5. Keys, Duplicates, and Out-of-Range Values (20–30 minutes)
    - 5.1) Candidate keys and duplicates: Provided a bit of an answer earlier in lab. I do not believe there is any set of cols that can be guaranteed to provide a unique identifier. Shows filtered rows that demonstrate that. Still need to do work the lab explicitly asks for.
    - 5.2) check for duplicates - **DONE**
    - 5.3) decide on duplicate handling rule - **NOT DONE**
6. Out-of-range latitude/longitude (optional but recommended) -- **NOT DONE**
7. Final cleaned dataset - **NOT DONE**






