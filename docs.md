# BNR Data Quality Program Dashboard

**Purpose:** This documentation defines the data quality dimensions and terms used in the dashboard, and explains how to use the system from end to end.

---

## Table of Contents

1. [What Is This System?](#1-what-is-this-system)
2. [Key Terms and Definitions](#2-key-terms-and-definitions)
3. [The Five Data Quality Dimensions](#3-the-five-data-quality-dimensions)
   - [Completeness](#31-completeness)
   - [Accuracy](#32-accuracy)
   - [Validity](#33-validity)
   - [Uniqueness](#34-uniqueness)
   - [Timeliness](#35-timeliness)
4. [Quality Scores Explained](#4-quality-scores-explained)
5. [The Issue Lifecycle](#5-the-issue-lifecycle)
6. [Dashboard User Guide — BNR Staff](#6-dashboard-user-guide--bnr-staff)
7. [Portal User Guide — Institution Staff](#7-portal-user-guide--institution-staff)
8. [The Data Correction Workflow](#8-the-remediation-workflow)
9. [Data Tables Reference](#9-data-tables-reference)
10. [Frequently Asked Questions](#10-frequently-asked-questions)

---

## 1. What Is This System?

The **BNR Data Quality (DQ) Dashboard** is a data quality tool used by the National Bank of Rwanda (BNR) and stakeholders, as part of the data quality program, to ensure the quality of data is maintained. 
Every month, data submitted to the EDWH (Electronic Data Warehouse) is automatically checked against stakeholder verifed  data quality rules. Institutions that fail a rule are assigned an **issue**. The system tracks those issues, assign remediation/data correction tasks, and confirm when the institution has corrected the  data.


---

## 2. Key Terms and Definitions

| Term | Definition |
|------|------------|
| **Institution** | A  financial entity (bank, MFI, or SACCO) that submits data to the EDWH |
| **Reporting Period** | The month for which an institution submitted its data (e.g., July 2026). |
| **Data Table** | One of the  datasets in the submission (e.g., Customers, Accounts, Contracts). |
| **Rule** | A specific data quality check applied to one or more fields in a data table. Each rule has a unique ID (e.g., COMP-001, ACC-016). |
| **Dimension** | A category that groups related rules by the type of quality problem they detect. There are five dimensions: Completeness, Accuracy, Validity, Uniqueness, and Timeliness. |
| **Score** | A percentage (0–100%) representing how well an institution's data passed the rules within a dimension. 100% means all records passed every rule. |
| **Failing Row** | A single record in a data table that violated a rule. For example, one customer record with a missing mandatory field. |
| **Issue** | A logged problem created when one or more failing rows are found for a specific rule at a specific institution. Issues have a 30-day SLA. |
| **SLA (Service Level Agreement)** | The deadline for resolving an issue. All issues must be corrected within 30 days of detection( this is subject to change upon agreement with stakeholders) |
| **Urgency** | How close an issue is to its SLA deadline. Escalates automatically from New → Attention → Urgent → Critical → Overdue. |

---

## 3. The Five Data Quality Dimensions

### 3.1 Completeness

**What it means:** All mandatory fields in a submitted record must contain a value. A field is "incomplete" if it is blank or null when data is required.


---

### 3.2 Accuracy

**What it means:** Data values must be correct, consistent, and internally coherent. This also includes referential integrity rules, which are rules set between more than one table. An example of such a rule is: Every  account in the accounts table having a corresponding customer in the customers table


> **Note:** Referential integrity scores are averaged with the other accuracy scores to produce a single Accuracy dimension score.

---

### 3.3 Validity

**What it means:** Data values must be in the correct format and within acceptable ranges, and must make logical sense when considered against other fields or business rules.

---

### 3.4 Uniqueness

**What it means:** Records that should be unique must not appear more than once. This covers both duplicate rows within a single submission and records that are repeated unchanged across two consecutive reporting periods when changes would be expected.
---

### 3.5 Timeliness

**What it means:** Data must reflect up-to-date account activity. An account that has had no transactions for an unusually long period may indicate stale, incorrect, or dormant-account data that has not been properly classified.

For example,an account marked as active must have had at least one transaction within the past 180 days. Accounts with no transaction activity for 180 days or more are flagged. | 180 days |

---

## 4. Quality Scores Explained

Each institution receives a score from **0% to 100%** for each of the five dimensions, calculated every month when the detection pipeline runs.

### How Scores Are Calculated

1. **Per-rule score:** For each rule run against a table, the system calculates the proportion of records that passed: `(passing rows ÷ total rows) × 100`.

2. **Per-dimension score:** Scores for all rules within a dimension are averaged across all relevant tables for that institution.

3. **Accuracy score:** Because referential integrity (REL-001 to REL-008) is closely related to Accuracy, those scores are averaged together with the other Accuracy rule scores to produce a single Accuracy dimension score.

4. **Dashboard display:** Scores are shown as percentages with colour indicators:
   - **Green (≥ 95%)** — Acceptable
   - **Amber (80–94%)** — Needs attention
   - **Red (< 80%)** — Critical issue


## 5. The Issue Lifecycle

When the monthly detection pipeline finds failing rows, it creates or updates an **Issue** in the system. Here is the full lifecycle:

```
Detection Run(monthly)
     │
     ▼
Failing rows found for a rule at Institution X
     │
     ▼
Issue Created (Status: New, SLA: 30 days from today)
     │
     ├── Age 0–7 days   → Urgency: NEW
     ├── Age 8–14 days  → Urgency: ATTENTION
     ├── Age 15–21 days → Urgency: URGENT
     ├── Age 22–29 days → Urgency: CRITICAL
     └── Age 30+ days   → Urgency: OVERDUE
          │
          ▼
  Inspector creates a Data Correction Request and assigns it to the institution
          │
          ▼
  Institution corrects data and resubmits
          │
          ▼
  Next detection run finds zero failing rows for that rule
          │
          ▼
  Issue marked PENDING RESOLUTION (tentative pass)
          │
          ▼
  Resolution scan independently re-checks the rule
          │
     ┌────┴────┐
  Still clean   Still failing
     │               │
     ▼               ▼
  RESOLVED        Stays OPEN
```

**Two-pass resolution** is used deliberately: an issue is only closed after *two independent clean checks* to prevent false closures caused by mid-upload data snapshots.

**Recurrence:** If the same issue reappears within 30 days of being resolved, it is reopened with a recurrence counter (shown as ↺ 2, ↺ 3, etc.), signalling a pattern of persistent non-compliance.

---

## 6. Dashboard User Guide — Inspector side

The inspector accesses the main dashboard after login. The dashboard is organized into four main areas.

### 6.1 Home — Institution side

The home page shows all  institutions grouped by category:

- **B** — Commercial Banks
- **MF** — Microfinance Institutions
- **OSACCO / SACCO** — Savings and Credit Cooperatives
To check the data quality for each institution/category, click on the corresponding card for that category

### 6.2 Category / Institution View

After selecting a category or institution, you see:

**Score Cards (top row):** Five KPI tiles showing the institution's current score for each dimension.

**Institution Reports Table:** Lists available issue evidence packages. Each row shows the institution name, tables with issues, and a **Download ZIP** button. The ZIP contains excel files of the actual failing rows, broken down by table and rule.

### 6.3 Remediation Tab ( Request Data Correction)

Used to create and manage data correction requests. See [Section 8](#8-the-remediation-workflow) for the full workflow.

---

### 6.4 Issues Tab (Check Resolved Issues)

This tab is contains resolved issues and is the primary reference  before approving a data correction submission by the institutions. 
### 6.5 Documentation

This is a global tab ( can be seen regardless of who is logged in), that contains the full list of rules being checked, and the documentation of this dashboard.

## 7. Portal User Guide — Institution Staff

Institution users log in to a separate portal where they see only their own institution's data.

### 7.1 Dashboard Page

Shows the same five dimension scores scoped to the institution and the last seven (7) runs for that institution per dimension.

### 7.2 Issues Page (My Issues)

Lists all open issues for the institution. For each issue the user can see:
- Which table and rule triggered the issue.
- How many records are failing.
- The SLA deadline and urgency level.

Each issue row has a button to download the specific CSV of failing records for that rule, so the institution's data team knows exactly which records need to be corrected.
 

The page also shows resolved issues for that institution after the have done data correction and the data has been loaded into EDWH.

### 7.3 Data Correction Page

Shows Data Correction Requests that the inspector has assigned to the institution. Institution users can:
- **Start Work** on a CR (moves it from Open to In Progress).
- **Submit** a corrected data request for review once corrections have been made.

Institution users cannot create or approve data correction requests — those actions belong to the inspector.


## 8. The Remediation/Data Correction  Workflow

When the inspector identifies issues that require formal correction, the remediation/data correction process follows these steps:

### Step 1 — The inspector Creates a Data Coorection Request

An inspector goes to the **Data Correction** tab, selects the institution, chooses the relevant issues from the issue list, fills in:
- **Title** — A short description of the  task.
- **Description** — Detailed explanation of what needs to be corrected.
- **Assigned Officer** — The institution officer responsible for following up.
- **Target Date** — Expected completion date.

The data correction request is saved and the institution/officer  is notified.

### Step 2 — Institution Starts Work

The institution's user logs into the portal, finds the Data Correction Request on their ***Data Correction*** tab, and clicks **Start Work**. The Data Correction request status changes to **In Progress**.

The institution downloads the reports, corrects the data in their source systems (core banking systems), and prepares a resubmission.

### Step 3 — Institution Submits for Review

Once corrections are made and data is resubmitted, the institution clicks **Submit** on the CR. The status changes to **Submitted**, and BNR is notified.

### Step 4 — Inspector Reviews and Approves

An inspector reviews the resubmission. In the main dashboard Data Correction tab. The officer can:
- **Approve** individual tables within the data correction request (if some are corrected and others are not).
- **Reject** the entire data correction request with written reviewer notes explaining what still needs to be fixed.

If all tables are approved, the CR status moves to **Approved** and then **Closed**.

### CR Status Summary

| Status | Meaning |
|--------|---------|
| Open | CR created by BNR; institution has not started work yet. |
| In Progress | Institution has accepted the CR and is correcting data. |
| Submitted | Institution has resubmitted corrected data and is awaiting BNR review. |
| Approved | BNR Inspector has confirmed the corrections are satisfactory. |
| Rejected | BNR Inspector has reviewed and found issues still outstanding; institution must re-correct. |
| Closed | All tables approved; data correction complete. |

---

## 9. Frequently Asked Questions

**Q: How often does the system check data quality?**  
The full detection pipeline runs monthly(this is subject to change depending on specific data needs). Score histories are updated daily to reflect any data corrections made between monthly runs.

**Q: What is the difference between a failing row and an issue?**  
A **failing row** is a single data record that violated a rule. An **issue** is the aggregated problem logged for a specific rule at a specific institution — it groups all failing rows for that rule into one trackable item with an SLA and urgency level. One issue can represent hundreds or thousands of failing rows.

**Q: What does "Pending Resolution" mean?**  
After a clean detection run (no failing rows found), the issue is set to Pending Resolution rather than closed immediately. The system runs a second independent check to confirm the data is genuinely clean before fully closing the issue. This two-step process prevents false closures caused by partial or mid-upload data snapshots.

**Q: Can an institution user see other institutions' data?**  
No. The institution portal enforces strict data isolation — each institution user can only see data for their own institution. BNR staff( Inspector) in the main dashboard can see all institutions.
