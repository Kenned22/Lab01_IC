Interactive Advertising Auctions — Final Project

This project analyzes bidding behavior, auction outcomes, and data quality issues within a two-day dataset of digital ad auctions. The work combines extensive data cleaning, exploratory data analysis (EDA), and modeling to understand what drives auction wins and how dataset quality affects results.

Project Goals

Clean and standardize a highly inconsistent raw dataset.

Explore distributions, patterns, and relationships in bid behavior.

Identify which features most influence win probability.

Evaluate systematic differences across devices, regions, and ad sizes.

Document data limitations and outline next steps.

Tools Used
RStudio

Used for all data cleaning, visualization, and modeling.
The team shared a consistent environment for running scripts and validating results.

GitHub

Version control system for managing updates to the code and analysis:

Branching for parallel work

Pull requests for code review

Commit history documenting development

Conflict resolution during merges

Jira

Task management and team coordination:

Assigned tasks and deadlines

Tracked progress using a Kanban board

Identified blockers and aligned workflow

Summary of the Analysis

The raw data contained major issues: corrupted fields, invalid timestamps, malformed ZIP codes, and out-of-state coordinates.

After cleaning, clearer patterns emerged in bidding volume, pricing behavior, and performance across devices and regions.

Higher bids consistently increased win probability.

Certain ad sizes and device types exhibited meaningful performance differences.

Overnight hours (1–3 AM) showed unexpectedly high bidding activity, likely automated.

Some rare categories produced misleading win rates due to low sample size.

Key Findings

Bid activity was heavily concentrated in a few formats and overnight hours.

Bid price was the strongest predictor of winning an auction.

Device type and region showed real performance differences.

Missingness and corrupted values were not random and significantly shaped early results.

Rare ad sizes created unstable win rates and required careful interpretation.

Uncertainties & Next Steps

The dataset only covers two days, limiting generalizability.

Some categories remain sparse even after cleaning.

Additional contextual features (advertiser, budget, competition) would strengthen modeling.

More robust modeling (mixed effects, time-series, or reinforcement learning) could deepen insights.

Future data pipelines should include automated validation checks.

How to Run the Project

Clone the repository:

git clone <repo-url>


Open the project in RStudio.

Run scripts/cleaning.md to reproduce the cleaned dataset.

Run scripts/eda.md for visualizations and exploratory analysis.

Run scripts/modeling.md for the logistic regression and scenario predictions.

Team Members

Neal, Lilly, Edwin, Bingtant
