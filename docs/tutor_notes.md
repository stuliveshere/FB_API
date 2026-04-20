# DATA7201 Project — Working Summary

## 1. Assignment at a glance

- **Course:** DATA7201 Data Analytics at Scale (2026)
- **Weight:** 60% of course mark
- **Deadline:** 3pm Friday 22 May 2026 (Week 12), via Turnitin
- **Deliverable:** 1,500-word structured report + code appendix
- **Goal:** Use big data analytics techniques on the Facebook Ad Library dataset to produce findings useful to non-technical decision makers, and justify technique/technology choices.

### Report structure (required sections) + word budget

| Section | Marks | Word budget | Notes |
|---|---|---|---|
| Structured abstract | 0 (required) | — | Not in word count |
| Table of contents | 0 (required) | — | Not in word count |
| Introduction | 5 | ~150 | Motivate distributed systems for big data, practical examples |
| Dataset description + pre-processing | part of 30 | **~500** | Dataset, schema-drift, key-stability, cleaning, typing |
| Analysis | part of 30 | **~500** | EDA methods, technique justification, visuals |
| Discussion and conclusions | 15 | **~500** | Main findings, lessons learned, take-home message |
| Presentation / communication | 10 | — | Assessed across the whole report |
| Appendix | 0 (required) | — | Code, scripts, queries — not assessed for quality |

Charts, tables, appendices don't count toward 1,500 words. Word count must be stated in the report.

**Mark allocation drives the word split.** The 30-mark Dataset Analytics block is roughly half description/pre-processing and half analysis, so split it ~500/500. Discussion gets its own 15 marks and its own ~500 words. Intro is ~150 words — motivate but don't over-explain. Totals to ~1,500.

---

## 2. Dataset

- **Source:** Facebook Ad Library API — https://www.facebook.com/ads/library/api/
- **Scope:** Sponsored posts targeted at Australian users, 03/2020 – 02/2024 (~4 years)
- **Covers:** Pre-2022 Federal Election, 2023 Voice Referendum, COVID period
- **Location on cluster:** `/data/ProjectDatasetFacebookAU` (HDFS)
- **Format:** JSON files, one per API call
- **Sampling cadence:** Nominally ~12-hourly but **not regular across the whole period** — needs checking
- **Each file = snapshot** of ads *active at that instant* (not a log of new ads). An ad that runs multiple days appears in many files.

### Known / suspected data quirks

- **Not all ads are political** — need to filter or classify before political analysis.
- **Duplicate ad records** across files for long-running campaigns. Deduplicate on ad ID.
- **Ad IDs should be stable per ad** — worth validating that keys aren't being rotated. Check whether identical ad-content appears under different IDs.
- **Schema drift over 4 years:** field names in the API have changed. Columns may be null for part of the period then populate (or vice versa). Tutor has confirmed this happens — flag any field that looks suspiciously sparse in one time window.
- **Date ranges vs. reality:** ad `start_date`/`end_date` should be validated against when the ad actually appears in snapshots. Ads can be pulled/cancelled early — so "which ads ran on day X" should be derived from snapshot presence, **not** declared dates.
- **Demographic / location data** is in nested fields and sometimes embedded in ad URLs.
- **Funding entity** is a usable proxy for party/campaign alignment and electorate targeting.

---

## 3. Tutor-provided constraints

**Do:**
- Use Spark.
- Use Spark **DataFrames** (not RDDs).
- Validate date ranges (declared vs. observed in snapshots).
- Check key stability (same ad, same ID over time).
- Watch for column-name / schema changes across the 4 years.
- Clean up non-political ads (classify by linked domain, funding entity, etc.).
- Motivate all choices — why this technique, why this tool, why this slice of data.
- Anchor the analysis to real events: COVID, 2022 election, Voice referendum.

**Don't:**
- Don't use Pig.
- Don't work with RDDs.

**Free to choose:** topics, questions, angles, external data integration (optional).

---

## 4. Narrative strategy — time is the spine

The rubric weights communication heavily (10 marks for presentation, plus the 15 for Discussion). A coherent story beats a scattered list of findings. The dataset hands us a natural spine: **time**. Every row has a snapshot timestamp, every ad has declared start/stop times, and three major external events — COVID waves, the 2022 federal election, the 2023 Voice referendum — sit on the same axis. Time-series is the dominant frame.

**Why this works for the report:**
- Every chart can share an x-axis. The reader doesn't re-orient between figures.
- External events overlay as vertical reference lines, instantly contextualising spikes and dips.
- Small multiples (same time axis, different metric per row) scale to any number of dimensions.
- "What happened around event X?" is a question any non-technical stakeholder can follow.

**Visual vocabulary to lean on:**
- Line / area charts for counts and spend over time.
- Stacked or 100%-stacked area for compositional shifts (e.g. share of spend by funding-entity type).
- Heatmaps (time × category) for dense categorical comparisons (e.g. week × top-20 advertisers).
- Histograms / CDFs for one-off distributional claims (ad duration, spend per ad).
- Scatter with marginals for correlation questions (spend vs. impressions).

**Story arc:**
1. *Here's what the dataset actually contains once you look closely* — pre-processing section. Schema drift and cadence irregularities are part of the narrative, not footnotes.
2. *Here's what political ad activity looks like over 4 years, with major events marked* — analysis section. Time-series is the spine.
3. *Here's what that tells a decision-maker* — discussion. Lessons about the medium, not just about the data.

---

## 5. Analysis — techniques, not results

Emphasis on **methods applied and justified**, not a laundry list of findings. For each technique the report should briefly say what it is, why it suits the question, and what it shows.

### 5.1 EDA techniques

- **Cadence / coverage audit.** Histogram of snapshot timestamps per day/week. Identifies gaps and irregular sampling — affects every downstream time-series interpretation.
- **Null-fraction-over-time per column.** Heatmap of (column × month) null rates. Surfaces schema drift without needing API changelog knowledge.
- **Cardinality profiling.** Distinct counts for `id`, `page_id`, `funding_entity` — overall and per month. Shows whether advertisers grow, churn, or get renamed.
- **Key-stability check.** Group by hash of `(page_id, ad_creative_body)` — does one ad ever get multiple IDs?
- **Declared-range vs. observed-range validation.** For each ad, compare declared `[ad_delivery_start_time, ad_delivery_stop_time]` with its first and last snapshot. Distribution of mismatch is itself a finding about data reliability.
- **Deduplication strategy.** Ad × snapshot is the natural grain; collapse to ad-level for lifetime metrics, keep snapshot-level for "ads live on day X" queries.

### 5.2 Analytical techniques (time-series-forward)

- **Daily/weekly aggregations** of ad counts, distinct advertisers, total spend (lower and upper bounds separately — they're range structs, not point estimates).
- **Rolling windows** (7-day, 30-day) to smooth cadence noise before plotting.
- **Event-window comparison.** Define windows around COVID waves, the 2022 election, and the 2023 Voice referendum; compare pre/during/post on a chosen metric.
- **Group-by time × category** — e.g. spend by funding-entity-type per week. Feeds a stacked area plot directly.
- **Ad-duration analysis.** Empirical CDF of ad lifetimes; compare across topics or funding entities.
- **Correlation analysis.** Spend vs. impressions — report both the scatter and a correlation coefficient, with a caveat about bucket-width bias (both fields are range structs).
- **URL/domain classification.** Parse ad link fields, extract domains, categorise (news / party / gov / fundraising / other). Simple rule-based classifier is fine and easy to defend.
- **Funding-entity → electorate mapping.** Join with public electorate metadata to see targeted regions per advertiser.
- **Language × currency analysis.** The `languages` field and `currency` field together are a signal for foreign influence or ethnic-community targeting. Ads in non-English languages (Spanish, Mandarin, Arabic, Vietnamese, etc.) paid in non-AUD currencies (USD, CNY, etc.) are worth flagging — they suggest either foreign-origin campaigns or deliberate targeting of diaspora communities. Exchange-rate normalisation (e.g. via RBA daily rates or a static lookup) is needed to compare spend across currencies on a common scale. **Caveat:** this analysis only makes sense on ads already classified as political — running it on the full dataset will mostly surface commercial advertisers like the Intch ads in the sample, which are in Spanish but clearly not political. The political classification step (by funding entity, domain, or keyword) must come first.
- **Language diversity over time.** A time-series of ad counts by language, anchored to the same event markers, may reveal whether non-English political advertising spiked around the Voice referendum (which had strong community-targeted campaigning) differently from the 2022 election.

### 5.3 Technology justification (for Introduction + Dataset sections)

- ~4 years × ~2 snapshots/day × thousands of ads per snapshot → tens of thousands of JSON files; too many small files for a laptop.
- Nested JSON + schema drift suits Spark's schema inference and lazy evaluation.
- DataFrame API is the natural fit: columnar operations, Catalyst optimisation, partition pruning after writing Parquet.
- RDDs would mean hand-writing all of that with none of the benefits — which is why the tutor excluded them.
- Pig is excluded — use PySpark or Spark SQL.

---

## 6. Immediate next steps — data loading & inspection

Goal: get the data into Spark DataFrames, understand what's actually in there, and set types properly before any analysis.

1. **Inventory the files.** `hdfs dfs -ls /data/ProjectDatasetFacebookAU` — count files, look at filename patterns (the timestamp is likely encoded in the filename).
2. **Parse filenames** to extract the snapshot timestamp. Critical — it's the "when was this ad live" signal.
3. **Load JSON into Spark DataFrame** with `spark.read.json(...)`. Let Spark infer schema first, then inspect.
4. **Show schema** with `df.printSchema()`. Note nested fields (`demographic_distribution`, `delivery_by_region`, `publisher_platforms`, etc.).
5. **Null-column audit:** null fraction overall AND per year/month. Fields that flip from all-null to populated (or vice versa) indicate schema drift / renamed fields.
6. **Set proper types:**
   - Dates: `ad_creation_time`, `ad_delivery_start_time`, `ad_delivery_stop_time` → timestamp.
   - Snapshot timestamp from filename → timestamp column.
   - Numeric ranges (`impressions`, `spend`) are `{lower_bound, upper_bound}` structs — keep as struct, derive midpoint columns if helpful.
7. **Key validation:** `id` cardinality, uniqueness, and whether identical `ad_creative_body` + `page_id` ever appears under multiple IDs.
8. **Persist as Parquet** partitioned by year/month once loading is stable — avoids re-parsing JSON for every downstream query.

### Relevant Ad Library API fields to expect

`id`, `ad_creation_time`, `ad_delivery_start_time`, `ad_delivery_stop_time`, `ad_creative_bodies`, `ad_creative_link_captions`, `ad_creative_link_descriptions`, `ad_creative_link_titles`, `ad_snapshot_url`, `bylines`, `currency`, `delivery_by_region`, `demographic_distribution`, `estimated_audience_size`, `funding_entity`, `impressions` (struct), `languages`, `page_id`, `page_name`, `publisher_platforms`, `spend` (struct), `target_ages`, `target_gender`, `target_locations`.

Confirm which of these are actually present in 2020 files vs. 2024 files — that's the schema-drift check.

---

## 7. Open questions to resolve while loading

- Total data volume (files, rows, bytes)? Drives whether sampling is needed.
- Real sampling cadence per month? (Histogram of snapshot timestamps.)
- Gaps in the time series (missing days/weeks)?
- Which fields are reliably populated across the full 4 years, and which aren't?
- Does the ad ID space change format at any point (would suggest key rotation)?

These go into the report's pre-processing narrative — they're not throwaway checks, they're part of the story of working with messy big data.
