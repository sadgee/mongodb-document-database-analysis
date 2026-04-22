# Lab 3 — Document Databases with MongoDB

A data analysis lab using MongoDB aggregation pipelines to retrieve, transform, and analyze data across multiple document databases. Built as part of a Master's in Data Management program.

## Overview

This lab explores MongoDB's aggregation framework across four real-world datasets hosted on a shared MongoDB Atlas instance. It covers everything from basic document queries to complex multi-stage pipelines involving `$unwind`, `$group`, `$lookup`, and statistical analysis.

**Part 1** — 14 aggregation pipeline questions across four datasets  
**Part 2** — Two one-page analytic briefs presenting data-driven insights

---

## Datasets

| Dataset | Description | Source |
|---------|-------------|--------|
| **Global Cities** | Countries, states, and cities worldwide (collection: `countries_states_cities`) | [Kaggle](https://www.kaggle.com/datasets/darshangada/countries-states-cities-database) |
| **Yelp** | Businesses, reviews, and users from the Yelp Open Dataset | [Yelp Dataset](https://www.yelp.com/dataset) · [Documentation](https://www.yelp.com/dataset/documentation/main) |
| **Sample Airbnb** | Listings and reviews from the MongoDB sample Airbnb dataset | [MongoDB Docs](https://www.mongodb.com/docs/atlas/sample-data/sample-airbnb/) |
| **NBA** | Historical NBA game data including box scores | [NBA Dataset](https://www.kaggle.com/datasets) |

---

## Part 1 — Aggregation Pipelines

### Global Cities (Questions 1–3)

| # | Task Summary | Key Concepts |
|---|-------------|--------------|
| 1 | Count countries per subregion; filter to ≥ 10 | `$group`, `$match`, `$sort` |
| 2 | Find all cities named "Rochester" worldwide | `$match`, `$project` |
| 3 | Countries using USD as currency with city counts | `$match`, `$group`, `$sort` |

### Yelp (Questions 4–7)

| # | Task Summary | Key Concepts |
|---|-------------|--------------|
| 4 | California cities with ≥ 10 outdoor-seating restaurants | Nested field queries, `$match`, `$group` |
| 5 | Philadelphia dim sum restaurants matching multiple criteria | Multi-condition `$match`, string concatenation (`$concat`) |
| 6 | Top 5 states for "Yoga" + "Meditation Centers" businesses | Array field matching (`$all`), `$limit` |
| 7 | Compare review behavior: "Jennifer" vs. "Jenny" users | `$match` with `$in`, `$group`, `$avg`, comparative analysis |

### Airbnb (Questions 8–10)

| # | Task Summary | Key Concepts |
|---|-------------|--------------|
| 8 | Price & cleaning fee stats for 6 global markets | `$min`, `$max`, `$avg`, `NumberDecimal` |
| 9 | Top 3 hosts by listing count in Porto | `$group`, `$sort`, `$limit`, host-level aggregation |
| 10 | Reviewers with ≥ 5 reviews in Montreal | `$unwind`, `$group`, reviewer deduplication |

### NBA (Questions 11–14)

| # | Task Summary | Key Concepts |
|---|-------------|--------------|
| 11 | Kobe Bryant vs. Tim Duncan — total games played | Player-level filtering, `$count` |
| 12 | 2003–04 season games where loser scored < 70 | Date range filtering, score extraction |
| 13 | Home-court advantage analysis (1995–2005) | Win percentage calculation, conditional aggregation |
| 14 | Correlation between steals leader and game winner (2000s) | Nested field access (`box.team.stl`), statistical analysis |

---

## Part 2 — Analytic Briefs

Two open-ended data analysis tasks, each presented as a one-page brief. These focus on:

- Framing analytic questions from broad prompts
- Structuring multi-query analyses
- Presenting data-driven conclusions clearly and concisely

---

## Tools & Environment

- **Database:** MongoDB Atlas (shared cluster)
- **Client:** Studio 3T (recommended)
- **Deliverables:** `.js` file (Part 1), PDF (Part 2)

## Skills Demonstrated

- MongoDB aggregation framework (`$match`, `$group`, `$sort`, `$project`, `$unwind`, `$limit`, `$lookup`)
- Working with nested documents and arrays
- Data type handling (`NumberInt`, `NumberDecimal`)
- String manipulation and field concatenation
- Date range filtering
- Statistical analysis via aggregation pipelines
- Translating analytical questions into database queries
- Communicating data insights in written form
