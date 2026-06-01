# pl_build_ended_match — Run History

All times UTC. Schedule: daily at 01:00 UTC (03:00 CET). All runs triggered by `trigger_build_ended_match`. All runs **Succeeded**.

---

## Activity Duration Breakdown — May 23–29, 2026

| Date | Pipeline Total | Act 1 · Silver | Act 2 · Gold | Act 3 · Discover Ended | Notes |
|---|---|---|---|---|---|
| May 23 | **4h 06m 58s** | 1h 06m 51s | 2h 59m 58s | — | Activity 3 not present in pipeline at this time |
| May 24 | **3h 54m 21s** | 0h 52m 16s | 2h 55m 56s | 0h 05m 50s | |
| May 25 | **3h 11m 57s** | 0h 54m 18s | 2h 13m 13s | 0h 04m 15s | |
| May 26 | **2h 31m 50s** | 0h 28m 06s | 1h 59m 20s | 0h 04m 10s | Shortest run this week |
| May 27 | **2h 43m 00s** | 0h 34m 12s | 2h 04m 34s | 0h 04m 03s | |
| May 28 | **3h 54m 07s** | 0h 37m 38s | 3h 11m 23s | 0h 04m 54s | Gold longest this week — likely heavy match day |
| May 29 | **3h 01m 07s** | 0h 37m 40s | 2h 17m 17s | 0h 05m 57s | |

---

## Share of Total Time (averages across 7 runs)

| Activity | Avg Duration | Typical % of total |
|---|---|---|
| Act 1 · RunSilverBuildEndedMatch | ~44m | ~19% |
| Act 2 · RunGoldBuildEndedMatch | ~2h 31m | ~78% |
| Act 3 · RunDiscoverCricketEnded | ~5m | ~3% |
| ADF overhead (transitions) | <30s | <1% |

**Gold (Activity 2) consistently dominates.** It accounts for ~78% of total runtime every day.

---

## What each activity does with its time

### Activity 1 — RunSilverBuildEndedMatch (19%)
Scans all bronze inplay snapshot manifests, diffs against existing processed-snapshot markers, then parses raw JSON into structured silver files for any snapshots not yet processed. Time scales with the number of new bronze snapshots written since the previous run — i.e. with how many matches were live and how many snapshots the dedup logic allowed through.

### Activity 2 — RunGoldBuildEndedMatch (78%)
For every event where the newest silver marker timestamp is newer than the existing `innings_1_from_silver.json` in gold, it reads **all silver snapshots for that event** and rebuilds the gold file from scratch. Time scales with:
- Number of events that have new silver data (i.e. matches captured in the last day)
- Total silver snapshot count per event (directly proportional to bronze volume, which was inflated by the dedup bug before May 29)

May 28 was the worst (3h 11m) — likely a heavy match day with many simultaneous live matches and high bronze volume from the pre-fix dedup gap.

**Expected improvement:** With the stats-only dedup fix deployed on May 29, bronze snapshot volume per match should drop ~95% during between-ball quiet periods. Gold will have less silver data to process per event, and this activity should shorten materially in coming nights.

### Activity 3 — RunDiscoverCricketEnded (3%)
Pure blob reads — scans gold for `innings_1_from_silver.json` files and writes the ended match index. Consistently fast (4–6 min). Time scales only with total number of ended matches ever, not with daily activity.

---

## ADF Run IDs (for Databricks drill-down)

| Date | Pipeline Run ID | Silver Databricks Run | Gold Databricks Run | Discover Databricks Run |
|---|---|---|---|---|
| May 23 | `092743dc-77e2-4b21-8f21-da568b51f651` | [link](https://adb-7405619046702431.11.azuredatabricks.net/?o=7405619046702431#job/171622281733328/run/187573625962298) | — | — |
| May 24 | `de0ff57b-7101-4c0f-8346-f368d0aa3ca9` | — | — | — |
| May 25 | `9fda1e21-f461-43f0-981b-bd11dde598a2` | — | — | — |
| May 26 | `b77826b1-b087-4c78-9c3d-2371d53393a3` | — | — | — |
| May 27 | `3dd55339-f7e5-4be1-b6d1-9bed35e06da8` | — | — | — |
| May 28 | `407ad166-c4b1-4455-aa83-e3d19a542815` | — | — | — |
| May 29 | `83ede716-7cba-4d21-8f58-55965595ea79` | [Silver](https://adb-7405619046702431.11.azuredatabricks.net/?o=7405619046702431#job/171622281733328/run/187573625962298) · [Gold](https://adb-7405619046702431.11.azuredatabricks.net/?o=7405619046702431#job/956940253234009/run/76603087743052) · [Discover](https://adb-7405619046702431.11.azuredatabricks.net/?o=7405619046702431#job/652644344321580/run/671254033159342) | — | — |
