# Plan Review: Open-Meteo Lakehouse Pipeline

**Reviewer stance:** Principal data engineer. Scope: decide whether the plan can be implemented, will be correct, and is suitable for the assignment. Not a search for a perfect solution.

**Verdict:** The plan is implementable and the architecture is sound. It has three correctness bugs that should be fixed before building, and each is a cheap fix.

**Basis:** The plan and supporting docs only (implementation plan, data dictionary, DQ spec), plus the Open-Meteo docs and one live API response. No code was reviewed, so this judges the design, not an implementation.

---

## 1. What holds up

- **Bronze:** One batched call, with `location_id` mapped by list position. Grid lat/lon are kept as metadata and never used as a key. Raw JSON lands first, then three append-only Bronze tables.
- **Silver and DQ:** The SCD2 hash approach is standard. The 13 DQ rules are numbered consistently across the docs. Null handling is mostly safe, because nullable columns use `IS NULL OR (...)`.
- **Gold and `is_forecast`:** Deriving `is_forecast` in Gold instead of storing it in Silver is the right call.
- **Stack:** Python 3.11, OpenJDK 17, PySpark 3.5 and Delta 3.2 is a compatible combination. The Windows host Python 3.14 / Java 14 mismatch is correctly sidestepped with Docker.

---

## 2. Must fix (correctness)

### 2.1 Time semantics are inconsistent

- The Silver docs parse `time[k]` as `observation_time_utc`, "requested in UTC". The seed config, however, carries a per-location timezone.
- The API returns naive local strings (for example `2026-10-22T00:15`), with the offset only at the top level (`utc_offset_seconds: 39600`).
- **If UTC is requested:** daily max and min are computed over UTC days, so a Sydney "day" runs 11:00 to 11:00 local. That is wrong for heat alerts.
- **If local timezones are requested:** parsing the strings as UTC mislabels every timestamp, and the error differs by location (Perth +8, Darwin +9:30).
- Silver hourly's `observation_date` is derived from the UTC time, while daily's date is local. Gold then joins hourly aggregates to the wrong day.

**Fix**
1. Request per-location timezones (the docs allow a comma-separated list).
2. Add `utc_offset_seconds` to the three Bronze tables, since it isn't inside the payload JSON.
3. Convert to true UTC in Silver using that offset, and keep a separate `observation_local_date` for the daily join.
4. Alternatively, request `timeformat=unixtime`.

No DST change falls inside the current window (the Australian change was 4 October). The API fixes the offset for the whole response, which is a good interview talking point.

### 2.2 DQ-DOM-001 omits WMO code 97

- The Open-Meteo docs list 97 (heavy thunderstorm). They also say most models report 95 and 97, not 96 and 99.
- The rule would quarantine real severe-storm rows, which is the worst place to lose data for an aged-care alert mart.

**Fix:** Add 97 to the rule, to `WMO_CODE_MAP`, and to the tests.

### 2.3 The Silver MERGE needs three guards, or "idempotent" isn't true

1. **Dedupe the source.** Several Bronze ingestions overlap on the same timestamps. If one run reads more than one (after a repair, for example), the MERGE source has duplicate keys, and Delta either errors or picks nondeterministically. Take the latest `ingested_at` per key with `row_number()` first.
2. **Update only when newer.** Use `WHEN MATCHED AND src.api_ingested_at_utc > tgt.api_ingested_at_utc`, and ideally only when values differ. Otherwise a replayed older batch overwrites newer data. An unconditional update also rewrites every row on every run, so the change data feed fills with no-op changes. That is the JD's "job reports success but downstream breaks" theme.
3. **Read only new Bronze rows** (by run id or watermark). If each run rescans all of Bronze, failed rows are re-quarantined every run and the quarantine table grows.

---

## 3. Should fix (cheap, and an interviewer will notice)

- **`peak_15m_precip_mm` is meaningless for Australia.** In the Sydney sample, 15-minute precipitation is the hourly value divided by 4 and rounded to 0.1 mm, so small totals vanish and the sums don't reconcile. The "flash rainfall intensity" claim is false. Drop the column or relabel it, and note this in the README.
- **`HISTORICAL_ACTUAL` is misleading.** Silver holds the latest model estimate, not observations. Use a label such as `PAST_MODELLED`.
- **`is_forecast` uses `current_date()` in UTC.** For about 11 hours a day that is yesterday in Sydney. Use the location's local date. Today is also partly past, so consider a third label.
- **The "native host" claim contradicts a PySpark transformer.** Either the transform is written twice, or the README overstates it. Keep DuckDB as a read-only query layer and state that Docker is a prerequisite.
- **Docker details to check early:**
  - Warm the Delta JAR download during `docker build`.
  - Test `_delta_log` writes on a Windows bind mount (a named volume is safer).
  - Add retry with backoff on 429 and 5xx responses.
- **Correction to an earlier statement:** the 0, 1, 2, 3 weather-code sequence in the Melbourne sample looked like numeric interpolation of a categorical value, but other transitions in the sample jump straight from one code to another. Invalid interpolated codes are not a concern.

---

## 4. Implementability

Everything above is buildable, but the scope is large for the time available: 10 tables, 7 checkpointed steps, a repair runner, a quota ledger, a Streamlit app and an S3 policy file.

**Core path (build first):** landing, Bronze, Silver facts, one Gold mart, tests, README.

**Droppable without redesign:** Streamlit, the quota ledger, the S3 policy file, the repair runner.

Every piece must be explainable in the interview. Cut anything you cannot confidently defend.

---

## 5. Not verified

- **Quota formula and limits.** The weight formula (`locations x days/14 x vars/10`) and the 600/min, 5,000/hr and 10,000/day limits are not on the docs page that was read. Check the pricing page. If the real formula has a floor of 1, the ledger undercounts small calls. This is a side feature, so it is not blocking.

---

## 6. Summary

| # | Issue | Severity | Fix effort |
|---|-------|----------|-----------|
| 2.1 | UTC vs local time semantics | Must fix | Small: request local timezones, store offset, convert in Silver |
| 2.2 | WMO code 97 missing from DQ rule | Must fix | Trivial |
| 2.3 | Silver MERGE not truly idempotent | Must fix | Small: dedupe, newer-only condition, incremental read |
| 3 | Precip-15m column, label naming, UTC `is_forecast`, host-run claim, Docker details | Should fix | Small each |
| 4 | Scope too large for the time available | Advisory | Build the core path first |

With fixes 2.1 to 2.3, the plan is correct and suitable for the assignment.

**Sources:** [Open-Meteo Weather Forecast API docs](https://open-meteo.com/en/docs)
