# AI-Assisted Engineering Evidence

Per the assignment instructions (*"AI evidence: screenshots of the material conversations with the AI tools used during the assignment"*):

1. **Primary Evidence Document:** See [AI_evidence.docx](file:///d:/Personal/Jobs/2026/144%20Loop%20IQ/Assignment/AI_evidence.docx) in the root of the repository (or place additional screenshots in this `docs/ai_evidence/` directory).
2. **Cross-Review Document:** See [plan_review_by_Claude.md](file:///d:/Personal/Jobs/2026/144%20Loop%20IQ/Assignment/plan_review_by_Claude.md) for the multi-model architectural peer review conducted prior to implementation, verifying:
   * Per-location IANA timezone & `utc_offset_seconds` conversion in Silver (Fix 2.1)
   * Inclusion of WMO weather code `97` (*Thunderstorm: Heavy without hail*) in `DQ-DOM-001` (Fix 2.2)
   * Three-guard idempotent Silver `MERGE INTO` (`pipeline_run_id` incremental filter, `ROW_NUMBER()` deduplication, and `api_ingested_at_utc` + `row_value_hash` change detection) (Fix 2.3)
   * Australian `minutely_15` interpolation handling and Gold `PAST_MODELLED` / `TODAY_MIXED` / `FORWARD_FORECAST` horizon classification (Fix 3).
