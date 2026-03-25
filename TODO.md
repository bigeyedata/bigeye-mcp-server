# Roadmap

## Future Improvements

- Add more granular filtering options for search results
- Implement caching for frequently accessed data
- Add support for bulk operations on issues

### Search Improvements
- Add automatic space-to-underscore conversion in table/column searches
- Implement fuzzy matching or wildcards for more flexible searches
- Add search result caching to avoid repeated API calls
- Consider parallel searching across Bigeye and Atlan

### Cross-System Integration
- Add mapping between Bigeye and Atlan naming conventions
- Help correlate the same assets across both systems

### Error Handling
- Fix JSON parsing exceptions in Bigeye responses
- Add retry logic for recoverable errors
- Better error messages when searches return no results

## New Tools to Implement

### Core Health & Issue Tools

1. **get_active_issues** (enhancement of existing `get_issues`)
   - Params: `severity_filter`, `schema_filter`, `owner_filter`, `time_window`
   - Purpose: More focused filtering than current `get_issues`

### Metric Management Tools

2. **get_metric_coverage**
   - Params: `table_identifier`
   - Purpose: Identify monitoring blind spots

3. ~~**create_metric**~~ ✅ Implemented
   - Params: `table_name`, `metric_type`, `column_name`, `schema_name`, `table_id`, `name`, `description`, `lookback_type`, `lookback_interval_type`, `lookback_interval_value`, `filters`, `group_bys`
   - Includes: metric type validation, column type compatibility checks, lookback enum mapping, error propagation

4. **get_metric_history**
   - Params: `metric_id`, `time_range`
   - Purpose: Trend analysis and pattern detection

### Tag Management Tools

10. ~~**list_tags, create_tag, update_tag, delete_tag, tag_entity, untag_entity, list_entity_tags**~~ ✅ Implemented
   - Full CRUD for workspace tags (v2 API) plus tag/untag any entity type
   - Supports: metrics, tables, columns, schemas, sources, deltas, SLAs, custom rules
   - Includes: entity type validation/normalization, color hex validation, name length validation

### Sensitive Data Scanning Tools

11. ~~**list_data_classes, get_table_sensitivity_findings, get_scan_findings**~~ ✅ Implemented
   - list_data_classes: fetches data classification categories with sensitivity levels
   - get_table_sensitivity_findings: name-based lookup with auto table resolution, supports aggregate + snapshot findings
   - get_scan_findings: lower-level ID-based query across multiple tables/columns, supports aggregate + snapshot findings
   - Supports: filtering by sensitivity, data class, table/column IDs, positive-only findings
   - Includes: pagination, search filtering

### Incident Management Tools

5. **get_sla_compliance**
   - Params: `table_identifier`, `time_period`
   - Purpose: Track service level compliance

### Analytics & Reporting Tools

6. **generate_quality_report**
   - Params: `scope` (schema/owner/tag), `time_period`, `format`
   - Purpose: Executive reporting and trending

7. **get_anomaly_patterns**
   - Params: `table_identifier`, `lookback_period`
   - Purpose: Identify systemic problems

### Integration Tools (Bigeye + Atlan)

8. **validate_catalog_coverage**
   - Params: `atlan_catalog_filter`
   - Purpose: Ensure comprehensive monitoring coverage

9. **enrich_issue_context**
   - Params: `issue_id`
   - Purpose: Issue details enriched with Atlan metadata (owners, documentation, tags)

## MCP Resources to Implement

### Data Quality Resources
- `resource://issues/active` — Active issues with schema/severity filtering
- `resource://issues/recent` — Recently resolved or updated issues

### Data Catalog Resources
- `resource://datasources` — All configured data sources
- `resource://schemas/{datasource_id}` — Schemas in a data source
- `resource://tables/{schema_id}` — Tables in a schema
- `resource://columns/{table_id}` — Columns in a table

### Monitoring Configuration Resources
- `resource://metrics/available` — Available metric types
- `resource://metrics/configured/{table_id}` — Configured metrics for a table
- `resource://deltas` — Data delta configurations

### Data Quality Dimensions Resources
- `resource://dimensions` — DQ dimension definitions
- `resource://dimensions/scores/{table_id}` — DQ scores by dimension for a table

### Operational Resources
- `resource://sla/definitions` — SLA definitions
- `resource://lineage/graph/{node_id}` — Lineage graph for a node
- `resource://notifications/rules` — Notification rules
- `resource://glossary/terms` — Business glossary terms
