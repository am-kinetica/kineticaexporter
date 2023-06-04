package kineticaotelexporter

// LOGS

// SQL statements to insert log data
const (
	InsertLog string = `INSERT INTO "%s".log
	(log_id, resource_id, scope_id, trace_id, span_id, time_unix_nano, observerd_time_unix_nano, severity_id, severity_text, body, flags)
	VALUES(%s, %s, %s, '%s', '%s', %s, %s, %d, '%s', '%s', %d)`

	InsertLogAttribute string = `INSERT INTO "%s".log_attribute
	(log_id, key, string_value, bool_value, int_value, double_value, bytes_value)
	VALUES(%s, '%s', '%s', %d, %d, %f, '%s')`

	InsertLogResourceAttribute string = `INSERT INTO "%s".log_resource_attribute
	(resource_id, key, string_value, bool_value, int_value, double_value, bytes_value)
	VALUES(%s, '%s', '%s', %d, %d, %f, '%s')`

	InsertLogScopeAttribute string = `INSERT INTO "%s".log_scope_attribute
	(scope_id, scope_name, scope_ver, key, string_value, bool_value, int_value, double_value, bytes_value)
	VALUES(%s, '%s', '%s', '%s', '%s', %d, %d, %f, '%s')`
)

// TRACES

// SQL statements to insert trace data
const (
	InsertTraceSpan string = `INSERT INTO "%s".trace_span
	(id, resource_id, scope_id, event_id, link_id, trace_id, span_id, parent_span_id, trace_state, name, span_kind, start_time_unix_nano, end_time_unix_nano, dropped_attributes_count, dropped_events_count, dropped_links_count, message, status_code)
	VALUES('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', %d, %s, %s, %d, %d, %d, '%s', %d)`

	InsertTraceSpanAttribute string = `INSERT INTO "%s".trace_span_attribute
	(span_id, "key", string_value, bool_value, int_value, double_value, bytes_value)
	VALUES('%s', '%s', '%s', %d, %d, %f, %s)`

	InsertTraceResourceAttribute string = `INSERT INTO "%s".trace_resource_attribute
	(resource_id, "key", string_value, bool_value, int_value, double_value, bytes_value)
	VALUES('%s', '%s', '%s', %d, %d, %f, %s)`

	InsertTraceScopeAttribute string = `INSERT INTO "%s".trace_scope_attribute
	(scope_id, name, version, "key", string_value, bool_value, int_value, double_value, bytes_value)
	VALUES('%s', '%s', '%s', '%s', '%s', %d, %d, %f, %s)`

	InsertTraceEventAttribute string = `INSERT INTO "%s".trace_event_attribute
	(event_id, event_name, "key", string_value, bool_value, int_value, double_value, bytes_value)
	VALUES('%s', '%s', '%s', '%s', %d, %d, %f, %s)`

	InsertTraceLinkAttribute string = `INSERT INTO "%s".trace_link_attribute
	(link_id, trace_id, span_id, "key", string_value, bool_value, int_value, double_value, bytes_value)
	VALUES('%s', '%s', '%s', '%s', '%s', %d, %d, %f, %s)`
)
