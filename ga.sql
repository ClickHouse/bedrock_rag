CREATE TABLE default.ga_daily
(
	`event_date` Date,
	`event_timestamp` DateTime64(3),
	`event_name` String,
	`event_params` Map(String, String),
	`ga_session_number` MATERIALIZED CAST(event_params['ga_session_number'], 'Int64'),
	`ga_session_id` MATERIALIZED CAST(event_params['ga_session_id'], 'String'),
	`page_location` MATERIALIZED CAST(event_params['page_location'], 'String'),
	`page_title` MATERIALIZED CAST(event_params['page_title'], 'String'),
	`page_referrer`  MATERIALIZED CAST(event_params['page_referrer'], 'String'),
	`event_previous_timestamp` DateTime64(3),
	`event_bundle_sequence_id` Nullable(Int64),
	`event_server_timestamp_offset` Nullable(Int64),
	`user_id` Nullable(String),
	`user_pseudo_id` Nullable(String),
	`privacy_info` Tuple(analytics_storage Nullable(String), ads_storage Nullable(String), uses_transient_token Nullable(String)),
	`user_first_touch_timestamp` DateTime64(3),
	`device` Tuple(category Nullable(String), mobile_brand_name Nullable(String), mobile_model_name Nullable(String), mobile_marketing_name Nullable(String), mobile_os_hardware_model Nullable(String), operating_system Nullable(String), operating_system_version Nullable(String), vendor_id Nullable(String), advertising_id Nullable(String), language Nullable(String), is_limited_ad_tracking Nullable(String), time_zone_offset_seconds Nullable(Int64), browser Nullable(String), browser_version Nullable(String), web_info Tuple(browser Nullable(String), browser_version Nullable(String), hostname Nullable(String))),
	`geo` Tuple(city Nullable(String), country Nullable(String), continent Nullable(String), region Nullable(String), sub_continent Nullable(String), metro Nullable(String)),
	`app_info` Tuple(id Nullable(String), version Nullable(String), install_store Nullable(String), firebase_app_id Nullable(String), install_source Nullable(String)),
	`traffic_source` Tuple(name Nullable(String), medium Nullable(String), source Nullable(String)),
	`stream_id` Nullable(String),
	`platform` Nullable(String),
	`event_dimensions` Tuple(hostname Nullable(String)),
	`collected_traffic_source` Tuple(manual_campaign_id Nullable(String), manual_campaign_name Nullable(String), manual_source Nullable(String), manual_medium Nullable(String), manual_term Nullable(String), manual_content Nullable(String), gclid Nullable(String), dclid Nullable(String), srsltid Nullable(String)),
	`is_active_user` Nullable(Bool)
)
ENGINE = MergeTree
ORDER BY (event_timestamp, event_name, ga_session_id)


-- assumes you have site_pages_raw created by a spider and the embed function.

CREATE TABLE site_pages_raw
(
    `url` String,
    `raw_title` String,
    `raw_content` String,
     `title` String MATERIALIZED extractTextFromHTML(raw_title),
    `content` String MATERIALIZED extractTextFromHTML(raw_content),
    `date` DateTime MATERIALIZED now()
)
ORDER BY url


CREATE TABLE site_pages
(
    `url` String,
     `title` String MATERIALIZED extractTextFromHTML(raw_title),
    `content` String MATERIALIZED extractTextFromHTML(raw_content),
    `date` DateTime MATERIALIZED now()
)
ORDER BY url


INSERT INTO pages SELECT url, title, content, embed(content) as embedding FROM site_pages_raw SETTINGS  merge_tree_min_rows_for_concurrent_read = 1, merge_tree_min_bytes_for_concurrent_read=0, min_insert_block_size_rows=10, min_insert_block_size_bytes=0