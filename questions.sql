CREATE TABLE raw_questions
(
    `question` String,
    `query` String
)
ENGINE = MergeTree
ORDER BY question

INSERT INTO raw_questions (question, query) VALUES
('total users', $$SELECT uniq(user_pseudo_id) AS total_users FROM ga_daily WHERE event_name = 'session_start'$$),
('active users', $$SELECT uniq(user_pseudo_id) AS active_users FROM ga_daily WHERE ((event_name = 'session_start') AND is_active_user) OR (event_name = 'first_visit')$$),
('total page views', $$SELECT count() as total_page_views FROM ga_daily WHERE (event_name = 'page_view')$$),
('new users', $$SELECT count() AS new_users FROM ga_daily WHERE event_name = 'first_visit'$$),
('returning users', $$SELECT uniqExact(user_pseudo_id) AS returning_users FROM ga_daily WHERE (event_name = 'session_start') AND is_active_user AND (ga_session_number > 1 OR user_first_touch_timestamp < event_date)$$),
('total sessions', $$SELECT uniqExact(ga_session_id, '_', user_pseudo_id) AS total_sessions FROM ga_daily$$),
('average views per page', $$SELECT page_location,count() FROM ga_daily WHERE (event_name = 'page_view') GROUP BY page_location$$)

CREATE TABLE questions
(
    `question` String,
    `query` String,
    `embedding` Array(Float32)
)
ENGINE = MergeTree
ORDER BY question

INSERT INTO questions SELECT
    question,
    query,
    embed(question) AS embedding
FROM raw_questions