
# ClickHouse and AWS Bedrock - A simple RAG pipeline

Files supporting blog post [Building a RAG pipeline for enhanced Google Analytics with ClickHouse and Amazon Bedrock]().

This simple RAG flow uses ClickHouse and Bedrock APIs to convert Google Analytics questions into a SQL responses.

Files:

- [embed.py](./embed.py) - Simple python UDF to generate an embedding using the `amazon.titan-embed-text-v1` model. Uses client from [bedrock.py](./bedrock.py).
- [bedrock_function.xml](./bedrock_function.xml) - ClickHouse config for above UDF.
- [questions.sql](./questions.sql) - Example questions seeded for the RAG flow.
- [question_to_sql.py](./question_to_sql.py) - RAG test script. Implements the RAG pipeline.
- [ga.sql](./ga.sql) - Schemas for Google Analytics and site data. See [Enhancing Google Analytics Data with ClickHouse](https://clickhouse.com/blog/enhancing-google-analytics-data-with-clickhouse) for more details.
- [spider][./spider] - Simple scrapy spider to generate site data. Specific to clickhouse.com but can be adapted.

Dependencies:

- python 3.10+
- ClickHouse instance with `amazon.titan-embed-text-v1` and `anthropic.claude-v2` models.
- Bedrock account with access to titan and 

Install Python dependencies:

```bash
pip install -r requirements.txt
```

## Running RAG Flow

Assumes ClickHouse port 8123 (non SSL).

```bash
export CLICKHOUSE_HOST=
export CLICKHOUSE_USERNAME=
export CLICKHOUSE_PASSWORD=

#optional AWS role and region
export AWS_ROLE=
export AWS_REGION=

python question_to_sql.py --question "What are the number of returning users per day for the month of October for doc pages?"
----------------------------------------------------------------------------------------------------
question: What are the number of returning users per day for the month of October for doc pages?

SELECT
    event_date,
    uniqExact(user_pseudo_id) AS returning_users
FROM ga_daily
WHERE event_name = 'session_start'
    AND page_location LIKE '%/docs/%'
    AND event_date BETWEEN '2022-10-01' AND '2022-10-31'
    AND (ga_session_number > 1 OR user_first_touch_timestamp < event_date)
GROUP BY event_date
ORDER BY event_date
```

## Example Questions

Example questions from [blog]().

1. "What are the number of returning users per day for the month of October for doc pages?"
1. "What are the number of new users for blogs about dictionaries over time?"
1. "What are the total sessions since January 2023 by month for pages where the url contains '/docs/en'?"
1. "What are the total page views over time?"
1. "How many active users have visited blogs about codecs and compression techniques?"
1. "What are the total users over time?"
1. "What are the total users over time for pages about materialized views?"
1. "What is the source of traffic over time?"
1. "What are the total website sessions for pages about Snowflake?"
1. "What are the average number views per blog post over time?"
1. "What is the average number of views for doc pages for each returning user per day?"
1. "How many users who visited the blog with the title 'Supercharging your large ClickHouse data loads - Tuning a large data load for speed?' were new?"
1. "For each day from September 2003 how many blog posts were published?"
1. "What was the ratio of new to returning users in October 2023?"


