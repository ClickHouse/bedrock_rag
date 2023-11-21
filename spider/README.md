# Example Spider for crawling clickhouse.com sites

This is an example spider. Users should adapt to their own site.

## Pre-requisites

- python3.10+
- clickhouse-connect
- [scrapy](https://scrapy.org/)

## Installing

`pip install -r requirements.txt`

Pre-create tables in ClickHouse.

### Table Schema for spider's target table

```sql
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
```

## Running

```shell 
scrapy runspider spider.py \
-s LOG_FILE=scrapy.log \
# ① ClickHouse connection settings for instance hosting the spider's target table
-a host=HOST \
-a port=PORT \
-a username=USERNAME  \
-a password=PASSWORD \
# ② ClickHouse target table settings
-a database=DATABASE \
-a table=TABLE
```
