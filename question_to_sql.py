#!/usr/bin/python3
import json
import os
import re
import sys
import argparse
from bs4 import BeautifulSoup
from tenacity import wait_random_exponential, stop_after_attempt, retry
from bedrock import get_bedrock_client
import clickhouse_connect

clickhouse_client = clickhouse_connect.get_client(host=os.getenv('CLICKHOUSE_HOST', default='localhost'),
                                                  username=os.getenv('CLICKHOUSE_USERNAME', default='default'),
                                                  password=os.getenv('CLICKHOUSE_PASSWORD', default=''))

client = get_bedrock_client(region=os.getenv('AWS_REGION', default='us-east-1'), silent=True, runtime=True,
                            assumed_role=os.getenv('AWS_ROLE', None))

accept = "application/json"
contentType = "application/json"


@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(20))
def bedrock_with_backoff(**kwargs):
    return client.invoke_model(**kwargs)


def generate_embedding(text):
    body = json.dumps({"inputText": text.strip()})
    response = bedrock_with_backoff(
        body=body, modelId="amazon.titan-embed-text-v1", accept=accept, contentType=contentType
    )
    response_body = json.loads(response.get("body").read())
    embedding = response_body.get("embedding")
    return json.dumps(embedding)


def find_example_questions_for_metrics(metrics):
    examples = []
    added = set()
    for metric in metrics:
        embedding = generate_embedding(metric)
        response = clickhouse_client.query(
            f"SELECT question, query FROM questions ORDER BY L2Distance(embedding, {embedding}) ASC LIMIT 1")
        for query in response.result_rows:
            if not query[0] in added:
                examples.append(f"/*Answer the following: {query[0].lower()}:*/\n{query[1]}")
            added.add(query[0])
    return examples


def find_pages_for_concept(concept, limit=3):
    embedding = generate_embedding(concept)
    response = clickhouse_client.query(
        f"SELECT url, title, content FROM site_pages ORDER BY cosineDistance(embedding, {embedding}) ASC LIMIT {limit}")
    return [result for result in response.result_rows]


def extract_by_tag(response: str, tag: str, extract_all=False):
    soup = BeautifulSoup(response, features="html.parser")
    results = soup.find_all(tag)
    if not results:
        return

    texts = [res.get_text() for res in results]
    if extract_all:
        return texts
    return texts[-1]


def extract_site_area_examples(question):
    areas = [("blog",
              "/*Answer the following: filter by blogs:*/ \n SELECT page_location FROM ga_daily WHERE page_location LIKE '%/blog/%'"),
             ("documentation",
              "/*Answer the following: filter by documentation:*/ \n SELECT page_location FROM ga_daily WHERE page_location LIKE '%/docs/%'"),
             ("doc",
              "/*Answer the following: filter by docs:*/ \n SELECT page_location FROM ga_daily WHERE page_location LIKE '%/docs/%'")]
    phrase_patterns = [re.compile(r'\b{}\s?\b'.format(area[0]), re.IGNORECASE) for area in areas]
    site_areas = set()
    i = 0
    for pattern in phrase_patterns:
        matches = pattern.findall(question.lower())
        for _ in matches:
            site_areas.add(areas[i][1])
        i += 1
    return list(site_areas)


def summary_phrases(concept, num_docs=3):
    pages = find_pages_for_concept(concept, limit=num_docs)
    words = []
    for c in pages:
        summary_prompt = f"""Human: Extract up to 3 keywords and phrases from the following text related to \"{concept}\".
            If words in \"{concept}\" are present include them.
            {c[2]}
            Put the extracted words in xml tags <word></word>.
            Assistant:
            """
        body = json.dumps({
            "prompt": summary_prompt,
            "max_tokens_to_sample": 4096,
            "temperature": 0,
            "top_p": 0.8,
            "stop_sequences": ["\n\nHuman:"]
        })
        response = bedrock_with_backoff(body=body,
                                        modelId="anthropic.claude-v2",
                                        accept=accept, contentType=contentType)
        response_body = json.loads(response.get("body").read())
        for phrase in extract_by_tag(response_body.get('completion'), "word", extract_all=True):
            words.append(phrase.strip().lower())
    return list(set(words))


def generate_sql(question, examples, debug=False):
    examples = '\n\n'.join(examples)
    sql_prompt_data = f"""\n\nHuman: You have to generate ClickHouse SQL using natural language query/request <request></request>. Your goal -- create accurate ClickHouse SQL statements and help user extract data from ClickHouse database. You will be provided with rules <rules></rules>, database schema <schema></schema> and relevant SQL statement examples </examples></examples>.

This is the table schema for ga_daily.

<schema>
CREATE TABLE ga_daily
(
    `event_date` Date,
    `event_timestamp` DateTime64(3),
    `event_name` Nullable(String),
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
ORDER BY event_timestamp
</schema>

<schema>
CREATE TABLE default.site_pages
(
    `url` String,
    `title` String,
    `content` String
)
ENGINE = MergeTree
ORDER BY url
</schema>

<rules> 
You can use the tables "ga_daily" and "site_pages".  

The table ga_daily contains website analytics data with a row for user events. The following columns are important:
    - event_name - A string column. Filter by 'first_visit' if identifying new users, 'session_start' for returning users and 'page_view' for page views.
    - event_date - A Date column on which the event occured
    - event_timestamp - A DateTime64(3) with the event time to milli-second accuracy
    - ga_session_id - A string identifying a user session.
    - ga_session_number - The session number for the user
    - user_pseudo_id - A string uniquely identifying a user
    - is_active_user - A boolean indicating if the user was active. True if active or engaged.
    - user_first_touch_timestamp - The first time a user visited the site.
    - page_location - the full url of the page. 
    - page_title - The page title e.g. for a doc or blog.
    - page_referer - The referer for the page. A full url.
    - traffic_source.name provides the source of the traffic.
</rules>

<examples>
{examples}
</examples>

<request> Considering all above generate a ClickHouse SQL statement for the following query:
<query>{question}</query></request>

Put the result statement in <sql></sql> tags:

\n\nAssistant:
"""
    if debug:
        print(f"prompt: \n{sql_prompt_data}")
    body = json.dumps({
        "prompt": sql_prompt_data,
        "max_tokens_to_sample": 3000,
        "temperature": 0,
        "top_k": 100,
        "stop_sequences": ["\n\nHuman:"]
    })
    response = bedrock_with_backoff(body=body,
                                    modelId="anthropic.claude-v2",
                                    accept=accept, contentType=contentType)
    response_body = json.loads(response.get("body").read())
    return response_body.get('completion')


def identify_concept(question):
    prompt_data = f"""Human: \n\nYou are an agent responsible for identifying the components of question which require a 
                text field to be searched in a database over just metadata. 
                These questions will be requesting analytics on pages of a website, where each page is a row in a database.
                One of these columns is a text field with the web page content. Not all questions need a text search.

                <examples>
                Examples of questions which need a text search:
                
                For the question: "show me new users over time for blogs about dictionaries"
                Answer: <words>dictionaries</words>
                
                For the question: "show me returning users over time for doc pages about TTL"
                Answer: <words>TTL</words>
            
                For the question: "show me returning users over time for pages containing Materialized views"
                Answer: <words>Materialized views</words>
                
                Examples of question which do not need a text search:
                
                For the question: "total sessions for pages where the url contains '/ru'"
                Answer: <words>all_docs</words>
                
                </examples>
                
                Analyze the following question:
                
                {question}
                
                Put the words in <words></words> tags. Respond with  "<words>all_docs</words>" if the question does 
                not filter on the text field. 
                
                Assistant:
                """
    body = json.dumps({
        "prompt": prompt_data,
        "max_tokens_to_sample": 1000,
        "temperature": 0,
        "top_k": 150,
        "stop_sequences": ["\n\nHuman:"]
    })
    response = bedrock_with_backoff(body=body, modelId="anthropic.claude-v2", accept=accept,
                                    contentType=contentType)
    response_body = json.loads(response.get("body").read())
    return extract_by_tag(response_body.get("completion").strip(), "words")


def extract_key_metrics(question):
    prompt_data = f"""Human: \n\nYou are an agent responsible for analyzing questions that would require analysis of a dataset using SQL.
                You must extract the key metrics for the SELECT clause that must be used in order to answer the question.
                Examples are provided in an <examples> tag.
                
                <examples>                
                Question: What are the number of new users over time for blogs about dictionaries?
                Answer: new users

                Question: What are the returning users over time for doc pages about TTL?
                Answer: returning users

                Question: What are the average number of views per week for documentation pages?
                Answer: average views

                Question: What is total number of sessions for pages where the url contains '/ru'?
                Answer: total sessions
                
                Question: How many blog posts have been published?
                Answer: unique pages

                Question: What is the average number of views per day for blog pages for each new user?
                Answer: average views

                Question: How many users who visited the blog with the title 'Optimizing ClickHouse with Schemas and Codecs' were active?
                Answer: active users
                </examples>

                Extract the key metric from the following question: 

                {question}

                Put each metric in a <metric></metric> tag.

                Assistant:
                """
    body = json.dumps({
        "prompt": prompt_data,
        "max_tokens_to_sample": 100,
        "temperature": 0,
        "top_k": 50,
        "stop_sequences": ["\n\nHuman:"]
    })
    response = bedrock_with_backoff(body=body, modelId="anthropic.claude-v2", accept=accept,
                                    contentType=contentType)
    response_body = json.loads(response.get("body").read())
    return extract_by_tag(response_body.get("completion").strip(), "metric", extract_all=True)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="RAG pipeline for Google Analytics with ClickHouse and Bedrock")
    parser.add_argument("--question", type=str, help="Question")
    parser.add_argument("--show_prompt", action="store_true", default=False, help="Show final model prompt")
    args = parser.parse_args()
    question = args.question
    try:
        print("-" * 100)
        print(f"question: {question}")
        key_metrics = extract_key_metrics(question)
        examples = extract_site_area_examples(question)
        if len(key_metrics) > 0:
            examples = examples + find_example_questions_for_metrics(key_metrics)
        concept = identify_concept(question)
        if concept != "all_docs":
            phrases = summary_phrases(concept, num_docs=3)
            question = f"{question}. For the topic of {concept}, filter by {','.join(phrases)}"
            filter = " OR ".join([f"content ILIKE '%{phrase}%'" for phrase in phrases])
            examples.append(
                f"/*Answer the following: To filter by pages containing words:*/ \n SELECT page_location FROM ga_daily WHERE page_location IN (SELECT url FROM site_pages WHERE {filter})\n")
        sql = generate_sql(question, examples, debug=args.show_prompt)
        print(extract_by_tag(sql, "sql"))
    except Exception as e:
        print(e)
    sys.stdout.flush()
