#!/usr/bin/python3
import json
import sys
from bedrock import get_bedrock_client
from tenacity import (
    retry,
    stop_after_attempt,
    wait_random_exponential,
)
import logging
logging.basicConfig(filename='embed.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# add assumed_role if required
bedrock_runtime = get_bedrock_client(region="us-east-1", silent=True)
accept = "application/json"
contentType = "application/json"
modelId = "amazon.titan-embed-text-v1"
char_limit = 10000  # 1500 tokens effectively


@retry(wait=wait_random_exponential(min=1, max=60), stop=stop_after_attempt(20))
def embeddings_with_backoff(**kwargs):
    return bedrock_runtime.invoke_model(**kwargs)


for size in sys.stdin:
    # collect batch to process
    for row in range(0, int(size)):
        try:
            text = sys.stdin.readline()
            text = text[:char_limit]
            body = json.dumps({"inputText": text})
            response = embeddings_with_backoff(
                body=body, modelId=modelId, accept=accept, contentType=contentType
            )
            response_body = json.loads(response.get("body").read())
            embedding = response_body.get("embedding")
            print(json.dumps(embedding))
        except Exception as e:
            logging.error(e)
            print(json.dumps([]))
    sys.stdout.flush()
