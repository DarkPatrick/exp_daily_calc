import time
import requests
from dotenv import dotenv_values


secrets: dict = dotenv_values(".env")
 
REDASH_API_KEY = secrets["redash_api_key"]
REDASH_BASE_URL = secrets["redash_base_url"]
QUERY_ID = 2232

headers = {
    'Authorization': f'Key {REDASH_API_KEY}',
    'Content-Type': 'application/json',
}
parameters = {
    "exp": "[UG Monetization] UG App: churn – winback right after cancel (2 iteration)",
    "source": ["UGT_IOS"]
}

# Step 1: Trigger query execution
response = requests.post(
    f"{REDASH_BASE_URL}/api/queries/{QUERY_ID}/results",
    # f"{REDASH_BASE_URL}/api/query_results",
    headers=headers,
    # json={
    #     "parameters": parameters
    # }
)

# response.raise_for_status()
res = response.json()
res['query_result']['id']
res['query_result'].keys()
# job = response.json().get('job')

# if not job:
#     print("No job returned. Response:", response.json())
#     exit()

job_id = res['query_result']['id']

# print(f"Started job {job_id}")


job_response = requests.get(
    f"{REDASH_BASE_URL}/api/jobs/9a853c3a-05f4-47c1-8eeb-a8a7b797fb1e",
    headers=headers
)
# job_response.raise_for_status()
job_info = job_response.json()

status = job_info['job']['status']


res = requests.get(
    f"{REDASH_BASE_URL}/status.json?api_key={REDASH_API_KEY}",
)
res.json()