import requests
from pprint import pprint
import json
import pandas as pd
from pandas.io.json import json_normalize

notion_api_key = 'secret_JKwylCa8UrhDfUy5G6SKnESAEQpa6OT3OAkbGdEoMI4'
database_id = '80f35ee0fd20437cbf2717c350750ca3'

headers = {"Authorization": f"Bearer {notion_api_key}",
           "Content-Type": "application/json",
           "Notion-Version": "2021-05-13"}

body = {
    "filter": {
        "property": "Name",
        "text": {
            "is_not_empty": True
        }
    }
}
request_url = f'https://api.notion.com/v1/databases/{database_id}/query'

response = requests.request('POST', url=request_url, headers=headers, data=json.dumps(body))

pprint(response.json())

# data = response.json()
# print({k: v['type'] for k, v in data['properties'].items()})
