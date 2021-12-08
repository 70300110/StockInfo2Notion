import requests
import json
import datetime
import csv, sys
from typing import Literal
from pprint import pprint
import pandas as pd

class NotionClient:

    def __init__(self, key):
        self.key = key
        self.URL = 'https://api.notion.com/v1'
        self.headers = {"Authorization": f"Bearer {key}",
                        "Content-Type": "application/json",
                        "Notion-Version": "2021-05-13"}
        self.body = {
            "filter": {
                "property": "Name",
                "text": {
                    "is_not_empty": True
                }
            }
        }

    def add_quest_to_database(self, database_id, title, category, time, tags, start_date, end_date):
        end_point = '/pages'

        if end_date:
            prop_date = {"date": {"start": start_date.isoformat(),
                                  "end": end_date.isoformat()}}
        else:
            prop_date = {"date": {"start": start_date.isoformat()}}

        if category:
            prop_category = {"select": {"name": category}}
        else:
            prop_category = {"select": {"name": 'None'}}

        if time:
            prop_time = {"select": {"name": time}}
        else:
            prop_time = {"select": {"name": 'None'}}

        prop_cbox = {"type": "checkbox", 'checkbox': False}

        if tags:
            if ',' in tags:
                prop_tag = {"multi_select": [{"name": tag} for tag in tags.split(',')]}
            else:
                prop_tag = {"multi_select": [{"name": tags}]}
        else:
            prop_tag = {"multi_select": [{"name": 'None'}]}

        body = {
            "parent": {
                "database_id": database_id},
            "properties": {
                "Name": {"title": [{"text": {"content": title}}]},
                "Category": prop_category,
                "Time": prop_time,
                "Tags": prop_tag,
                "Date": prop_date,
                "Done": prop_cbox,
            }}

        return requests.request('POST', url=self.URL + end_point, headers=self.headers, data=json.dumps(body))

    def create_heading_on_page(self, page_id, heading_type: Literal['heading_1', 'heading_2', 'heading_3'], content):
        end_point = f'/blocks/{page_id}/children'
        body = {
            "children": [
                {
                    "object": "block",
                    "type": heading_type,
                    heading_type: {"text": [{"type": "text", "text": {"content": content}}]}
                }
            ]
        }
        return requests.request('PATCH', url=self.URL + end_point, headers=self.headers, data=json.dumps(body))

    def create_paragraph_on_page(self, page_id, content):
        end_point = f'/blocks/{page_id}/children'
        body = {
            "children": [
                {
                    "object": "block",
                    "type": "paragraph",
                    "paragraph": {
                        "text": [
                            {"type": "text", "text": {"content": content, }}
                        ]
                    }
                }
            ]
        }
        return requests.request('PATCH', url=self.URL + end_point, headers=self.headers, data=json.dumps(body))

    def create_image_on_page(self, page_id, image_url):
        end_point = f'/blocks/{page_id}/children'
        body = {
            "children": [
                {
                    "object": "block",
                    "type": "image",
                    # ...other keys excluded
                    "image": {
                        "type": "external",
                        "external": {
                            "url": image_url
                        }
                    }
                }
            ]
        }
        return requests.request('PATCH', url=self.URL + end_point, headers=self.headers, data=json.dumps(body))

    def create_database_on_page(self, page_id):
        pass

    def get_database_list():
        pass

    def get_page_list():
        pass

    def get_database(database_id):
        request_url = f'https://api.notion.com/v1/databases/{database_id}/query'

        response = requests.request('POST', url=request_url, headers=headers, data=json.dumps(body))

        pprint(response.json())



def initialize_csv():
    main_df = pd.DataFrame(
        [
            ["0001", "Drink GBC", "Road to Millonare", "Daily", "test,quest", datetime.datetime(year=2021, month=9, day=17), datetime.datetime(year=2021, month=9, day=17)],
            ["0002", "Drink BBM", "Road to Health", "Daily", "test,quest", datetime.datetime(year=2021, month=9, day=17), datetime.datetime(year=2021, month=9, day=17)]
        ],
        columns=['id', 'title', 'category', 'time', 'tags', 'start_date', 'end_date'],
    )
    main_df.to_csv("quest.csv", mode="a")
    main_df.to_csv(sys.stdout)

def control_csv():
    df = pd.DataFrame(
        [
            ["0003", "Drink GBC", "Road to Millonare", "Daily", "test,quest", datetime.datetime(year=2021, month=9, day=17), datetime.datetime(year=2021, month=9, day=17)],
            ["0004", "Drink BBM", "Road to Health", "Daily", "test,quest", datetime.datetime(year=2021, month=9, day=17), datetime.datetime(year=2021, month=9, day=17)]
        ]
    )

    df.to_csv("quest.csv", mode="a", header=False)
    df.to_csv(sys.stdout)

def test(notion_database_id, client):
    df = pd.read_csv('./quest.csv')
    for index, row in df.iterrows():
        sd = row.start_date.split("-")
        ed = row.end_date.split("-")
        res = client.add_quest_to_database(
            notion_database_id,
            title=row.title,
            category=row.category,
            time=row.time,
            tags=row.tags,
            start_date=datetime.datetime(year=int(float(sd[0])), month=int(float(sd[1])), day=int(float(sd[2]))),
            end_date=datetime.datetime(year=int(float(ed[0])), month=int(float(ed[1])), day=int(float(ed[2])))
        )
        pprint(res.json())


def main():
    notion_api_key = 'secret_JKwylCa8UrhDfUy5G6SKnESAEQpa6OT3OAkbGdEoMI4'
    notion_database_id = '80f35ee0fd20437cbf2717c350750ca3'
    client = NotionClient(notion_api_key)
    # test(notion_database_id, client)
    res = client.create_image_on_page("9651e4df-eab8-4e1c-96a8-4aec293e18b7", "https://rizimg2.net/img/chart/l/20210921/7/7050.png")
    pprint(res.json())

if __name__ == "__main__":
    # initialize_csv()
    # control_csv()
    main()
