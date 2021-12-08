import os, sys, time, datetime, subprocess, logging, threading
from logging import getLogger, StreamHandler, Formatter, FileHandler
import requests
from bs4 import BeautifulSoup
from urllib.request import urlopen
from requests.compat import urljoin
from os import path
import re, os
import csv
import pandas as pd
from typing import Literal
from pprint import pprint
import json
import schedule

logger = getLogger(__name__)
logger.setLevel(logging.DEBUG)
# flogger = getLogger(__name__)
# flogger.setLevel(logging.DEBUG)
stream_handler = StreamHandler()
stream_handler.setLevel(logging.DEBUG)
# file_handler = FileHandler("C:/Dumps/log/".replace('/', os.sep) + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")[2:] + ".log")
# file_handler = FileHandler(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")[2:] + ".log")
# file_handler.setLevel(logging.DEBUG)
handler_format = Formatter('[ %(asctime)s ] [ %(funcName)s ] [ %(levelname)s ] [%(lineno)s] %(message)s')
stream_handler.setFormatter(handler_format)
# file_handler.setFormatter(handler_format)
logger.addHandler(stream_handler)
# flogger.addHandler(file_handler)

user_agent = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.115 Safari/537.36'
header = {
    'User-Agent': user_agent
}

class NotionClient:

    def __init__(self, key):
        self.key = key
        self.URL = 'https://api.notion.com/v1'
        self.headers = {"Authorization": f"Bearer {key}",
                        "Content-Type": "application/json",
                        "Notion-Version": "2021-08-16"}
        self.pageheaders = {"Authorization": f"Bearer {key}",
                        "Notion-Version": "2021-08-16"}
        self.body = {
            "filter": {
                "property": "Name",
                "text": {
                    "is_not_empty": True
                }
            }
        }

    def add_stock_to_database(self, database_id, time, company, url, title, category, rate, total, result, totalup, tags, start_date, end_date):
        logger.info("Starting add_stock_to_database func")
        end_point = '/pages'

        if time:
            prop_time = {"select": {"name": time}}
        else:
            prop_time = {"select": {"name": 'None'}}

        if company:
            prop_company = {"select": {"name": company}}
        else:
            prop_company = {"select": {"name": 'None'}}

        if url:
            prop_url = {"url": url}
        else:
            prop_url = {"url": None}

        if category:
            prop_category = {"select": {"name": category}}
        else:
            prop_category = {"select": {"name": 'None'}}

        if tags:
            if ',' in tags:
                prop_tag = {"multi_select": [{"name": tag} for tag in tags.split(',')]}
            else:
                prop_tag = {"multi_select": [{"name": tags}]}
        else:
            prop_tag = {"multi_select": [{"name": 'None'}]}

        if end_date:
            prop_date = {"date": {"start": start_date.isoformat(),
                                  "end": end_date.isoformat()}}
        else:
            prop_date = {"date": {"start": start_date.isoformat()}}

        prop_cbox = {"type": "checkbox", 'checkbox': False}


        body = {
            "parent": {
                "database_id": database_id},
            "properties": {
                "Time": prop_time,
                "Company": prop_company,
                "URL" : prop_url,
                "Name": {"title": [{"text": {"content": title}}]},
                "Category": prop_category,
                "Rate": {"number": float(rate[1:-1])/100},
                "Total": {"rich_text": [{"text": {"content": total[:-1]}}]},
                "Result": {"rich_text": [{"text": {"content": result}}]},
                "TotalUp": {"rich_text": [{"text": {"content": totalup}}]},
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

    def create_empty_block_on_page(self, page_id):
        end_point = f'/blocks/{page_id}/children'
        body = {
            "children": [
                {
                    'object': 'block',
                    'type': 'unsupported',
                    'unsupported': {}
                }
            ]
        }
        return requests.request('PATCH', url=self.URL + end_point, headers=self.headers, data=json.dumps(body))

    def E_create_database_on_page(self, page_id):
        end_point = f'/blocks/{page_id}/children'
        body = {
            "children": [
                {
                    "object": "block",
                    "type": "child_database",
                    "child_database": {
                        "title": "My database"
                    }
                }
            ]
        }
        return requests.request('PATCH', url=self.URL + end_point, headers=self.headers, data=json.dumps(body))

    def update_block(self, block_id, content):
        end_point = f'/blocks/{block_id}/'
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

    def get_child_database_on_page(self, page_id):
        db = self.get_block_list(page_id)
        for idx in range(len(db['results'])):
            type = db['results'][idx]['type']
            if type == 'child_database':
                page = db['results'][idx]['id']
                print('{} : id {}'.format(type, page))

    def get_database_pages(self, database_id):
        db = self.get_database(database_id)
        # pprint(db)
        # logger.info(db['results'])

        for idx in range(len(db['results'])):
            name = db['results'][idx]['properties']['Name']['title'][0]['text']['content']
            page = db['results'][idx]['id']
            print('{} : id {}'.format(name, page))

    def get_database(self, database_id):
        request_url = f'https://api.notion.com/v1/databases/{database_id}/query'

        response = requests.request('POST', url=request_url, headers=self.headers, data=json.dumps(self.body))

        pprint(response.json())

        return response.json()

    def get_block_list(self, page_id):
        # request_url = f'https://api.notion.com/v1/blocks/{page_id}'
        request_url = f'https://api.notion.com/v1/blocks/{page_id}/children'

        response = requests.request('GET', url=request_url, headers=self.pageheaders)
        pprint(response.json())

        db = response.json()
        for idx in range(len(db['results'])):
            # name = db['results'][idx]['properties']['Name']['title'][0]['text']['content']
            id = db['results'][idx]['id']
            print('id {}'.format(id))

        return response.json()

    def search_database(self, database_id, title):
        body = {
            "filter": {
                "property": "Name",
                "text": {
                    "contains": title
                }
            }
        }
        request_url = f'https://api.notion.com/v1/databases/{database_id}/query'
        db = requests.request('POST', url=request_url, headers=self.headers, data=json.dumps(body))
        db = db.json()
        titleID = []
        for idx in range(len(db['results'])):
            name = db['results'][idx]['properties']['Name']['title'][0]['text']['content']
            page = db['results'][idx]['id']
            logger.info('{} : id {}'.format(name, page))
            titleID.append({'title': name, 'id': page})
        return titleID

    def check_duplicate_db(self, database_id, ticker):
        body = {
            "filter": {
                "property": "Company",
                "select": {
                    "equals": ticker
                }
            }
        }
        request_url = f'https://api.notion.com/v1/databases/{database_id}/query'
        db = requests.request('POST', url=request_url, headers=self.headers, data=json.dumps(body))
        logger.info("res [status:{}]".format(db))
        db = db.json()
        pprint(db)
        titleID = []
        for idx in range(len(db['results'])):
            name = db['results'][idx]['properties']['Name']['title'][0]['text']['content']
            page = db['results'][idx]['id']
            date = db['results'][idx]['properties']['Date']['date']['start']
            logger.info('{} : id {} : date {}'.format(name, page, date))
            titleID.append({'title': name, 'id': page, 'date': date})
        date = datetime.datetime.now()
        check = None
        for idx in range(len(titleID)):
            logger.info("row:{}, now:{}".format(titleID[idx]['date'], date.date()))
            if titleID[idx]['date'] == str(date.date()):
            # if re.search(titleID[idx]['date'], str(date.date())):
                logger.info("row:{}, now:{}".format(titleID[idx]['date'], date.date()))
                check = False
                logger.info(check)
        logger.info(check)
        if check == False:
            return False
        else:
            return True


def crow_stock_prompt(inurl):
    # df = csv2df("./stock.csv")

    html = urlopen(inurl)
    bsObj = BeautifulSoup(html, "html.parser")

    table = bsObj.findAll("table", {"class":"table table-striped"})[0]
    rows = table.findAll("tr")

    num = 0
    for row in rows:
        csvRow = []
        for cell in row.findAll(['td', 'th']):
            if num > 5:
                res = re.sub("\n", "", cell.get_text())
                if res:
                    csvRow.append(res)
                link = cell.find('a')
                if link:
                    # logger.info(link.get('href'))
                    pattern = "/brand/.*"
                    match = re.search(pattern, link.get('href'))
                    # logger.info(match)
                    if match:
                        csvRow.append(match.group(0))
                        csvRow.append(match.group(0)[-4:])
            num += 1
        logger.info(csvRow)

        # logger.info(csvRow)
        add_row2csv(csvRow)
        try:
            crow_brand2Notion("https://kabureal.net" + csvRow[2], csvRow)
        except Exception as e:
            logger.debug(e)
        finally:
            pass
            # logger.debug("All finish")

def crow_brand2Notion(burl, row):
    logger.info("Starting crow_brand2Notion [burl:{}]".format(burl))
    notion_api_key = 'secret_JKwylCa8UrhDfUy5G6SKnESAEQpa6OT3OAkbGdEoMI4'
    notion_database_id = '146ccefc561a445680cfcd3a43cf8531'
    client = NotionClient(notion_api_key)
    # client.get_database_pages(notion_database_id)
    ticker = burl[-4:]
    logger.info("ticker : {}".format(ticker))
    date = datetime.datetime.now()
    if client.check_duplicate_db(notion_database_id, ticker):
        achieve = crow_brand_achievement(burl)
        res = client.add_stock_to_database(notion_database_id, row[0], row[3], "https://kabureal.net"+row[2], row[1], row[4], row[5], achieve[0], achieve[1], achieve[2], None, date.date(), None)
        logger.info("Add stock_info to DB [status:{}]".format(res))
        print(res.content)
        res = client.search_database(notion_database_id, ticker)
        logger.info("Searched {} : {}".format(ticker, res[0]['id']))
        crow_stock_brand(burl, res[0]['id'])

def crow_stock_brand(burl, page_id):
    logger.info("Starting crow_stock_brand func")
    pnglist = []
    response = requests.get(burl, headers=header)
    soup = BeautifulSoup(response.content,'lxml')

    notion_api_key = 'secret_JKwylCa8UrhDfUy5G6SKnESAEQpa6OT3OAkbGdEoMI4'
    notion_database_id = '146ccefc561a445680cfcd3a43cf8531'
    client = NotionClient(notion_api_key)
    logger.info("Generated NotionClient")

    main_title = soup.findAll("div", {"class":"span8"})[0]
    main_title = main_title.findAll("h1")
    res = client.create_heading_on_page(page_id,'heading_2', main_title[0].get_text())
    logger.info("Got Mian title : {}, [status:{}]".format(main_title[0].get_text(), res))

    # Get PNG data
    images = soup.find_all('img')
    for target in images:
        match = re.findall("https://rizimg2.net/.*png$", target['src'])
        if len(match)!=0:
            logger.info(match[0])
            pnglist.append(match[0])
    # print(pnglist)
    res = client.create_image_on_page(page_id, pnglist[0])
    logger.info("Got Mian Image : {}, [status:{}]".format(pnglist[0], res))

    title = soup.findAll("div", {"class":"mhd"})[0]
    title = title.findAll("h2")
    res = client.create_heading_on_page(page_id,'heading_2', title[0].get_text())
    logger.info("Got h2 Title : {}, [status:{}]".format(title[0].get_text(), res))

    table = soup.findAll("table", {"class":"tbl_18"})[0]
    rows = table.findAll("tr")
    ptalbe = []
    for row in rows:
        prow = []
        for cell in row.findAll(['td', 'th']):
            # print(cell.get_text())
            prow.append(cell.get_text())
        ptalbe.append(prow)
        content = "{0:10s}:{1:15s}:{2:20s}".format(prow[0], prow[1], prow[2])
        # print(content)
        res = client.create_paragraph_on_page(page_id, content)
        logger.info("Got Table Row : {}, [status:{}]".format(content, res))
    # print(ptalbe)

    plist = []
    past = soup.findAll("div", {"class" : "pright20"})[0]
    # print(past)
    plists_png = past.findAll("img", {"class" : "mtop018"})
    # print(plists_png)
    plists_exp = past.findAll("div", {"class" : "ptop2"})
    # print(plists_exp)
    for idx in range(len(plists_exp)):
        res_i = client.create_image_on_page(page_id, plists_png[idx]['src'])
        logger.info("Got Past Image : {}, [status:{}]".format(plists_png[idx]['src'], res_i))
        res_e = client.create_paragraph_on_page(page_id, plists_exp[idx].get_text())
        logger.info("Got Table Row : {}, [status:{}]".format(plists_exp[idx].get_text(), res_e))

def crow_brand_achievement(burl):
        logger.info("Starting crow_brand_achievement [burl:{}]".format(burl))
        response = requests.get(burl, headers=header)
        soup = BeautifulSoup(response.content,'lxml')

        table = soup.findAll("table", {"class":"tbl_18"})[0]
        rows = table.findAll("tr")
        ptalbe = []
        fordb = []
        for row in rows:
            prow = []
            for cell in row.findAll(['td', 'th']):
                # print(cell.get_text())
                prow.append(cell.get_text())
            ptalbe.append(prow)
            content = "{0:10s}:{1:15s}:{2:20s}".format(prow[0], prow[1], prow[2])
            # print(content)
            fordb.append(prow[1])
            logger.info("Got Table Row : {}".format(content))
        # print(ptalbe)
        return fordb

def initialize_csv():
    main_df = pd.DataFrame(
        [
            ['0001', '10:20', 'TEST (0000・東証)', '/brand/?code=3975', '0000', '三角保ち合い上放れ初日 ', '+97.67 %', '株価'],
            ['0002', '10:20', 'TEST (0000・東証)', '/brand/?code=3975', '0000', '三角保ち合い上放れ初日 ', '+97.67 %', '株価']
        ],
        columns=['id', 'time', 'name', 'url', 'ticker', 'reason', 'rate', 'stock'],
    )
    main_df.to_csv("stock.csv", mode="w")
    main_df.to_csv(sys.stdout)

def add_row2csv(csvRow):
    print(csvRow)
    df = pd.DataFrame(
        [],
        columns=['id', 'time', 'name', 'url', 'ticker', 'reason', 'rate', 'stock'],
    )

    print(df)
    if len(csvRow) != 0:
        df = df.append({'id': datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'), 'time': csvRow[0], 'name': csvRow[1], 'url': csvRow[2], 'ticker': csvRow[3], 'reason': csvRow[4], 'rate': csvRow[5], 'stock': csvRow[6]}, ignore_index=True)
        # name = pd.Series(csvRow, index=['id', 'time', 'name', 'url', 'ticker', 'reason', 'rate', 'stock'])
        # df.append(name, ignore_index=True)
        logger.info(df)

    df.to_csv("stock.csv", mode="a", header=False)
    df.to_csv(sys.stdout)

def df2csv(path_csv, df):
    logger.info("df2csv")
    # df.to_csv(sys.stdout)
    try:
        print(df)
        df.to_csv(path_csv, mode="a", header=False)
    except Exception as e:
        logger.debug(e)
    finally:
        pass
        # logger.debug("All finish")

def csv2df(path_csv):
    logger.info("csv2df")
    try:
        df = pd.read_csv(path_csv, encoding="utf_8")
        print(df)
    except Exception as e:
        logger.debug(e)
    finally:
        pass
        # logger.debug("All finish")

    return df

def main():
    # inurl = input("URL: ")
    inurl = "https://kabureal.net/prompt/"
    # inurl = "https://kabureal.net/prompt/?page=2"
    logger.debug(inurl)
    crow_stock_prompt(inurl)

def notion():
    logger.info("Starting notion")
    notion_api_key = 'secret_JKwylCa8UrhDfUy5G6SKnESAEQpa6OT3OAkbGdEoMI4'
    notion_database_id = '146ccefc561a445680cfcd3a43cf8531'
    client = NotionClient(notion_api_key)
    # client.get_database_pages(notion_database_id)
    ticker = "7594"
    res = client.check_duplicate_db(notion_database_id, ticker)
    logger.info(res)

def test():
    logger.info("Test Func Start")
    inurl = "https://kabureal.net/prompt/"
    # inurl = "https://kabureal.net/prompt/?page=2"
    # burl = "https://kabureal.net/brand/?code=7594"
    # crow_brand_achievement(burl)
    crow_stock_prompt(inurl)

def startJob():
    # 10分毎に実施するジョブ登録
    schedule.every(10).minutes.do(runJob)
    logger.info('startJob：' + str(datetime.datetime.now()))

def runJob():
    logger.info('runJob：' + str(datetime.datetime.now()))
    test()

def endJob():
    logger.info('endJob：' + str(datetime.datetime.now()))
    for jobV in schedule.jobs:
        if 'runJob()' in str(jobV):
            # メイン処理のジョブを削除
            schedule.cancel_job(jobV)
            break

def crow_schedule():
    schedule.every().day.at("09:02").do(startJob)
    schedule.every().day.at("15:10").do(endJob)

    while True:
        schedule.run_pending()
        time.sleep(1)
        logger.info("sleeping night....")

if __name__ == "__main__":
    # initialize_csv()
    # add_row2csv()
    # main()
    # test()
    # notion()
    crow_schedule()
