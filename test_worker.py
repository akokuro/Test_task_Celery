from celery import Celery, Task

import requests
import xmltodict
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

app = Celery('tasks', 
    broker='amqp://myuser:mypassword@localhost:5672/myvhost',
    backend='rpc://myuser:mypassword@localhost:5672/myvhost'
    )

class get_docPublishDate(Task):
    name = "get_docPublishDate"
    retry_kwargs = {'max_retries': 5}

    def run(self, row_link, ua):
        row_link_xml = row_link.replace("view.html", "viewXml.html")
        xml_file = requests.get(row_link_xml, headers={'User-Agent': ua}).content
        doc = xmltodict.parse(xml_file)
        doc_data = next(iter(doc.values()))
        
        if "docPublishDate" in doc_data:
            return doc_data["docPublishDate"]   
        else:
            return None

class getLinks(Task):
    name = "getLinks"
    retry_kwargs = {'max_retries': 5}

    def getLink(self, row):
        return row.find('a', {'target':'_blank'})['href']

    def run(self, link, page_count):
        ua = UserAgent().chrome
        content = requests.get(link + str(page_count), headers={'User-Agent': ua}).content 
        soup = BeautifulSoup(content, "lxml")
        tasks = []
        rows = soup.body("div", {"class": "w-space-nowrap ml-auto registry-entry__header-top__icon"})
        for row in rows:
            row_link = "https://zakupki.gov.ru" + self.getLink(row)
            tasks.append(row_link)
        return tasks

app.tasks.register(getLinks())
app.tasks.register(get_docPublishDate())