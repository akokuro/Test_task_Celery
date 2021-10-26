from test_worker import getLinks, get_docPublishDate

from fake_useragent import UserAgent

link = "https://zakupki.gov.ru/epz/order/extendedsearch/results.html?fz44=on&pageNumber="

tasks = []
for i in range(1, 3):
    tasks.append((getLinks().delay(link, i), i))

ua = UserAgent().chrome
result = {}
for task, page_number in tasks:
    result[page_number] = {}
    links = task.get(propagate=True) 
    for link in links:
        result[page_number][link] = get_docPublishDate().delay(link, ua)

for page in result:
    print("Page " + str(page))
    for link in result[page]:
        result[page][link] = result[page][link].get(propagate=True)
        print(link, result[page][link])
    print()
