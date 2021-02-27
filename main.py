import bs4 as bs 
import requests 
import json
from multiprocessing import Process, Queue
import time
import random
import sys

def thread_scraper_fn(q, page_info):
    print(f' Started: {page_info["name"]}({page_info["href"]})')
    scrape_start_time = time.time()
    page = 0
    while True:
        sys.stdout.flush()

        page_href = f'{page_info["href"]}&honnan={page * 10}'
        print("Request " + page_href)
        req_start_time = time.time()
        soup = bs.BeautifulSoup(requests.get('https://www.viccesviccek.hu/' + page_href).text, "lxml") 
        print(f"Got data and parsed page {page_href} in {(time.time() - req_start_time) * 1000} ms")

        for jid in (6, 8, 10, 14, 16, 18, 20, 24, 26, 28):
            matches = soup.select(f"body center center:nth-child(2) table tr:nth-child(2) td:nth-child(3) table tr td table:nth-child({jid}) tr:nth-child(2) td")
            if len(matches) == 0: # No match means that the page has no more content, which means that there are no more pages
                print("Break 25 " + page_info["name"])
                break
            t = matches[0].text
            q.put(t[:t.find("Szerinted")] ) # put stripped text in the queue
        else: # https://stackoverflow.com/questions/189645/how-to-break-out-of-multiple-loops
            page += 1
            #time.sleep(random.randint(0, 5))
            # Continue if the inner loop wasn't broken.
            continue
        # Inner loop was broken, break the outer.
        print(f'Category {page_info["name"]} has been finished in {(time.time() - scrape_start_time) * 1000} ms')
        q.put('STOP')
        break

if __name__ == "__main__":
    # [
    #   "href": eg.: /vicces_viccek
    #   "name": eg.: Összes
    #   "jokes": All jokes in this cateogry
    # ]
    categories = []
    soup = bs.BeautifulSoup(requests.get("https://www.viccesviccek.hu/vicces_viccek").text, "lxml") 
    for t in soup.select("body center center:nth-child(2) table tr:nth-child(2) td:nth-child(1) table tr.kistabla_sor td table table"):
        anchor_tag = t.tr.td.a
        cat_info = {
            "href": anchor_tag["href"],
            "name": anchor_tag.text,
            "jokes": []
        }

        q = Queue()
        cat_info["mp"] = {
            "process": Process(target=thread_scraper_fn, args=(q, cat_info, )), 
            "queue": q
        }

        categories.append(cat_info)
        cat_info["mp"]["process"].start()

    for v in categories:
        # Transfer queue items into the array
        for i in iter(v["mp"]["queue"].get, 'STOP'):
            v["jokes"].append(i)

        # And only now join the thread after queue 
        # transfer is done, otherwise some kind of deadlock occurs
        v["mp"]["process"].join()
        

        del v["mp"] # JSON serialization..
    print("All finished")
    
    with open("jokes.json", "w", encoding='utf-8') as f:
        json.dump(categories, f, indent=4, sort_keys=True, ensure_ascii=False)
    print("File wrote")
        