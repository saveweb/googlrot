import argparse
import asyncio
import logging
import logging.handlers
import os

from github import Github, Auth
from github.GithubException import UnknownObjectException as GithubUnknownObjectException
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from pymongo.server_api import ServerApi
from googlrot.mode.code import code_mode
from googlrot.mode.repo import repo_mode
from googlrot.util.utils import POISON, urls_pusher


MONGODB_URI = os.environ["MONGODB_URI"]
# AVOID_OWNERS = open("AVOID_OWNERS.txt", "r").readlines()
auth = Auth.Token(open("GH_TOKEN.env", "r").read().strip())

logger = logging.getLogger(__name__)


async def create_url_index(collection: AsyncIOMotorCollection):
    if "url_1" in await collection.index_information():
        return
    await collection.create_index("url", unique=True)
    logger.info(f"Created index for {collection.name}")

def argparser():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["repo", "code", "code_task_gen", "crawl"], default="crawl")
    return parser.parse_args()

async def main():
    args = argparser()
    mode = args.mode

    logger.setLevel(logging.INFO)
    logger.addHandler(logging.StreamHandler())

    g = Github(auth=auth) # Non-async is fine, due to github rate limit
    mclient = AsyncIOMotorClient(MONGODB_URI, server_api=ServerApi("1"))

    db = mclient["googlrot"]
    googl_urls_collection = db["googl_urls"]
    bad_urls_collection = db["bad_urls"]
    logger.info("Creating indexes...")
    c1 = create_url_index(googl_urls_collection)
    c2 = create_url_index(bad_urls_collection)
    await asyncio.gather(c1, c2)

    googl_perfix_queue_collection = db["googl_perfix_queue"]
    if "prefix_1" not in await googl_perfix_queue_collection.index_information():
        await googl_perfix_queue_collection.create_index("prefix", unique=True)

    if mode == "repo":
        await repo_mode(g, googl_urls_collection, bad_urls_collection)
    elif mode == "code":
        while not os.path.exists("stop"):
            await code_mode(g, googl_perfix_queue_collection, googl_urls_collection, bad_urls_collection)
    elif mode == "code_task_gen":
        SHORTCODES_LOWERCASE = "abcdefghijklmnopqrstuvwxyz0123456789"
        def generate_prefixes():
            for a in SHORTCODES_LOWERCASE:
                for b in SHORTCODES_LOWERCASE:
                    for c in SHORTCODES_LOWERCASE:
                        yield a + b + c
        prefixes = []
        for prefix in generate_prefixes():
            prefixes.append({"prefix": prefix, "status": "TODO"})
        print(len(prefixes))
        while prefixes:
            print(len(prefixes))
            await googl_perfix_queue_collection.insert_many(prefixes[:1000])
            prefixes = prefixes[1000:]
    elif mode == "crawl":
        import httpx
        import re
        from tqdm import tqdm
        import time
        import sqlite3

        def init_db():
            conn = sqlite3.connect("googl_urls.db")
            c = conn.cursor()
            # url, status, redirect_url
            c.execute("CREATE TABLE IF NOT EXISTS urls (url TEXT PRIMARY KEY, status TEXT, redirect_url TEXT)")
            conn.commit()
            return conn
        def get_crawled(conn: sqlite3.Connection)->set:
            c = conn.cursor()
            c.execute("SELECT url FROM urls")
            return set([row[0] for row in c.fetchall()])

        def write_url(conn: sqlite3.Connection, url, status, redirect_url):
            c = conn.cursor()
            c.execute("INSERT INTO urls (url, status, redirect_url) VALUES (?, ?, ?)", (url, status, redirect_url))
            print(f"{url} -> {status} -> {redirect_url[:64]}")

        def ratelimited(response: httpx.Response) -> bool:
            if 'Location' not in response.headers:
                return False
            result_url = response.headers['Location']
            response.content  # read the response to allow connection reuse
            return not not re.search(r'^https?://(?:www\.)?google\.com/sorry/index\?continue=https://goo.gl/[^&]+&q=', result_url)
        async def crawl():
            conn = init_db()
            googl_urls = []
            with open("googl_urls.github.txt") as f:
                for line in f:
                    googl_urls.append(line.strip())
            googl_urls = set(googl_urls)
            googl_urls.remove("") if "" in googl_urls else None
            googl_urls.remove("https://goo.gl/") if "https://goo.gl/" in googl_urls else None

            crawled = get_crawled(conn)            
            to_crawl = googl_urls - crawled

            print(f"Total: {len(googl_urls)}, Crawled: {len(crawled)}, To crawl: {len(to_crawl)}")

            # 302 is redirect, 404 is not found, 403 is banned, 302 to https://www.google.com/sorry/index* is rate limited, 200 is for deleted URLs

            sess = httpx.AsyncClient(timeout=6)
            sess.headers["User-Agent"] = "savethewebproject/googlrot (+github.com/saveweb)"
            
            to_crawl = list(to_crawl)
            to_crawl.sort()

            to_crawl_queue = asyncio.Queue()
            for url in to_crawl:
                to_crawl_queue.put_nowait(url)

            async def progress(conn: sqlite3.Connection):
                start = time.time()
                await asyncio.sleep(1)
                while True:
                    now = time.time()
                    # eta = (left / speed)
                    left = to_crawl_queue.qsize()
                    speed = (len(to_crawl) - left) / (now - start)
                    eta = left / speed
                    print(f"Progress: {to_crawl_queue.qsize()}/{len(to_crawl)}, ETA: {eta:.2f}s")
                    await asyncio.sleep(1)
                    conn.commit()

            async def crawl_worker():
                while True:
                    url = await to_crawl_queue.get()
                    try:
                        r = await sess.get(url, follow_redirects=False)
                        r.content
                        if ratelimited(r):
                            print("Rate limited")
                            await asyncio.sleep(3)
                            continue
                        if r.status_code == 302:
                            write_url(conn, url, "302", r.headers["Location"])
                        elif r.status_code == 301:
                            write_url(conn, url, "301", r.headers["Location"])
                        elif r.status_code == 404:
                            write_url(conn, url, "404", "")
                        elif r.status_code == 403:
                            write_url(conn, url, "403", "")
                        elif r.status_code == 200:
                            write_url(conn, url, "200", "")
                        elif r.status_code == 400:
                            if "blocked" in r.text:
                                write_url(conn, url, "400", "blocked")
                        else:
                            print("")
                            print(url)
                            print(f"Unknown status code {r.status_code}")
                            print(r.headers)
                            print(r.text)
                    except Exception as e:
                        print(f"Error: {e}")
                        continue
                    finally:
                        to_crawl_queue.task_done()
            async with asyncio.TaskGroup() as group:
                group.create_task(progress(conn=conn))
                for _ in range(50):
                    group.create_task(crawl_worker())
                await to_crawl_queue.join()
                conn.commit()

        await crawl()

if __name__ == "__main__":
    asyncio.run(main())
