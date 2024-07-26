import argparse
import asyncio
import logging
import logging.handlers
import os

from github import Github, Auth
from github.GithubException import UnknownObjectException as GithubUnknownObjectException
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from pymongo.server_api import ServerApi
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
    parser.add_argument("--mode", choices=["repo", "code", "code_task_gen"], default="code")
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

async def code_mode(g: Github, googl_perfix_queue_collection: AsyncIOMotorCollection, googl_urls_collection: AsyncIOMotorCollection, bad_urls_collection: AsyncIOMotorCollection):
    # for snippet in tqdm(g.search_code("goo.gl/uhr26v -is:archived -language:html AND NOT .goo.gl AND NOT /goo.gl\\/[^0-9a-zA-Z]/")):
    from urlextract import URLExtract
    from googlrot.url_type import BasicGooGlURL
    extractor = URLExtract(extract_localhost=False)

    googl_urls_queue = asyncio.Queue(maxsize=1000)
    bad_urls_queue = asyncio.Queue(maxsize=1000)

    task = await googl_perfix_queue_collection.find_one_and_update({"status": "TODO"}, {"$set": {"status": "PROCESSING"}})
    if task is None:
        logger.info("No more tasks")
        return

    prefix:str = task["prefix"]
    assert len(prefix) == 3

    logger.info(f"Processing prefix {prefix}")
    OK_urls_count = 0
    BAD_urls_count = 0
    async with asyncio.TaskGroup() as group:
        group.create_task(urls_pusher(urls_collection=googl_urls_collection, urls_queue=googl_urls_queue))
        group.create_task(urls_pusher(urls_collection=bad_urls_collection, urls_queue=bad_urls_queue))

        # for result in g.search_code(f"goo.gl/{prefix} -is:archived AND NOT .goo.gl AND NOT /goo.gl\\/[^0-9a-zA-Z]/"):
        for result in g.search_code(f"goo.gl/{prefix} AND NOT is:fork"):
            logger.info(f"Processing {result.repository.full_name} ==")

            try:
                content = result.decoded_content.decode("utf-8")
            except GithubUnknownObjectException as e:
                if e.status == 404:
                    logger.error(f"404: {e}, skip")
                    continue
                else:
                    raise e
            except UnicodeDecodeError:
                logger.error(f"UnicodeDecodeError: {result.repository.full_name}, skip")
                continue
            print("conetnt: ", content[:256])
            for url in extractor.gen_urls(content):
                assert isinstance(url, str)
                if "goo.gl/" not in url:
                    logger.debug(f"skipping url {url}")
                    continue
                try:
                    stdurl = BasicGooGlURL(url)
                    await googl_urls_queue.put(stdurl)
                    logger.info(f"OK: {url} -> {stdurl}")
                    OK_urls_count += 1
                except Exception as e:
                    logger.error(f"Error: {e} -> {url}")
                    await bad_urls_queue.put(url)
                    BAD_urls_count += 1

        await googl_urls_queue.put(POISON)
        await bad_urls_queue.put(POISON)

        await googl_urls_queue.join()
        await bad_urls_queue.join()

    logger.info(f"OK: {OK_urls_count}, BAD: {BAD_urls_count}")

    # set task to DONE
    await googl_perfix_queue_collection.find_one_and_update({"prefix": prefix}, {"$set": {"status": "DONE"}})
    logger.info(f"Finished prefix {prefix} -> DONE")

if __name__ == "__main__":
    asyncio.run(main())