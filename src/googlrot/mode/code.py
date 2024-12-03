
import asyncio
import logging

from github import Github

from github.GithubException import UnknownObjectException as GithubUnknownObjectException
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection

from googlrot.util.utils import POISON, urls_pusher

logger = logging.getLogger(__name__)

async def code_mode(g: Github, googl_perfix_queue_collection: AsyncIOMotorCollection, googl_urls_collection: AsyncIOMotorCollection, bad_urls_collection: AsyncIOMotorCollection):
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

        for result in g.search_code(f"goo.gl/{prefix} AND NOT is:fork"):
            logger.info(f"Processing {result.repository.full_name} ==")

            try:
                content = result.decoded_content.decode("utf-8")
            except TypeError:
                logger.error(f"TypeError: {result.repository.full_name}, skip")
                continue
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
            try:
                urls = extractor.find_urls(content)
            except UnicodeError:
                logger.error(f"UnicodeError: {result.repository.full_name}, skip")
                with open(f"UnicodeError.{prefix}.txt", "w") as f:
                    f.write(content)
                continue
            for url in urls:
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
