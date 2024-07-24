import asyncio
import logging
import time


from motor.motor_asyncio import AsyncIOMotorCollection
import pymongo.errors


POISON = False
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())


async def insert_many_ok(collection: AsyncIOMotorCollection, urls: set|list):
    try:
        r = await collection.insert_many([{"url": url} for url in urls], ordered=False)
        logger.info(f"Inserted {len(r.inserted_ids)} urls to {collection.name}")
    except pymongo.errors.BulkWriteError as e:
        for error in e.details["writeErrors"]:
            if error["code"] == 11000:
                pass # duplicate ok
            else:
                raise e
        logger.info(f"Inserted {len(urls)-len(e.details['writeErrors'])} urls to {collection.name}")


async def urls_pusher(urls_collection: AsyncIOMotorCollection, urls_queue: asyncio.Queue):
    urls_batch_size = 100
    urls = set()
    logger.info(f"urls_pusher for {urls_collection.name} started")
    last_pushed_at = 0
    while True:
        url = await urls_queue.get()
        if url is POISON:
            logger.info(f"urls_pusher for {urls_collection.name} got poison")
            break
        urls.add(url)
        if len(urls) >= urls_batch_size or time.time() - last_pushed_at > 10:
            await insert_many_ok(urls_collection, urls)
            urls.clear()
            last_pushed_at = time.time()
        urls_queue.task_done()
    if urls:
        await insert_many_ok(urls_collection, urls)
        urls.clear()
    logger.info(f"urls_pusher for {urls_collection.name} finished")
    urls_queue.task_done() # poison ack
