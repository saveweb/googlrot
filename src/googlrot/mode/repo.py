import logging
import asyncio

from motor.motor_asyncio import AsyncIOMotorCollection
from github import Github
from urlextract import URLExtract

from googlrot.url_type import BasicGooGlURL
from googlrot.util.utils import POISON, urls_pusher

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger.addHandler(logging.StreamHandler())
extractor = URLExtract(extract_localhost=False)

async def repo_mode(g: Github, googl_urls_collection: AsyncIOMotorCollection, bad_urls_collection: AsyncIOMotorCollection):
    googl_urls_queue = asyncio.Queue(maxsize=1000)
    bad_urls_queue = asyncio.Queue(maxsize=1000)


    print("Starting...")
    repos_count = 0
    async with asyncio.TaskGroup() as group:
        group.create_task(urls_pusher(urls_collection=googl_urls_collection, urls_queue=googl_urls_queue))
        group.create_task(urls_pusher(urls_collection=bad_urls_collection, urls_queue=bad_urls_queue))

        # org:saveweb goo.gl/ -language:HTML

        logger.info("Searching for repositories...")
        # for snippet in tqdm(g.search_code("goo.gl/uhr26v -is:archived -language:html AND NOT .goo.gl AND NOT /goo.gl\\/[^0-9a-zA-Z]/")):
        #     logger.info(f"== snippet [{snippet.repository.full_name}] ==")
            # snippet.decoded_content.decode()
        SHORTCODES_LOWERCASE = "abcdefghijklmnopqrstuvwxyz0123456789"
        for char in SHORTCODES_LOWERCASE:
            print(f"char: {char}")
            for repo in g.search_repositories(f"goo.gl/{char}"):
                repos_count += 1
                await asyncio.sleep(0)
                if not repo.description:
                    logger.debug(f"[repo] skipping {repo.full_name}")
                    continue # skip

                logger.info(f"== [repo] Processing [{repo.full_name}] ==")
                for url in extractor.gen_urls(repo.description):
                    assert isinstance(url, str)
                    if "goo.gl/" not in url:
                        logger.debug(f"skipping url {url}")
                        continue
                    await asyncio.sleep(0)
                    try:
                        stdurl = BasicGooGlURL(url)
                        logger.info(f"OK: {url} -> {stdurl}")
                        await googl_urls_queue.put(stdurl)
                    except Exception as e:
                        logger.error(f"Error: {e}")
                        await bad_urls_queue.put(url)

        await googl_urls_queue.put(POISON)
        await bad_urls_queue.put(POISON)

        await googl_urls_queue.join()
        await bad_urls_queue.join()

    
    assert googl_urls_queue.empty()
    assert bad_urls_queue.empty()

    logger.info(f"Processed {repos_count} repositories")