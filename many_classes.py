import weaviate
import time
import random
import sys
from loguru import logger

# TODO: When this turns into a PR this should be moved into a chaos pipeline
# repo. But during development it's easier to have this in the same repo.

client = weaviate.Client("http://localhost:8080", timeout_config=(20, 240))

timeout = 10

checkpoint = time.time()
interval = 100
for i in range(1_000):
    if i != 0 and i % interval == 0:
        avg = (time.time() - checkpoint) / interval
        logger.info(f"avg create duration is {avg} over past {interval} creates")
        checkpoint = time.time()

    before = time.time()
    client.schema.create_class(
        {
            "class": "Article" + str(i),
            "description": "A written text, for example a news article or blog post",
            "properties": [
                {
                    "dataType": ["string"],
                    "name": "title",
                },
                {"dataType": ["text"], "name": "content"},
                {"dataType": ["int"], "name": "int"},
                {"dataType": ["number"], "name": "number"},
            ],
            "shardingConfig": {
                "desiredCount": 1,
                "virtualPerPhysical": 1,
            },
        }
    )
    took = time.time() - before
    if took > timeout:
        logger.error(
            f"last class action took {took}s, but toleration limit is {timeout}s"
        )
        sys.exit(1)

# i = 0
# checkpoint = time.time()
# interval = 10
# while True:
#     if i != 0 and i % interval == 0:
#         logger.info(
#             f"avg delete duration is {(time.time()-checkpoint)/interval} over past {interval} deletes"
#         )
#         checkpoint = time.time()

#     res = client.schema.get()
#     if len(res["classes"]) == 0:
#         break

#     to_delete = random.choice(res["classes"])["class"]
#     before=time.time()
#     client.schema.delete_class(to_delete)
#     took = time.time()-before
#     if took > timeout:
#         logger.error(f"last class action took {took}s, but toleration limit is {timeout}s")
#         sys.exit(1)
#     i += 1