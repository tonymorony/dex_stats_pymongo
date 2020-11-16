from celery import Celery
from celery.utils.log import get_task_logger

from dex_stats.parser import Parser

# Create the celery app and get the logger
celery_app = Celery('tasks', broker='pyamqp://guest@rabbit//')
logger = get_task_logger(__name__)

celery_app.conf.task_routes = {
        "app.tasks.celery_worker.test_celery": "test-queue"
}

p = Parser()

@celery_app.task(acks_late=True)
async def parser_init():
    await p.create_mongo_collections()