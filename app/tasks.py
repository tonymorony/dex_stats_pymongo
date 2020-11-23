from celery import Celery
from celery.utils.log import get_task_logger

from dex_stats.parser import Parser
from dex_stats.fetcher import Fetcher


celery_app = Celery('tasks', broker='pyamqp://guest@rabbit//')
logger = get_task_logger(__name__)


@celery_app.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(60.0, parser_init.s(), name='init')


@celery_app.task
def parser_init():
    p = Parser()
    p.create_mongo_collections()
    f = Fetcher()
    f.pipeline()
    p.clean_up()