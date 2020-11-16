import logging

from motor.motor_asyncio import AsyncIOMotorClient
from .mongodb import db


async def connect_to_mongo():
    logging.info("Connecting to mongo...")
    db.client = AsyncIOMotorClient()
    logging.info("Connected!")


async def close_mongo_connection():
    logging.info("Closing connection to mongo...")
    db.client.close()
    logging.info("Connection to mongo closed!")