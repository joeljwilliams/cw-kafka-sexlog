#!/usr/bin/env python3
import asyncio
import logging
import time
import json
import sys
import os
import uuid

from collections import defaultdict

from telethon import TelegramClient
from asynckafka import Consumer

logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)


def get_env(name, message, cast=str):
    if name in os.environ:
        return cast(os.environ[name])
    while True:
        value = input(message)
        try:
            return cast(value)
        except ValueError as e:
            print(e, file=sys.stderr)
            time.sleep(1)


API_ID = get_env('API_ID', 'Enter your API ID: ', int)  # API id from https://my.telegram.org
API_HASH = get_env('API_HASH', 'Enter your API hash: ')  # API hash from https://my.telegram.org
TOKEN = get_env('TOKEN', 'Enter the bot token: ')  # Telegram bot toke from https://t.me/botfather
CHANNEL_ID = get_env('CHAN_ID', 'Enter your channel ID: ', int)  # Numeric channel id where we posting
NAME = TOKEN.split(':')[0]  # session name
INTERVAL = 10


Q = asyncio.Queue()
lock = asyncio.Lock()
loop = asyncio.get_event_loop()

bot = TelegramClient(NAME, API_ID, API_HASH, loop=loop)
bot.start(bot_token=TOKEN)


async def konsoom(k):
    """Coro to konsoom messages from kafka and dump it on Q to be processed"""
    async for message in k:
        data = json.loads(message.payload.decode())
        logging.debug(f"Received message: {data}")
        Q.put_nowait(data)


async def post_message():
    """Coro to fill bucket and dump to channel on INTERVAL seconds"""
    last_dump = time.time()
    bucket = defaultdict(list)
    post = str()
    while True:
        data = await Q.get()
        logging.debug(f"Popped off Q: {data}")
        line = "  <pre>{}{} => {}{}, {} x {}💰</pre>".format(data["sellerCastle"], data["sellerName"],
                                                             data["buyerCastle"], data["buyerName"],
                                                             data["qty"], data["price"])
        logging.debug(f"{line} added to bucket.")

        async with lock:
            bucket[data["item"]].append(line)

        if time.time() - last_dump >= INTERVAL and bucket:
            logging.debug("30s since last dump, dumping bucket to channel")
            async with lock:
                for item, lines in bucket.items():
                    post += f"<b>{item}:</b>\n"
                    post += "\n".join(lines)
                    post += "\n\n"
                await bot.send_message(CHANNEL_ID, post, parse_mode='HTML')
                last_dump = time.time()
                bucket.clear()
                post = str()


consumer = Consumer(
    brokers='digest-api.chtwrs.com:9092',
    topics=['cw2-deals'],
    group_id=str(uuid.uuid4()),
)
consumer.start()

asyncio.ensure_future(konsoom(consumer))
asyncio.ensure_future(post_message())

try:
    loop.run_forever()
finally:
    consumer.stop()
    loop.stop()

