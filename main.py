import asyncio
import configparser
import datetime
import sys
import traceback
import re
import sqlite3
import time
from threading import Thread

import requests
from telethon import *

try:
    config = configparser.ConfigParser()
    config.read('config.ini')
    ################################################
    api_id = config['USER']['ApiID']
    api_hash = config['USER']['ApiHash']
    listening_channel = config['USER']['ListenOn']
    webhook_list = config['USER']['Webhooks']
    #Wait time in seconds
    wait_time = 10
    ################################################
except Exception:
    traceback.print_exc()


def print_status():
    print('---------------------------------------')
    print('CURE-v01 started @', datetime.datetime.now())
    print('Listens: ', listening_channel)
    print('Webhook: ', webhook_list)
    print('---------------------------------------')


async def send_to_webhook(payload, webhook):
    print('Sending payload', payload, 'to webhook', webhook)
    try:
        r = requests.post(webhook, data=payload)
        print('Message sent')
    except:
        print('Error sending data to the webhook')

def start_thread(coin, time_to_wait):
    con = sqlite3.connect('cure_db.sqlite')
    cs = con.cursor()
    del_sql = f"DELETE from pending where coin = \"{coin}\""
    cs.execute(del_sql)
    con.commit()
    print(f'Started worker for coin {coin}')
    sql = ''' INSERT INTO pending(coin,begin_date,thread_id)
                  VALUES(?,?,?) '''
    pend = (coin, str(datetime.datetime.now()), str(coin+'-thread'))
    cs.execute(sql, pend)
    con.commit()
    #Now wait
    time.sleep(time_to_wait)
    print('Time Finished')
    cs.execute(del_sql)
    con.commit()

async def parser(message):
    print('')
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print('Incoming Signal |', datetime.datetime.now())

    lines = iter(message.splitlines())

    for eachline in lines:
        if ('SIGNAL BUY' in eachline.upper()):
            #Signal buy logic
            coin = re.findall(r"#(\w+)", eachline)[0]
            main = Thread(target=start_thread, args=[coin,wait_time], name=coin+'-thread')
            main.start()
            return
        if('BUY AGAIN' in eachline.upper()):
            #Buy again logic
            coin = re.findall(r"#(\w+)", eachline)[0]

            return
    return

async def Run():

    try:

        #Cache telegram entities
        dialogs = await client.get_dialogs()

        @client.on(events.NewMessage(chats=listening_channel))
        async def handler(event):
            await parser(event.raw_text)

        print_status()
        await client.run_until_disconnected()

    except Exception as e:
        print(e)
        print('Error occured. Telegram authentification failed')
        sys.exit('Program terminated')


conn = sqlite3.connect('cure_db.sqlite')
c = conn.cursor()

sql_create_table = """ CREATE TABLE IF NOT EXISTS pending (
                                        id integer PRIMARY KEY,
                                        coin text NOT NULL,
                                        begin_date text,
                                        thread_id text
                                    ); """
c.execute(sql_create_table)

cursor = c.execute('select * from pending;')

recovered_list = cursor.fetchall()

for entry in recovered_list:
    finishing_time = datetime.datetime.strptime(entry[2], '%Y-%m-%d %H:%M:%S.%f') + datetime.timedelta(seconds=wait_time)
    if(datetime.datetime.now() > finishing_time):
        c.execute(f"DELETE from pending where coin = \"{entry[1]}\"")
        conn.commit()
    else:
        #Reopen threads with remaining time
        seconds_remaining = (finishing_time - datetime.datetime.now()).seconds
        main = Thread(target=start_thread, args=[entry[1], seconds_remaining], name=entry[1] + '-thread')
        main.start()
        print("Recovered " + str(entry[1]) + " thread with " + str(finishing_time - datetime.datetime.now()) + " time remaining")

c.close()
conn.close()
print('Database fully loaded')

client = TelegramClient('Telegram_Session', api_id, api_hash).start()
loop = asyncio.get_event_loop()
loop.run_until_complete(Run())
loop.close()
