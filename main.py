import asyncio
import configparser
import datetime
import re
import sqlite3
import sys
import threading
import time
import traceback
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
    # Wait time in seconds
    wait_time = 86400
    ################################################
except Exception:
    traceback.print_exc()


def print_status():
    print('---------------------------------------')
    print('CURE-v01 started @', datetime.datetime.now())
    print('Listens:', listening_channel)
    print('Webhook:', webhook_list)
    print('Waiting time:', datetime.timedelta(seconds=wait_time))
    print('---------------------------------------')


def send_to_webhook(payload, webhook):
    print(datetime.datetime.now(), '| Sending payload', payload, 'to webhook', webhook)
    try:
        r = requests.post(webhook, data=payload)
    except:
        print('Error sending data to the webhook')

def print_active_threads(var):
    print('---------------------------------------')
    print('Total threads active:', var-1)
    print('---------------------------------------')

def start_thread(coin, time_to_wait):
    con = sqlite3.connect('cure_db.sqlite')
    cs = con.cursor()
    del_sql = f"DELETE from pending where coin = \"{coin}\""
    cs.execute(del_sql)
    con.commit()
    print(datetime.datetime.now(), f'| Started worker for coin {coin} - waiting until {datetime.datetime.now() + datetime.timedelta(seconds=time_to_wait)}')
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print_active_threads(threading.active_count())
    sql = ''' INSERT INTO pending(coin,begin_date,thread_status)
                  VALUES(?,?,?) '''
    sql_log = ''' INSERT INTO log(coin,begin_date,thread_status)
                  VALUES(?,?,?) '''
    pend = (coin, str(datetime.datetime.now()), 'running')
    cs.execute(sql, pend)
    cs.execute(sql_log, pend)
    con.commit()
    finishing_at = datetime.datetime.now() + datetime.timedelta(seconds=time_to_wait)
    while 1:
        data = cs.execute(f'select * from pending where coin=\"{coin}\";').fetchall()[0]
        if finishing_at <= datetime.datetime.now():
            print()
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            print(datetime.datetime.now(), f'| {coin}-Thread finished')
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            print_active_threads(threading.active_count()-1)
            cs.execute(del_sql)
            con.commit()
            # send to webhook
            payload = f'symbol={coin}BTC,type=cancelorders;symbol={coin}BTC,type=market,side=sell,quantity=100;'
            send_to_webhook(payload, webhook_list)
            return
        else:
            if data[3] == 'stopped':
                print()
                print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                print(datetime.datetime.now(), f'| {coin}-Thread was stopped')
                print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
                print_active_threads(threading.active_count()-1)
                cs.execute(del_sql)
                con.commit()
                return


async def parser(message):
    print('')
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
    print('Incoming Signal |', datetime.datetime.now())

    lines = iter(message.splitlines())

    for eachline in lines:
        if ('SIGNAL BUY' in eachline.upper()):
            # Signal buy logic
            coin = re.findall(r"#(\w+)", eachline)[0]
            thread = Thread(target=start_thread, args=[coin, wait_time], name=coin + '-thread')
            thread.start()
            return
        if ('BUY AGAIN' in eachline.upper()):
            # Buy again logic
            coin = re.findall(r"#(\w+)", eachline)[0]
            print(datetime.datetime.now(), f'| Buy again received for {coin}')
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            update_sql = f'''UPDATE pending SET thread_status = 'stopped' WHERE coin = \"{coin}\";'''
            con = sqlite3.connect('cure_db.sqlite')
            crs = con.cursor()
            crs.execute(update_sql)
            con.commit()
            crs.close()
            con.close()
            time.sleep(2)
            new_time = wait_time / 2
            thread = Thread(target=start_thread, args=[coin, new_time], name=coin + '-thread')
            thread.start()
            return
        if ('REACHED' in eachline.upper()):
            # Target reached logic
            coin = re.findall(r"#(\w+)", eachline)[0]
            print(datetime.datetime.now(), f'| Target reached received for {coin}')
            print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
            update_sql = f'''UPDATE pending SET thread_status = 'stopped' WHERE coin = \"{coin}\";'''
            con = sqlite3.connect('cure_db.sqlite')
            crs = con.cursor()
            crs.execute(update_sql)
            con.commit()
            crs.close()
            con.close()
            return

    print('Sintax not found')
    print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')

    return


async def Run():
    try:

        # Cache telegram entities
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
                                        thread_status text
                                    ); """

sql_create_log = """ CREATE TABLE IF NOT EXISTS log (
                                        id integer PRIMARY KEY,
                                        coin text NOT NULL,
                                        begin_date text,
                                        thread_status text
                                    ); """
c.execute(sql_create_table)
c.execute(sql_create_log)

cursor = c.execute('select * from pending;')

recovered_list = cursor.fetchall()

for entry in recovered_list:
    finishing_time = datetime.datetime.strptime(entry[2], '%Y-%m-%d %H:%M:%S.%f') + datetime.timedelta(seconds=wait_time)
    if (datetime.datetime.now() > finishing_time):
        c.execute(f"DELETE from pending where coin = \"{entry[1]}\"")
        conn.commit()
    else:
        # Reopen threads with remaining time
        seconds_remaining = (finishing_time - datetime.datetime.now()).seconds
        main = Thread(target=start_thread, args=[entry[1], seconds_remaining], name=entry[1] + '-thread')
        main.start()
        print("Recovered " + str(entry[1]) + " thread with " + str(
            finishing_time - datetime.datetime.now()) + " time remaining")

c.close()
conn.close()
print('Database fully loaded')

client = TelegramClient('Telegram_Session', api_id, api_hash).start()
loop = asyncio.get_event_loop()
loop.run_until_complete(Run())
loop.close()
