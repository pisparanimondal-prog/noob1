"""
OTP Bot — Main entry point
Starts the multi-API OTP polling bot + Telegram admin bot in parallel threads.
"""
import threading
import logging
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s [%(name)s]: %(message)s',
    datefmt='%H:%M:%S'
)

BOT_DIR = os.path.dirname(__file__)
CONFIG_PATH = os.path.join(BOT_DIR, 'config.json')


def main():
    with open(CONFIG_PATH) as f:
        config = json.load(f)

    # Start Telegram admin bot in background thread
    from telegram_admin import start_admin_bot
    admin_thread = threading.Thread(
        target=start_admin_bot,
        daemon=True,
        name='telegram-admin'
    )
    admin_thread.start()
    logging.info('Telegram admin bot thread started')

    # Start OTP polling bot (blocks main thread)
    from otp_bot import start_bot
    start_bot()


if __name__ == '__main__':
    main()
