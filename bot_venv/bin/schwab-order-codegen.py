#!/Users/patrickhillstrom/dev_projects/python-trading-bot-with-thinkorswim/bot_venv/bin/python3.12
from schwab.scripts.orders_codegen import latest_order_main

if __name__ == '__main__':
    import sys
    sys.exit(latest_order_main(sys.argv[1:]))
