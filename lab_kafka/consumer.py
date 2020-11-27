import json
import sys
from json.decoder import JSONDecodeError
from time import sleep

import pandas as pd
from kafka import KafkaConsumer


def main(args):
    try:
        topic_name = args[1]
    except IndexError:
        topic_name = 'btc_transactions'
    consumer = KafkaConsumer(topic_name,
                             bootstrap_servers='localhost',
                             group_id=f'{topic_name}_group',
                             enable_auto_commit=False)
    memory_df = empty_df()
    while True:
        if memory_df.empty:
            memory_df = take_top_ten_by_price(poll(consumer))
            show_df_and_commit(memory_df, consumer)
            continue
        delta_df = poll(consumer)
        memory_df = take_top_ten_by_price(pd.concat([memory_df, delta_df]))
        show_df_and_commit(memory_df, consumer)


def get_message_data(message):
    # it is also open question how to better unpack messages:
    # with this implementation or with pd.from_json
    if not message.value:
        return
    m = message.value.decode('utf-8')
    if not m:
        return
    try:
        message_data = json.loads(m, encoding='utf-8').get('data')
        return message_data
    except JSONDecodeError:
        # Some broken value happen we can handle it somehow, but skipping for now
        return


def df_from_messages(msg_pack):
    df_list = [pd.DataFrame([get_message_data(message)
                             for message in messages
                             if get_message_data(message)],
                            columns=columns())
               for tp, messages in msg_pack.items()]
    if df_list:
        return pd.concat(df_list)
    return empty_df()


def poll(consumer):
    msg_pack = consumer.poll(timeout_ms=5000)
    return df_from_messages(msg_pack)


def take_top_ten_by_price(df):
    return df.sort_values('price', ascending=False).head(10)


def show_current_top(df):
    print(df) if not df.empty else print('We have no data yet.')


def empty_df():
    return pd.DataFrame(columns=columns())


def columns():
    return ["id",
            "id_str",
            "order_type",
            "datetime",
            "microtimestamp",
            "amount",
            "amount_str",
            "price",
            "price_str"]


def show_df_and_commit(df, consumer):
    show_current_top(df)
    consumer.commit()
    # this code is example, so have sleep here to not spam stdout
    sleep(1)


if __name__ == '__main__':
    main(sys.argv)
