from kafka import KafkaProducer
import numpy as np
from datetime import datetime, timedelta
import time
import json
import pandas as pd
import random

def normalize_probabilities(values):
    total = values.sum()
    return values / total

zone_lookup = pd.read_csv('./mnt/bronze_input/taxi_zone_lookup.csv', usecols=['LocationID'])
valid_pu_ids = zone_lookup['LocationID'].astype(int).tolist()

pu_location_counts = pd.read_csv('pu_location_counts.csv', index_col=0)
do_location_counts  = pd.read_csv('do_location_counts.csv', index_col=0)
pu_do_matrix        = pd.read_csv('pu_do_matrix.csv', index_col=0)

# Cast index/columns về int
pu_location_counts.index = pu_location_counts.index.astype(int)
do_location_counts.index = do_location_counts.index.astype(int)
pu_do_matrix.index      = pu_do_matrix.index.astype(int)
pu_do_matrix.columns    = pu_do_matrix.columns.astype(int)

pu_location_counts = pu_location_counts.loc[
    pu_location_counts.index.intersection(valid_pu_ids)
]
pu_props = normalize_probabilities(pu_location_counts['proportion'])
pu_ids   = pu_props.index.tolist()
pu_probs = pu_props.values.tolist()

do_location_counts = do_location_counts.loc[
    do_location_counts.index.intersection(valid_pu_ids)
]
do_props_global = normalize_probabilities(do_location_counts['proportion'])
do_ids_global   = do_props_global.index.tolist()
do_probs_global = do_props_global.values.tolist()

pu_do_matrix = pu_do_matrix.loc[
    pu_do_matrix.index.intersection(valid_pu_ids),
    pu_do_matrix.columns.intersection(valid_pu_ids)
]

try:
    customers    = pd.read_csv('customers.csv')
    phonenumbers = customers['phonenumber'].dropna().tolist()
    if not phonenumbers:
        raise ValueError("Danh sách phonenumber trong customers.csv rỗng.")
except FileNotFoundError:
    raise FileNotFoundError("Không tìm thấy file customers.csv.")
except Exception as e:
    raise Exception(f"Lỗi khi đọc customers.csv: {str(e)}")

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

vendor_id_probs_normalized       = normalize_probabilities(pd.Series({1:0.244767, 2:0.755233}))
pickup_hour_probs_normalized     = normalize_probabilities(pd.Series({
    0:0.027322,1:0.018700,2:0.012725,3:0.008278,4:0.005728,5:0.005978,
    6:0.013571,7:0.028287,8:0.039875,9:0.042970,10:0.045172,11:0.048182,
    12:0.052431,13:0.053990,14:0.057841,15:0.059665,16:0.061520,17:0.068705,
    18:0.074774,19:0.065122,20:0.057418,21:0.057544,22:0.052695,23:0.041506
}))
pickup_dayofweek_probs_normalized = normalize_probabilities(pd.Series({
    0:0.114016,1:0.128650,2:0.146553,3:0.197010,4:0.141065,5:0.148261,6:0.124445
}))
passenger_count_probs_normalized = normalize_probabilities(pd.Series({
    0:0.012082,1:0.778419,2:0.143049,3:0.030730,4:0.016909,5:0.011283,6:0.007520,7:0.000006,9:0.000001
}))
ratecode_id_probs_normalized     = normalize_probabilities(pd.Series({
    1:0.945858,2:0.032203,3:0.002761,4:0.002127,5:0.006383,6:0.000001,99:0.010669
}))
store_and_fwd_flag_probs_normalized = normalize_probabilities(pd.Series({
    'N':0.996203,'Y':0.003797
}))

def generate_pickup_datetime():
    current_time = datetime.now()
    hour   = int(np.random.choice(list(pickup_hour_probs_normalized.index), p=pickup_hour_probs_normalized.values))
    dow    = int(np.random.choice(list(pickup_dayofweek_probs_normalized.index), p=pickup_dayofweek_probs_normalized.values))
    minute = np.random.randint(0,60)
    second = np.random.randint(0,60)
    # Điều chỉnh ngày cho đúng dayofweek
    diff_days = (dow - current_time.weekday()) % 7
    pickup_time = (current_time + timedelta(days=diff_days))\
                  .replace(hour=hour, minute=minute, second=second, microsecond=0)
    return pickup_time

def generate_location_ids():
    # pickup zone
    pu = int(np.random.choice(pu_ids, p=pu_probs))
    # downstream zone distribution
    if pu in pu_do_matrix.index:
        row = pu_do_matrix.loc[pu]
        row = row.loc[row.index.intersection(valid_pu_ids)]
        probs = normalize_probabilities(row)
        do_ids_local = probs.index.tolist()
        do_probs_local = probs.values.tolist()
    else:
        do_ids_local   = do_ids_global
        do_probs_local = do_probs_global
    do = int(np.random.choice(do_ids_local, p=do_probs_local))
    return str(pu), str(do)

def select_phonenumber():
    return random.choice(phonenumbers)

def generate_taxi_data():
    vendor_id         = str(np.random.choice(list(vendor_id_probs_normalized.index),
                                             p=vendor_id_probs_normalized.values))
    pickup_time       = generate_pickup_datetime()
    passenger_count   = str(np.random.choice(list(passenger_count_probs_normalized.index),
                                             p=passenger_count_probs_normalized.values))
    trip_distance     = round(max(0.1, min(50, np.random.normal(3.860859, 254.6735))), 2)
    ratecode_id       = str(np.random.choice(list(ratecode_id_probs_normalized.index),
                                             p=ratecode_id_probs_normalized.values))
    store_and_fwd     = np.random.choice(list(store_and_fwd_flag_probs_normalized.index),
                                         p=store_and_fwd_flag_probs_normalized.values)
    pu_location_id, do_location_id = generate_location_ids()
    phonenumber       = select_phonenumber()

    return {
        "VendorID": vendor_id,
        "tpep_pickup_datetime": pickup_time.strftime('%Y-%m-%d %H:%M:%S'),
        "passenger_count": passenger_count,
        "trip_distance": trip_distance,
        "RatecodeID": ratecode_id,
        "store_and_fwd_flag": store_and_fwd,
        "PULocationID": pu_location_id,
        "DOLocationID": do_location_id,
        "phonenumber": phonenumber
    }

topic = 'taxi-stream'
sleep_time = 0.5  # trung bình ~2 bản ghi/giây
sent = 0

while True:
    try:
        data = generate_taxi_data()
        producer.send(topic, data)
        sent += 1
        print(f"Sent: {data}")

        # Flush định kỳ
        if sent % 1000 == 0:
            producer.flush()

        # Jitter ±20%
        jitter = random.uniform(0.8, 1.2)
        time.sleep(sleep_time * jitter)

    except Exception as e:
        print(f"Error producing message: {e}")
        time.sleep(5)
