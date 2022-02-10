import time, datetime
from argparse import ArgumentParser
from faker import Faker
from threading import Thread
from tqdm import trange
import os

BUFFER_LIMIT = 1000000
MAX_RECORDS_PER_THREAD = 10000000
DEFAULT_RECORDS = 1000000000
MIN_THREADS = 1
MAX_THREADS = 4
DEFAULT_START_INDEX = 0


class DataTask(object):
    def __init__(self, total_records):
        self.faker = Faker()
        Faker.seed(total_records)

    def orders(self, index, total_records, records, orders_fd):
        faker = self.faker
        orders = []
        for ind in range(records):
            mfd_date = faker.date_time()
            sold_date = faker.date_time_ad(start_datetime=mfd_date)
            order_id = ind + index
            order_record = \
                f"{order_id},{faker.pyint(min_value=1, max_value=total_records)}," \
                f"{faker.name()}," \
                f"{faker.pyfloat(left_digits=3, right_digits=10, positive=True, min_value=1, max_value=100)}," \
                f"{faker.pyint(min_value=1, max_value=100)},{faker.coordinate()},{faker.pybool()}," \
                f"{mfd_date.date().isoformat()},{sold_date.isoformat()}\n"
            orders.append(order_record)
        orders_fd.writelines(orders)
        return

    def profiles(self, index, total_records, records, profiles_fd):
        faker = self.faker
        profiles = []
        print(records)
        for ind in range(records):
            profile = faker.simple_profile()
            profile_id = index + ind
            profile_record = f"{profile_id}," \
                             f"{profile['username']}," \
                             f"{profile['name']},{profile['sex']},{profile['mail']},{profile['birthdate']}\n"
            profiles.append(profile_record)
        profiles_fd.writelines(profiles)
        return


def gen_task(start_index, end_index, buffer_size):
    _records = end_index - start_index
    chunks = int(_records / buffer_size)
    data_task = DataTask(_records)
    return data_task, buffer_size, chunks, _records


def data_orders(thread_id, output, start_index, end_index, buffer_size):
    (data_task, buffer_limit, chunks, _records) = gen_task(start_index, end_index, buffer_size)
    orders_fd = open(f"{output}/orders/orders_{thread_id}.csv", 'w')
    for index in trange(chunks, desc=f"Orders-{thread_id}"):
        data_task.orders(start_index + index, _records, buffer_limit, orders_fd)
    orders_fd.close()


def data_profiles(thread_id, output, start_index, end_index, buffer_size):
    (data_task, buffer_limit, chunks, _records) = gen_task(start_index, end_index, buffer_size)
    profiles_fd = open(f"{output}/profiles/profiles_{thread_id}.csv", 'w')
    for index in trange(chunks, desc=f"Profiles-{thread_id}"):
        data_task.profiles(start_index + index, _records, buffer_limit * 2, profiles_fd)
    profiles_fd.close()


def main():
    _parser = ArgumentParser()
    _parser.add_argument('-i', '--records', dest='records', default=DEFAULT_RECORDS, type=int, required=False, help='')
    _parser.add_argument('-o', '--output', dest='output', default='./data_test', required=False, help='')
    _parser.add_argument('-t', '--max_threads', dest='max_threads', default=MAX_THREADS,
                         type=int, required=False, help='')
    _parser.add_argument('-m', '--max_records_per_thread', dest='max_records_per_thread',
                         default=MAX_RECORDS_PER_THREAD, type=int, required=False, help='')
    _parser.add_argument('-b', '--buffer_size', dest='buffer_size',
                         default=BUFFER_LIMIT, type=int, required=False, help='')
    _parser.add_argument('-s', '--default_start_index', dest='default_start_index',
                         default=DEFAULT_START_INDEX, type=int, required=False, help='')
    parse_input = _parser.parse_args()
    total_records = 10 if parse_input.records is None else int(parse_input.records)
    _output = parse_input.output
    if not os.path.exists(_output):
        os.makedirs(_output)
    if not os.path.exists(f"{_output}/orders"):
        os.makedirs(f"{_output}/orders")
    if not os.path.exists(f"{_output}/profiles"):
        os.makedirs(f"{_output}/profiles")

    buffer_size = parse_input.buffer_size
    max_threads = parse_input.max_threads
    max_records_per_thread = parse_input.max_records_per_thread
    data_threads = MIN_THREADS
    if total_records > max_records_per_thread:
        data_threads = int(total_records / max_records_per_thread)

    records_per_thread = int(total_records / data_threads)
    gen_threads = []
    print(f"data_threads={data_threads} , records per thread = {records_per_thread}")

    for i in range(0, data_threads):
        _start_index = i * records_per_thread + parse_input.default_start_index
        _end_index = _start_index + records_per_thread
        profiles_thread = Thread(target=data_profiles, args=[i, _output, _start_index, _end_index, buffer_size],
                                 daemon=True)
        gen_threads.append(profiles_thread)

    for i in range(0, data_threads):
        _start_index = i * records_per_thread
        _end_index = _start_index + records_per_thread
        orders_thread = Thread(target=data_orders, args=[i, _output, _start_index, _end_index, buffer_size],
                               daemon=True)
        gen_threads.append(orders_thread)

    count = 0
    watch_thread = []
    while True:
        while len(gen_threads) > 0 and (count < max_threads):
            dt = gen_threads.pop(0)
            dt.start()
            count = count + 1
            watch_thread.append(dt)

        while len(watch_thread) > 0:
            dt = watch_thread.pop(0)
            if dt.is_alive():
                watch_thread.append(dt)
                time.sleep(1)
            else:
                dt.join()
                count = count - 1
                break

        if len(gen_threads) == 0 and len(watch_thread) == 0:
            break


if __name__ == '__main__':
    main()
