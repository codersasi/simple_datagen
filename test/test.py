from argparse import ArgumentParser
from multiprocessing import Process, Pool
from faker import Faker
from tqdm import trange, tqdm
import os
from time import time, gmtime, strftime
import logging


class Constants:
    DEFAULT_START_INDEX = 0
    BUFFER_LIMIT = 100000
    MAX_RECORDS_PER_THREAD = 1000000
    DEFAULT_RECORDS = 10000000
    MIN_THREADS = 2
    MAX_THREADS = 4
    OUTPUT_DIR = './data_10M'
    LOG_FILE_PATH = './app.log'
    LOG_FORMAT = '%(asctime)s - %(processName)s - %(message)s'


class DataTask(Process):

    def __init__(self, thread_id, start_index, end_index, buffer_size, total_records, output, logger):
        super(Process, self).__init__(daemon=True)
        self.start_index = start_index
        self.end_index = end_index
        self.buffer_size = buffer_size
        self.total_records = total_records
        self.output = output
        self.thread_id = thread_id
        self.logger = logger
        Faker.seed(total_records)
        self.faker = Faker()


class DataOrders(DataTask):
    def __init__(self, thread_id, start_index, end_index, buffer_size, total_records, output, logger):
        super(DataOrders, self).__init__(thread_id, start_index, end_index, buffer_size, total_records, output, logger)

    def run(self) -> None:
        (buffer_size, start_index, end_index, total_records, output) = self.buffer_size, \
                                                                       self.start_index, \
                                                                       self.end_index, \
                                                                       self.total_records, \
                                                                       self.output

        orders_fd = open(f"{output}/orders/orders_{self.thread_id}.csv", 'w')
        orders_fd.write("id,user_id,name,amount,quantity,geo_location,active,mfd_date,sold_time\n")
        faker = self.faker
        logger = self.logger
        orders = []
        start_time = time()
        for order_id in trange(start_index + 1, end_index + 1, desc=f"Orders {self.thread_id}:"):
            mfd_date = faker.date_time()
            sold_date = faker.date_time_ad(start_datetime=mfd_date)
            order_record = \
                f"{order_id},{faker.pyint(min_value=1, max_value=total_records)}," \
                f"{faker.name()}," \
                f"{faker.pyfloat(left_digits=3, right_digits=10, positive=True, min_value=1, max_value=100)}," \
                f"{faker.pyint(min_value=1, max_value=100)},{faker.coordinate()},{faker.pybool()}," \
                f"{mfd_date.date().isoformat()},{sold_date.isoformat()}\n"
            orders.append(order_record)
            if order_id % buffer_size == 0:
                orders_fd.writelines(orders)
                orders_fd.flush()
                orders.clear()
                elapsed = strftime("%H:%M:%S", gmtime(time() - start_time))
                logger.info(f"Orders Batch Time: {elapsed}")
                start_time = time()
        if len(orders) > 0:
            orders_fd.writelines(orders)
            orders_fd.flush()
            orders.clear()
        orders_fd.close()


class DataProfiles(DataTask):
    def __init__(self, thread_id, start_index, end_index, buffer_size, total_records, output, logger):
        super(DataProfiles, self).__init__(thread_id, start_index, end_index,
                                           buffer_size, total_records, output, logger)

    def run(self) -> None:
        (buffer_size, start_index, end_index, total_records, output) = self.buffer_size, \
                                                                       self.start_index, \
                                                                       self.end_index, \
                                                                       self.total_records, \
                                                                       self.output
        profiles_fd = open(f"{output}/profiles/profiles_{self.thread_id}.csv", 'w')
        profiles_fd.write("id,username,name,gender,mail,dob\n")
        faker = self.faker
        logger = self.logger
        profiles = []
        start_time = time()
        for profile_id in trange(start_index + 1, end_index + 1, desc=f"Profiles {self.thread_id}:"):
            name = faker.name()
            user_name = faker.user_name()
            gender = faker.random_choices(elements=('M', 'F'))[0]
            email = faker.email()
            dob = faker.date_time().date().isoformat()

            profile_record = f"{profile_id}," \
                             f"{user_name}," \
                             f"{name},{gender},{email},{dob}\n"
            profiles.append(profile_record)
            if profile_id % buffer_size == 0:
                profiles_fd.writelines(profiles)
                profiles_fd.flush()
                profiles.clear()
                elapsed = strftime("%H:%M:%S", gmtime(time() - start_time))
                logger.info(f"Profiles Batch Time: {elapsed}")
                start_time = time()
        if len(profiles) > 0:
            profiles_fd.writelines(profiles)
            profiles_fd.flush()
            profiles.clear()
        profiles_fd.close()


class MyInput(object):

    @staticmethod
    def parse():
        _parser = ArgumentParser()
        _parser.add_argument('-i', '--records', dest='total_records', default=Constants.DEFAULT_RECORDS, type=int,
                             required=False,
                             help='')
        _parser.add_argument('-o', '--output', dest='output', default=Constants.OUTPUT_DIR, required=False, help='')
        _parser.add_argument('-t', '--max_threads', dest='max_threads', default=Constants.MAX_THREADS,
                             type=int, required=False, help='')
        _parser.add_argument('-m', '--max_records_per_thread', dest='max_records_per_thread',
                             default=Constants.MAX_RECORDS_PER_THREAD, type=int, required=False, help='')
        _parser.add_argument('-b', '--buffer_size', dest='buffer_size',
                             default=Constants.BUFFER_LIMIT, type=int, required=False, help='')
        _parser.add_argument('-s', '--default_start_index', dest='default_start_index',
                             default=Constants.DEFAULT_START_INDEX, type=int, required=False, help='')
        return _parser.parse_args()


def execute_chunks(gen_threads, max_threads, logger):
    count = 0
    watch_thread = []
    complete_threads = []

    while True:
        while len(gen_threads) > 0 and (count < max_threads):
            dt = gen_threads.pop(0)
            dt.start()
            count = count + 1
            watch_thread.append(dt)
        logger.info(f"Threads running : {len(watch_thread)}")
        while len(watch_thread) > 0:
            dt = watch_thread.pop(0)
            if dt.is_alive():
                watch_thread.append(dt)
            else:
                complete_threads.append(dt)
                count = count - 1
                break

        if len(gen_threads) == 0 and len(watch_thread) == 0:
            logger.info("Completed all threads, completing the main")
            while len(complete_threads) > 0:
                dt = complete_threads.pop(0)
                dt.join()
            break


def main():
    parse_input = MyInput.parse()

    _output = parse_input.output
    if not os.path.exists(_output):
        os.makedirs(_output)
    if not os.path.exists(f"{_output}/orders"):
        os.makedirs(f"{_output}/orders")
    if not os.path.exists(f"{_output}/profiles"):
        os.makedirs(f"{_output}/profiles")

    _output = parse_input.output
    logging.basicConfig(filename=Constants.LOG_FILE_PATH, level=logging.INFO,
                        format=Constants.LOG_FORMAT,
                        filemode='w')
    logger = logging.getLogger()
    buffer_size = parse_input.buffer_size
    max_threads = parse_input.max_threads
    max_records_per_thread = parse_input.max_records_per_thread
    data_threads = Constants.MIN_THREADS
    total_records = parse_input.total_records
    if total_records > max_records_per_thread:
        data_threads = int(total_records / max_records_per_thread)

    records_per_thread = int(total_records / data_threads)
    gen_threads = []

    logger.info(f"data_threads={data_threads} , records/thread = {records_per_thread}")

    default_start_index = parse_input.default_start_index
    for thread_id in range(0, data_threads):
        _start_index = (thread_id * records_per_thread) + default_start_index
        _end_index = _start_index + records_per_thread
        profiles_thread = DataProfiles(thread_id, _start_index, _end_index, buffer_size, total_records, _output, logger)
        gen_threads.append(profiles_thread)
        orders_thread = DataOrders(thread_id, _start_index, _end_index, buffer_size, total_records, _output, logger)
        gen_threads.append(orders_thread)

    start_time = time()
    execute_chunks(gen_threads, max_threads, logger)
    elapsed = strftime("%H:%M:%S", gmtime(time() - start_time))
    logger.info(f"Took: {elapsed}")


if __name__ == '__main__':
    main()
