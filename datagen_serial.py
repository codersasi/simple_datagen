import time

from faker import Faker
import argparse
from threading import Thread
import progressbar


class Data(object):
    def __init__(self, records, output):
        self.records = records
        output = output if output is not None else '.'
        self.profiles = open(f"{output}/profiles.csv", 'w')
        self.orders = open(f"{output}/orders.csv", 'w')
        self.fake = Faker()
        self.profiles.write("id,username,name,gender,mail,dob\n")
        self.orders.write("id,user_id,name,amount,quantity,geo_location,active,mfd_date,sold_time\n")
        Faker.seed(records)

    def gen_profiles(self, index):
        _profile_record = self.profile_record(index)
        print(','.join([str(i) for i in _profile_record]), file=self.profiles)
        if index % 10000 == 0:
            self.profiles.flush()

    def gen_orders(self, index):
        _order_record = self.sales_record(index)
        print(','.join([str(i) for i in _order_record]), file=self.orders)
        if index % 10000 == 0:
            self.orders.flush()

    def generate(self):
        widgets = [' [', progressbar.Timer(format='elapsed time: %(elapsed)s'), ']', progressbar.Bar('*'), ' (',
                   progressbar.Counter(), ') ', ]
        bar = progressbar.ProgressBar(max_value=self.records, widgets=widgets).start()
        for index in range(1, self.records + 1):
            self.gen_profiles(index)
            self.gen_orders(index)
            bar.update(index)
        self.profiles.close()
        self.orders.close()

    def sales_record(self, index):
        fake = self.fake
        mfd_date = fake.date_time()
        sold_date = fake.date_time_ad(start_datetime=mfd_date)
        return (
            index,
            fake.pyint(min_value=1, max_value=self.records),
            fake.name(),
            fake.pyfloat(left_digits=3, right_digits=10, positive=True, min_value=1, max_value=100),
            fake.pyint(min_value=1, max_value=100),
            fake.coordinate(),
            fake.pybool(),
            mfd_date.date().isoformat(),
            sold_date.isoformat()
        )

    def profile_record(self, index):
        fake = self.fake
        profile = fake.simple_profile()
        return (
            index,
            profile['username'],
            profile['name'],
            profile['sex'],
            profile['mail'],
            profile['birthdate']
        )


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    _parser = argparse.ArgumentParser()
    _parser.add_argument('-i', '--records', dest='records', default=10, type=int, required=False, help='')
    _parser.add_argument('-o', '--output', dest='output', required=False, help='')
    parse_input = _parser.parse_args()
    _records = 10 if parse_input.records is None else int(parse_input.records)
    _output = parse_input.output
    data = Data(_records, _output)
    data.generate()
