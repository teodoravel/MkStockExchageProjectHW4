# Homework4/filter_service/base_filter.py

"""
base_filter.py

Defines the BaseFilter class using the Template Method pattern.
Each concrete filter (Filter1, Filter2, Filter3) inherits from this class
and overrides the needed methods.
"""

import abc

class BaseFilter(metaclass=abc.ABCMeta):
    def run(self):
        self.setup()
        raw_data = self.scrape_data()
        parsed_data = self.parse_data(raw_data)
        self.save_data(parsed_data)
        self.call_next_filter()

    def setup(self):
        pass

    @abc.abstractmethod
    def scrape_data(self):
        raise NotImplementedError

    @abc.abstractmethod
    def parse_data(self, raw_data):
        raise NotImplementedError

    @abc.abstractmethod
    def save_data(self, parsed_data):
        raise NotImplementedError

    def call_next_filter(self):
        pass
