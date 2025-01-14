"""
base_filter.py

Defines the BaseFilter class using the Template Method pattern.
Each concrete filter (Filter1, Filter2, Filter3) will inherit
from this class and override the methods as needed.
"""

import abc

class BaseFilter(metaclass=abc.ABCMeta):
    """
    BaseFilter defines the 'template method' run(), which outlines
    the general pipeline steps:
      1) setup()
      2) scrape_data()
      3) parse_data()
      4) save_data()
      5) call_next_filter()

    Subclasses must implement or override the abstract methods
    (scrape_data, parse_data, save_data) and can optionally
    override setup() or call_next_filter() if needed.
    """

    def run(self):
        """
        The template method. It ensures each filter follows the same
        high-level steps in the same order.
        """
        self.setup()
        raw_data = self.scrape_data()
        parsed_data = self.parse_data(raw_data)
        self.save_data(parsed_data)
        self.call_next_filter()

    def setup(self):
        """
        Hook for any pre-scraping setup. By default, does nothing.
        Subclasses can override if needed.
        """
        pass

    @abc.abstractmethod
    def scrape_data(self):
        """
        Must be overridden. Retrieves or scrapes raw data from a source
        (URL, database, file, etc.) and returns it.
        """
        raise NotImplementedError("scrape_data() must be overridden by subclass")

    @abc.abstractmethod
    def parse_data(self, raw_data):
        """
        Must be overridden. Parses or transforms raw data into a structured form.
        """
        raise NotImplementedError("parse_data() must be overridden by subclass")

    @abc.abstractmethod
    def save_data(self, parsed_data):
        """
        Must be overridden. Persists the parsed data to a DB, file, or other storage.
        """
        raise NotImplementedError("save_data() must be overridden by subclass")

    def call_next_filter(self):
        """
        Hook to optionally call the next filter. By default, does nothing.
        Subclasses can override this to chain filters.
        """
        pass
