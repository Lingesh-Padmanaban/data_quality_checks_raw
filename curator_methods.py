import re
from collections import defaultdict
import datetime

class dtype_curator():
    def check_duplicate_values_in_dict(self, input_dict):
        value_dict = defaultdict(list)

        for key, value in input_dict.items():
            value_dict[value].append(key)

        # Filter out entries with only one key (no duplicates)
        duplicate_keys = {value: keys for value, keys in value_dict.items() if len(keys) > 1}

        return duplicate_keys

    def clean_string(self, input_string):
        # Remove characters other than alphabets, numbers, and spaces
        cleaned_string = re.sub('[^A-Za-z0-9\s]+', '', input_string)
        # Convert consecutive spaces into a single space
        cleaned_string = re.sub('\s+', ' ', cleaned_string).strip()
        return cleaned_string

    def get_current_date(self):
        current_date = datetime.datetime.now()
        return str(current_date)[:10]

    def level_of_coverage_standardize(self):
        rename = {"Subscriber/Spouse": "Subscriber and Spouse",
                  "Subscriber Only": "Subscriber",
                  "Family (3 or More People)": "Family",
                  "Parent/Children": "Parent and Children"}
        return rename


