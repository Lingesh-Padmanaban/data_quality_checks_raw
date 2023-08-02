import json
class mapper_select():
    def __init__(self, feed_name):
        self.feed_name = feed_name

    def read_json(self, input_path):
        obj =  json.load(open(input_path, "r"))
        return obj

    def return_mapper(self):
        if self.feed_name=="carefirst":
            obj = self.read_json("carefirst_mapper.json")
            return obj