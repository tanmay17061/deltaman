from typing import Union, Dict, List
import json

class JSONSample:
    def __init__(self, digests):
        self.digests = digests

    @staticmethod
    def parse_str_payload(payload: str, max_depth: int):

        j = json.loads(payload)
        all_value_digests = JSONValue._digest_raw_value(value=j, value_path='', value_level=0, max_depth=max_depth)
        return JSONSample(digests=values_digest_l)
