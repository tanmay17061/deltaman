from typing import Union, Dict, List, Tuple
import json
from src.deltaman.value import JSONValue

class JSONSampleCollection:
    def __init__(self, raw_sample_l: List[Tuple[str,str]], max_depth: int):

        self.sample_collection = {}
        for sample_id, sample_payload in raw_sample_l:
            self.sample_collection[sample_id] = JSONSample.parse_str_payload(sample_id=sample_id, payload=sample_payload, max_depth=max_depth)

    @staticmethod
    def _initialize_aggregate_scalar_metrics():
        pass


class JSONSample:
    def __init__(self, sample_id, values):

        self.sample_id = sample_id
        self.values = values
    @staticmethod
    def parse_str_payload(sample_id: str, payload: str, max_depth: int):

        j = json.loads(payload)
        value_l = JSONValue._digest_raw_value(raw_value=j, value_path='root', value_level=0, max_depth=max_depth)
        return JSONSample(sample_id=sample_id, values=value_l)
