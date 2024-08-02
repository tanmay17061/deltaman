from typing import Dict
import json
from src.deltaman.value import JSONValue
import pandas as pd
import os
import math


class JSONSample:
    def __init__(self, sample_id, values):

        self.sample_id = sample_id
        self.values = values

    def flatten_to_list(self, include_value_object=False):

        flat_l = []
        for value_path, value in self.values:
                flat_l.append(pd.Series({"raw_value": value.raw_value, "value_type_str": value.value_type_str, "value_path": value_path, "value_level": value.value_level, "sample_id": self.sample_id, **value.metrics}))
                if include_value_object:
                    flat_l[-1]["value_object"] = value
        return flat_l

    def diff(self, s_other):

        self_flat_l = self.flatten_to_list(include_value_object=True)
        other_flat_l = s_other.flatten_to_list(include_value_object=True)

        self_flat_df = pd.DataFrame(self_flat_l).set_index("value_path")
        other_flat_df = pd.DataFrame(other_flat_l).set_index("value_path")

        common_flat_joined_df = self_flat_df.join(other_flat_df, how="inner", lsuffix='_l', rsuffix='_r')
        common_diff = {}
        for i in range(common_flat_joined_df.shape[0]):
            common_flat_joined_row = common_flat_joined_df.iloc[i]
            value_path_row = common_flat_joined_df.index[i]
            raw_value_diff = common_flat_joined_row["value_object_l"].diff_of_raw_value(common_flat_joined_row["value_object_r"])
            common_diff[value_path_row] = raw_value_diff


        self_keys = set(self_flat_df.index)
        other_keys = set(other_flat_df.index)

        positive_diff_keys = self_keys - other_keys
        negative_diff_keys = other_keys - self_keys
        # common_keys = self_keys.intersection(other_keys)

        positive_diff = {k: "value missing in RHS JSONSample" for k in positive_diff_keys}
        negative_diff = {k: "value missing in LHS JSONSample" for k in negative_diff_keys}
        # common_diff = {k: JSONValue.compare_diff_of_raw_values(self_scalar_metrics[k], other_scalar_metrics[k]) for k in common_keys}

        ret_diff = common_diff
        ret_diff.update(positive_diff)
        ret_diff.update(negative_diff)

        return ret_diff

    @staticmethod
    def parse_dict_payload(sample_id: str, payload: Dict, max_depth: int, root_path="root"):

        value_l = JSONValue._digest_raw_value(raw_value=payload, value_path=root_path, value_level=0, max_depth=max_depth)
        return JSONSample(sample_id=sample_id, values=value_l)

    @staticmethod
    def parse_str_payload(sample_id: str, payload: str, max_depth: int):

        j = json.loads(payload)
        return __class__.parse_dict_payload(sample_id=sample_id, payload=j, max_depth=max_depth)
