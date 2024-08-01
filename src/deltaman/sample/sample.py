from typing import Union, Dict, List, Tuple
import json
from src.deltaman.value import JSONValue
import pandas as pd
import glob
import os
import math

class JSONSampleCollection:
    def __init__(self, raw_sample_l: List[Tuple[str,str]], max_depth: int):

        self.sample_collection = {}
        for sample_id, sample_payload in raw_sample_l:
            self.sample_collection[sample_id] = JSONSample.parse_str_payload(sample_id=sample_id, payload=sample_payload, max_depth=max_depth)

        self.initialize_path_aggregate_scalar_metrics()

    @staticmethod
    def from_directory(directory_path: str, max_depth: int):
        filename_l = glob.glob(os.path.join(directory_path,"*"))
        raw_sample_l = []
        for filename in filename_l:
            with open(filename, "rt") as f:
                filecontents = f.read()
            raw_sample_l.append((filename, filecontents))
        return JSONSampleCollection(raw_sample_l=raw_sample_l, max_depth=max_depth)


    @staticmethod
    def extract_path_aggregate_metrics_from_path_collected_rows(rows):

        ret_path_aggregate_value_metrics = {}
        ret_path_aggregate_value_metrics["total_samples"] = rows.shape[0]
        ret_path_aggregate_value_metrics["is_present_count"] = rows.is_present.sum()
        ret_path_aggregate_value_metrics["is_filled_count"] = rows.is_filled.sum()

        value_type_counts = rows.value_type_str.value_counts()

        value_type_counts_dict = value_type_counts.to_dict()
        ret_path_aggregate_value_metrics["value_type_counts"] = value_type_counts_dict

        if len(value_type_counts_dict) > 1:
            # For now, 1 value_path can only consist of 1 value_type_str for aggregation to work.
            ret_path_aggregate_value_metrics["path_aggregate_value_metrics_extraction_success"] = False
            return ret_path_aggregate_value_metrics
        else:
            ret_path_aggregate_value_metrics["path_aggregate_value_metrics_extraction_success"] = True

        if 'dict' in value_type_counts_dict.keys() or 'list' in value_type_counts_dict.keys():
            ret_path_aggregate_value_metrics["mean_num_items"] = rows.num_items.mean()
            ret_path_aggregate_value_metrics["median_num_items"] = rows.num_items.median()
            ret_path_aggregate_value_metrics["std_num_items"] = rows.num_items.std()

        if 'int' in value_type_counts_dict.keys() or 'float' in value_type_counts_dict.keys():
            ret_path_aggregate_value_metrics["mean_value"] = rows.raw_value.mean()
            ret_path_aggregate_value_metrics["median_value"] = rows.raw_value.median()
            ret_path_aggregate_value_metrics["std_value"] = rows.raw_value.std()
        
        if 'bool' in value_type_counts_dict.keys():
            ret_path_aggregate_value_metrics["value_true_count"] = rows.raw_value.astype(int).sum()
            ret_path_aggregate_value_metrics["value_false_count"] = rows.raw_value.shape[0] - rows.raw_value.astype(int).sum()

        if 'str' in value_type_counts_dict.keys():
            ret_path_aggregate_value_metrics["mean_length"] = rows.length.mean()
            ret_path_aggregate_value_metrics["median_length"] = rows.length.median()
            ret_path_aggregate_value_metrics["std_length"] = rows.length.std()
            ret_path_aggregate_value_metrics["can_be_numeric_count"] = rows.can_be_numeric.astype(int).sum()
            ret_path_aggregate_value_metrics["can_not_be_numeric_count"] = rows.can_be_numeric.shape[0] - rows.can_be_numeric.astype(int).sum()

        # print(f"{type(ret_path_aggregate_value_metrics)=}")
        return ret_path_aggregate_value_metrics

    def initialize_path_aggregate_scalar_metrics(self):
        '''
            Compute metrics aggregated on `value_path` across samples
        '''

        path_collected_metrics_series_l = []

        for sample_id, sample in self.sample_collection.items():
            path_collected_metrics_series_l.extend(sample.flatten_to_list())

        path_collected_metrics_df = pd.DataFrame(path_collected_metrics_series_l)
        del path_collected_metrics_series_l
        self.path_collected_metrics_df = path_collected_metrics_df
        self.path_aggregate_metrics = path_collected_metrics_df.groupby("value_path").apply(JSONSampleCollection.extract_path_aggregate_metrics_from_path_collected_rows)

    def get_path_aggregate_scalar_metrics(self):
        print(f"{type(self.path_aggregate_metrics)=}")
        return self.path_aggregate_metrics.to_dict()
        # return self.path_aggregate_metrics.to_dict(orient='index')
    
    def diff(self, sc_other):

        self_scalar_metrics = self.get_path_aggregate_scalar_metrics()
        other_scalar_metrics = sc_other.get_path_aggregate_scalar_metrics()
        self_scalar_metrics_sample = JSONSample.parse_dict_payload(sample_id="self_scalar_metrics", payload=self_scalar_metrics, max_depth=10)
        other_scalar_metrics_sample = JSONSample.parse_dict_payload(sample_id="other_scalar_metrics", payload=other_scalar_metrics, max_depth=10)
        return self_scalar_metrics_sample.diff(other_scalar_metrics_sample)


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
        diff_k = {}
        for i in range(common_flat_joined_df.shape[0]):
            common_flat_joined_row = common_flat_joined_df.iloc[i]
            value_path_row = common_flat_joined_df.index[i]
            raw_value_diff = common_flat_joined_row["value_object_l"].diff_of_raw_value(common_flat_joined_row["value_object_r"])
            diff_k[value_path_row] = raw_value_diff
        return diff_k


        # self_keys = set(self_flat_df.index)
        # other_keys = set(other_flat_df.index)

        # positive_diff_keys = self_keys - other_keys
        # negative_diff_keys = other_keys - self_keys
        # common_keys = self_keys.intersection(other_keys)

        # positive_diff = {k:self_scalar_metrics[k] for k in positive_diff_keys}
        # negative_diff = {k:other_scalar_metrics[k] for k in negative_diff_keys}
        # common_diff = {k: JSONValue.compare_diff_of_raw_values(self_scalar_metrics[k], other_scalar_metrics[k]) for k in common_keys}

    @staticmethod
    def parse_dict_payload(sample_id: str, payload: dict, max_depth: int):

        value_l = JSONValue._digest_raw_value(raw_value=payload, value_path='root', value_level=0, max_depth=max_depth)
        return JSONSample(sample_id=sample_id, values=value_l)

    @staticmethod
    def parse_str_payload(sample_id: str, payload: str, max_depth: int):

        j = json.loads(payload)
        return __class__.parse_dict_payload(sample_id=sample_id, payload=j, max_depth=max_depth)
