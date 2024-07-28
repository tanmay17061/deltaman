from typing import Union, Dict, List, Tuple
import json
from src.deltaman.value import JSONValue
import pandas as pd


class JSONSampleCollection:
    def __init__(self, raw_sample_l: List[Tuple[str,str]], max_depth: int):

        self.sample_collection = {}
        for sample_id, sample_payload in raw_sample_l:
            self.sample_collection[sample_id] = JSONSample.parse_str_payload(sample_id=sample_id, payload=sample_payload, max_depth=max_depth)

        self.initialize_path_aggregate_scalar_metrics()

    @staticmethod
    def extract_path_aggregate_metrics_from_path_collected_rows(rows):

        ret_path_aggregate_value_metrics = {}
        ret_path_aggregate_value_metrics["total_samples"] = rows.shape[0]
        ret_path_aggregate_value_metrics["is_present_count"] = rows.is_present.sum()
        ret_path_aggregate_value_metrics["is_filled_count"] = rows.is_filled.sum()

        value_type_counts = rows.value.apply(lambda v: type(v).__name__).value_counts()

        value_type_counts_dict = value_type_counts.to_dict()
        ret_path_aggregate_value_metrics["value_type_counts"] = value_type_counts_dict

        if len(value_type_counts_dict) > 1:
            ret_path_aggregate_value_metrics["path_aggregate_value_metrics_extraction_success"] = False
            return ret_path_aggregate_value_metrics
        else:
            ret_path_aggregate_value_metrics["path_aggregate_value_metrics_extraction_success"] = True

        if 'dict' in value_type_counts_dict.keys() or 'list' in value_type_counts_dict.keys():
            ret_path_aggregate_value_metrics["mean_num_items"] = rows.num_items.mean()
            ret_path_aggregate_value_metrics["median_num_items"] = rows.num_items.median()
            ret_path_aggregate_value_metrics["std_num_items"] = rows.num_items.std()

        if 'int' in value_type_counts_dict.keys() or 'float' in value_type_counts_dict.keys():
            ret_path_aggregate_value_metrics["mean_value"] = rows.value.mean()
            ret_path_aggregate_value_metrics["median_value"] = rows.value.median()
            ret_path_aggregate_value_metrics["std_value"] = rows.value.std()
        
        if 'bool' in value_type_counts_dict.keys():
            ret_path_aggregate_value_metrics["value_true_count"] = rows.value.astype(int).sum()
            ret_path_aggregate_value_metrics["value_false_count"] = rows.value.shape[0] - rows.value.astype(int).sum()


        if 'str' in value_type_counts_dict.keys():
            ret_path_aggregate_value_metrics["mean_length"] = rows.length.mean()
            ret_path_aggregate_value_metrics["median_length"] = rows.length.median()
            ret_path_aggregate_value_metrics["std_length"] = rows.length.std()
            ret_path_aggregate_value_metrics["can_be_numeric_count"] = rows.can_be_numeric.astype(int).sum()
            ret_path_aggregate_value_metrics["can_not_be_numeric_count"] = rows.can_be_numeric.shape[0] - rows.can_be_numeric.astype(int).sum()


        return ret_path_aggregate_value_metrics

    def initialize_path_aggregate_scalar_metrics(self):
        '''
            Compute metrics aggregated on `value_path` across samples
        '''

        path_collected_metrics_series_l = []

        for sample_id, sample in self.sample_collection.items():
            for value_path, value in sample.values:
                path_collected_metrics_series_l.append(pd.Series({"value_path": value_path, "value_level": value.value_level, "sample_id": sample_id, **value.metrics}))
        path_collected_metrics_df = pd.DataFrame(path_collected_metrics_series_l)
        del path_collected_metrics_series_l
        path_collected_metrics_df = path_collected_metrics_df
        path_aggregate_metrics = path_collected_metrics_df.groupby("value_path").apply(JSONSampleCollection.extract_path_aggregate_metrics_from_path_collected_rows)
        self.path_aggregate_metrics = path_aggregate_metrics.apply(lambda l: pd.Series(l))


class JSONSample:
    def __init__(self, sample_id, values):

        self.sample_id = sample_id
        self.values = values
    @staticmethod
    def parse_str_payload(sample_id: str, payload: str, max_depth: int):

        j = json.loads(payload)
        value_l = JSONValue._digest_raw_value(raw_value=j, value_path='root', value_level=0, max_depth=max_depth)
        return JSONSample(sample_id=sample_id, values=value_l)
