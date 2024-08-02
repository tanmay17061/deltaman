import os
from typing import List, Tuple
import glob
import pandas as pd
from deltaman.sample import JSONSample
import json

class JSONSampleCollection:
    def __init__(self, raw_sample_l: List[Tuple[str,str]], max_depth: int):

        self.sample_collection = {}
        for sample_id, sample_payload in raw_sample_l:
            self.sample_collection[sample_id] = JSONSample.parse_str_payload(sample_id=sample_id, payload=sample_payload, max_depth=max_depth)

        self.initialize_path_aggregate_scalar_metrics()

    @staticmethod
    def from_directory(directory_path: str, max_depth: int = 10):
        filename_l = glob.glob(os.path.join(directory_path,"*"))
        assert filename_l, "directory for initialising sample collection: {directory_path} is empty or does not exist."
        raw_sample_l = []
        for filename in filename_l:
            with open(filename, "rt") as f:
                filecontents = f.read()
            raw_sample_l.append((filename, filecontents))
        return JSONSampleCollection(raw_sample_l=raw_sample_l, max_depth=max_depth)


    @staticmethod
    def extract_path_aggregate_metrics_from_path_collected_rows(rows):

        ret_path_aggregate_value_metrics = {}
        ret_path_aggregate_value_metrics["total_samples"] = float(rows.shape[0])
        ret_path_aggregate_value_metrics["is_present_count"] = float(rows.is_present.sum())
        ret_path_aggregate_value_metrics["is_filled_count"] = float(rows.is_filled.sum())

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
            ret_path_aggregate_value_metrics["mean_num_items"] = float(rows.num_items.mean())
            ret_path_aggregate_value_metrics["median_num_items"] = float(rows.num_items.median())
            ret_path_aggregate_value_metrics["std_num_items"] = float(rows.num_items.std())

        if 'int' in value_type_counts_dict.keys() or 'float' in value_type_counts_dict.keys():
            ret_path_aggregate_value_metrics["mean_value"] = float(rows.raw_value.mean())
            ret_path_aggregate_value_metrics["median_value"] = float(rows.raw_value.median())
            ret_path_aggregate_value_metrics["std_value"] = float(rows.raw_value.std())
        
        if 'bool' in value_type_counts_dict.keys():
            ret_path_aggregate_value_metrics["value_true_count"] = float(rows.raw_value.astype(int).sum())
            ret_path_aggregate_value_metrics["value_false_count"] = float(rows.raw_value.shape[0] - rows.raw_value.astype(int).sum())

        if 'str' in value_type_counts_dict.keys():
            ret_path_aggregate_value_metrics["mean_length"] = float(rows.length.mean())
            ret_path_aggregate_value_metrics["median_length"] = float(rows.length.median())
            ret_path_aggregate_value_metrics["std_length"] = float(rows.length.std())
            ret_path_aggregate_value_metrics["can_be_numeric_count"] = float(rows.can_be_numeric.astype(int).sum())
            ret_path_aggregate_value_metrics["can_not_be_numeric_count"] = float(rows.can_be_numeric.shape[0] - rows.can_be_numeric.astype(int).sum())

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
        return self.path_aggregate_metrics.to_dict()
    
    def diff(self, sc_other):

        self_scalar_metrics = self.get_path_aggregate_scalar_metrics()
        other_scalar_metrics = sc_other.get_path_aggregate_scalar_metrics()
        self_scalar_metrics_sample = JSONSample.parse_dict_payload(sample_id="self_scalar_metrics", payload=self_scalar_metrics, max_depth=10, root_path='')
        other_scalar_metrics_sample = JSONSample.parse_dict_payload(sample_id="other_scalar_metrics", payload=other_scalar_metrics, max_depth=10, root_path='')
        return json.dumps(self_scalar_metrics_sample.diff(other_scalar_metrics_sample))

