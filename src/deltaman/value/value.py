from deltaman.valuetype import ValueType, type_str_to_valuetype_mapper
from typing import Union, Dict, List


class JSONValue:
    def __init__(self, raw_value, value_level, value_path, value_type_str):
        self.raw_value = raw_value
        self.value_level = value_level
        self.value_path = value_path
        self.value_type_str = value_type_str
        self.value_type = type_str_to_valuetype_mapper[value_type_str]

        self._initialize_scalar_metrics()

    def _initialize_scalar_metrics(self):
        self.metrics = {}
        self.metrics.update(self.value_type.generate_base_scalar_metrics(self.raw_value))
        self.metrics.update(self.value_type.generate_type_scalar_metrics(self.raw_value))

    def _get_nested_values(self):
        return self.value_type.get_nested_values_for_type(self.raw_value)

    @staticmethod
    def _get_value_type_str(value: Union[Dict, List]):
        type_name = type(value).__name__
        if type_name in ['int', 'float']: type_name = 'numerical'
        return type_name

    def diff_of_raw_value(self, v_other):

        if self.value_type_str != v_other.value_type_str:
            return f"value type mismatch: ({self.value_type_str} != {v_other.value_type_str}) for raw values: ({self.raw_value} != {v_other.raw_value})"
        else:
            return self.value_type.diff_of_raw_values(self.raw_value, v_other.raw_value)

    @staticmethod
    def _digest_raw_value(raw_value, value_path: str, value_level: int, max_depth: int = 3):

        value_path = str(value_path)
        all_values = []
        cur_value = JSONValue(**{
                "raw_value": raw_value,
                "value_level": value_level,
                "value_path": value_path,
                "value_type_str": JSONValue._get_value_type_str(raw_value),
                # "is_present": True,
                # "is_filled": JSONValue._get_value_is_filled(value)
            }
        )
        all_values = [(value_path,cur_value,),]
        if value_level >= max_depth: return all_values

        for nested_value_path,nested_raw_value in cur_value._get_nested_values():

            nested_value_path = str(nested_value_path)
            if value_path:
                nested_value_path = value_path + "." + nested_value_path

            all_values.extend(JSONValue._digest_raw_value(raw_value=nested_raw_value, value_path=nested_value_path, value_level=value_level+1, max_depth=max_depth))
        return all_values