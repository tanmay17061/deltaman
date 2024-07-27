from src.deltaman.valuetype import ValueType, type_str_to_valuetype_mapper

class JSONValue:
    def __init__(self, raw_value, value_level, value_path, value_type):
        self.raw_value = raw_value
        self.value_level = value_level
        self.value_path = value_path
        self.value_type_str = value_type
        self.value_type = type_str_to_valuetype_mapper[value_type]

    def _get_nested_values(self):
        return self.value_type._get_nested_values_for_type(self.raw_value)

    @staticmethod
    def _get_value_type_str(value: Union[Dict, List]):
        return type(value).__name__

    @staticmethod
    def _digest_raw_value(raw_value, value_path: str, value_level: int, max_depth: int = 3):
        all_values = []
        cur_value = JSONValue(**{
                "raw_value": raw_value,
                "value_level": value_level,
                "value_path": value_path,
                "value_type": JSONValue._get_value_type_str(value),
                # "is_present": true,
                # "is_filled": JSONValue._get_value_is_filled(value)
            }
        )
        all_values = [cur_value]
        if value_level >= max_depth: return all_values

        for nested_value_path,nested_raw_value in cur_value._get_nested_values():
            nested_value_path = value_path + "." + nested_value_path
            all_values.extend(JSONValue._digest_raw_value(raw_value=nested_raw_value, value_path=nested_value_path, value_level=value_level+1, max_depth=max_depth))
        return all_values