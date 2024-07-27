type_str_to_valuetype_mapper = {
    "str": JSONString,
    "int": JSONNumerical,
    "float": JSONNumerical,
    "dict": JSONDict,
    "list": JsonArray,
    "bool": JSONBool,
}

class ValueType:
    def __init__(self):
        pass

    def generate_base_scalars(self):
        pass
    @staticmethod
    def _get_nested_values_for_type(raw_value):
        '''
            Return an iterable
        '''
        raise NotImplementedError()
