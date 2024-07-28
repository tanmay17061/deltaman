from src.deltaman.valuetype.valuetype import ValueType


class JSONArray(ValueType):
    def __init__(self):
        pass

    @staticmethod
    def generate_type_scalar_metrics(raw_value):
        '''
            Return type scalar metrics
        '''
        if raw_value is None:
            return {
                "num_items": 0
            }

        else:
            return {
                "num_items": len(raw_value)
            }

    @staticmethod
    def get_nested_values_for_type(raw_value):
        '''
            For now, we are not treating arrays as nested. Hence, returning an empty iterable object.
        '''
        return iter(())

class JSONDict(ValueType):
    def __init__(self):
        pass

    @staticmethod
    def generate_type_scalar_metrics(raw_value):
        '''
            Return type scalar metrics
        '''
        if raw_value is None:
            return {
                "num_items": 0
            }

        else:
            return {
                "num_items": len(raw_value)
            }

    @staticmethod
    def get_nested_values_for_type(raw_value):
        '''
        '''
        return raw_value.items()

class JSONBool(ValueType):
    def __init__(self):
        pass

    @staticmethod
    def generate_type_scalar_metrics(raw_value):
        '''
            Return type scalar metrics
        '''
        if raw_value is None:
            return {
                "value": 0
            }

        else:
            return {
                "value": raw_value
            }

    @staticmethod
    def get_nested_values_for_type(raw_value):
        '''
        '''
        return iter(())

class JSONNumerical(ValueType):
    def __init__(self):
        pass

    @staticmethod
    def generate_type_scalar_metrics(raw_value):
        '''
            Return type scalar metrics
        '''
        if raw_value is None:
            return {}

        else:
            return {
                "value": raw_value
            }

    @staticmethod
    def get_nested_values_for_type(raw_value):
        '''
        '''
        return iter(())

class JSONString(ValueType):
    def __init__(self):
        pass

    @staticmethod
    def _raw_value_can_cast_to_numeric(raw_value):
        try:
            num = float(raw_value)
            return True
        except:
            return False

    @staticmethod
    def generate_type_scalar_metrics(raw_value):
        '''
            Return type scalar metrics
        '''
        if raw_value is None:
            return {}

        else:
            return {
                "value": raw_value,
                "length": len(raw_value),
                "can_be_numeric": __class__._raw_value_can_cast_to_numeric(raw_value)
            }

    @staticmethod
    def get_nested_values_for_type(raw_value):
        '''
        '''
        return iter(())