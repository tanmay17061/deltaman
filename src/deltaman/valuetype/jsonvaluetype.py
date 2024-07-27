from src.deltaman.valuetype.valuetype import ValueType


class JSONArray(ValueType):
    def __init__(self):
        pass

    @staticmethod
    def _get_nested_values_for_type(raw_value):
        '''
            For now, we are not treating arrays as nested. Hence, returning an empty iterable object.
        '''
        return iter(())

class JSONDict(ValueType):
    def __init__(self):
        pass

    @staticmethod
    def _get_nested_values_for_type(raw_value):
        '''
        '''
        return raw_value.items()

class JSONBool(ValueType):
    def __init__(self):
        pass

    @staticmethod
    def _get_nested_values_for_type(raw_value):
        '''
        '''
        return iter(())

class JSONNumerical(ValueType):
    def __init__(self):
        pass

    @staticmethod
    def _get_nested_values_for_type(raw_value):
        '''
        '''
        return iter(())

class JSONString(ValueType):
    def __init__(self):
        pass

    @staticmethod
    def _get_nested_values_for_type(raw_value):
        '''
        '''
        return iter(())