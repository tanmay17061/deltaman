
class ValueType:
    def __init__(self):
        pass
    
    @staticmethod
    def _get_is_value_filled(raw_value):
        return raw_value not in [None,]

    @staticmethod
    def generate_base_scalar_metrics(raw_value):
        '''
            Return base scalar metrics
        '''
        ret_metrics = {
            "is_present": True,
            "is_filled": __class__._get_is_value_filled(raw_value)
        }
        return ret_metrics
    
    @staticmethod
    def generate_type_scalar_metrics(raw_value):
        '''
            Return type scalar metrics
        '''
        raise NotImplementedError()

    @staticmethod
    def get_nested_values_for_type(raw_value):
        '''
            Return an iterable
        '''
        raise NotImplementedError()
