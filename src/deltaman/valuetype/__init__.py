from src.deltaman.valuetype.valuetype import *
from src.deltaman.valuetype.jsonvaluetype import *

type_str_to_valuetype_mapper = {
    "str": JSONString,
    "int": JSONNumerical,
    "float": JSONNumerical,
    "dict": JSONDict,
    "list": JSONArray,
    "bool": JSONBool,
    "NoneType": JSONNull,
}