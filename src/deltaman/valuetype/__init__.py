from deltaman.valuetype.valuetype import *
from deltaman.valuetype.jsonvaluetype import *

type_str_to_valuetype_mapper = {
    "str": JSONString,
    "int": JSONNumerical,
    "float": JSONNumerical,
    "numerical": JSONNumerical,
    "dict": JSONDict,
    "list": JSONArray,
    "bool": JSONBool,
    "NoneType": JSONNull,
}