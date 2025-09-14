from datetime import date
from typing import cast

from .result import MoexTableResult

PropertyValue = str | int | float | bool | date


def to_properties(table: MoexTableResult) -> dict[str, PropertyValue]:
    property_name_index = table.get_column_index('name')
    property_value_index = table.get_column_index('value')
    property_type_index = table.get_column_index('type')
    property_precision_index = table.get_column_index('precision')

    properties: dict[str, PropertyValue] = {}

    for row in table.get_rows():
        property_name = cast(str, row[property_name_index])
        property_value = cast(str, row[property_value_index])
        property_type = cast(str, row[property_type_index])
        property_precision = row[property_precision_index]

        if property_type == 'string':
            pass
        elif property_type == 'number':
            if property_precision == 0:
                property_value = int(property_value)
            else:
                property_value = float(property_value)
        elif property_type == 'boolean':
            property_value = property_value == '1'
        elif property_type == 'date':
            property_value = date.fromisoformat(property_value)
        else:
            raise ValueError(f"property '{property_name}' has unknown type '{property_type}'")

        properties[property_name] = property_value

    return properties
