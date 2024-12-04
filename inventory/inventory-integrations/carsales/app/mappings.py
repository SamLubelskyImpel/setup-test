from json import dumps


def get_nested_value(data: dict, key_path: str, default_value = None):
    '''Accesses a nested value in a dictionary using dot notation, supporting list indices.'''
    keys = key_path.split('.')
    value = data
    for key in keys:
        if isinstance(value, dict):
            value = value.get(key, None)  # Get the value for the current key
        elif isinstance(value, list):
            try:
                index = int(key)  # Try to interpret the key as a list index
                value = value[index]
            except (ValueError, IndexError):
                return default_value  # Return default if index is invalid
        else:
            return default_value  # Return default if value is neither dict nor list
    return value


def get_from_list(e: dict, field: str, value_field: str, comparison_field: str, comparison_value: str):
    return next(iter([v.get(value_field) for v in get_nested_value(e, field, []) if v.get(comparison_field) == comparison_value]), None)


FIELD_MAPPINGS = {
    'inv_vehicle': {
        'vin': lambda e: get_from_list(e, 'Identification', 'Value', 'Type', 'VIN'),
        'oem_name': 'Specification.Make',
        'type': lambda e: get_from_list(e, 'Specification.Attributes', 'Value', 'Name', 'BodyStyle'),
        'mileage': lambda e: str(get_nested_value(e, 'OdometerReadings.-1.Value', '')),
        'make': 'Specification.Make',
        'model': 'Specification.Model',
        'year': 'Specification.ReleaseDate.Year',
        'new_or_used': 'ListingType',
        'stock_num': lambda e: get_from_list(e, 'Identification', 'Value', 'Type', 'StockNumber'),
        'metadata': lambda e: dumps({
            'odometer_units': get_nested_value(e, 'OdometerReadings.-1.UnitOfMeasure'),
            'source': 'Redbook',
            'source_id': get_nested_value(e, 'Specification.SpecificationCode')
        })
    },
    'inv_dealer_integration_partner': {
        'provider_dealer_id': 'Seller.Identifier',
    },
    'inv_inventory': {
        'list_price': lambda e: get_from_list(e, 'PriceList', 'Amount', 'Type', 'DAP'),
        'fuel_type': lambda e: get_from_list(e, 'Specification.Attributes', 'Value', 'Name', 'FuelType'),
        'exterior_color': lambda e: get_from_list(e, 'Colours', 'Name', 'Location', 'Exterior'),
        'interior_color': lambda e: get_from_list(e, 'Colours', 'Name', 'Location', 'Interior'),
        'doors': lambda e: get_from_list(e, 'Specification.Attributes', 'Value', 'Name', 'Doors'),
        'seats': lambda e: get_from_list(e, 'Specification.Attributes', 'Value', 'Name', 'Seats'),
        'transmission': lambda e: get_from_list(e, 'Specification.Attributes', 'Value', 'Name', 'Transmission'),
        'photo_url': 'Media.Photos.0.Url',
        'comments': 'Description',
        'drive_train': lambda e: get_from_list(e, 'Specification.Attributes', 'Value', 'Name', 'Drive'),
        'cylinders': lambda e: get_from_list(e, 'Specification.Attributes', 'Value', 'Name', 'Cylinders'),
        'body_style': lambda e: get_from_list(e, 'Specification.Attributes', 'Value', 'Name', 'BodyStyle'),
        'series': 'Specification.Series',
        'on_lot': lambda e: get_nested_value(e, 'SaleStatus') in ['For Sale', 'Withdrawn'],
        'vin': lambda e: get_from_list(e, 'Identification', 'Value', 'Type', 'VIN'),
        'region': lambda _: 'AU',
        'trim': lambda e: get_from_list(e, 'Specification.Attributes', 'Value', 'Name', 'Badge'),
        'special_price': lambda e: get_from_list(e, 'PriceList', 'Amount', 'Type', 'EGC'),
        'engine': lambda e: get_from_list(e, 'Specification.Attributes', 'Value', 'Name', 'EngineType'),
        'engine_displacement': lambda e: get_from_list(e, 'Specification.Attributes', 'Value', 'Name', 'EngineSize'),
        'factory_certified': lambda e: len(get_nested_value(e, 'Certifications', [])) > 0,
        'options': 'options'
    },
}
