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


def get_options(vehicle: dict):
    return [
        o.get("Description")
        for o in vehicle.get("ListOfOptions", [])
        if o.get("Header") != "Priority"
    ]


def get_priority_options(vehicle: dict):
    return [
        o.get("Description")
        for o in vehicle.get("ListOfOptions", [])
        if o.get("Header") == "Priority"
    ]


FIELD_MAPPINGS = {
    'inv_vehicle': {
        'vin': lambda d: get_nested_value(d, 'VehicleInfo.VIN'),
        'oem_name': lambda d: get_nested_value(d, 'VehicleInfo.Make'),
        'type': lambda d: get_nested_value(d, 'VehicleInfo.BodyType'),
        'mileage': lambda d: str(get_nested_value(d, 'VehicleInfo.Mileage')),
        'make': lambda d: get_nested_value(d, 'VehicleInfo.Make'),
        'model': lambda d: get_nested_value(d, 'VehicleInfo.Model'),
        'year': lambda d: get_nested_value(d, 'VehicleInfo.Year'),
        'new_or_used': lambda d: 'new' if get_nested_value(d, 'VehicleInfo.IsNew') else 'used',
        'stock_num': lambda d: get_nested_value(d, 'VehicleInfo.StockNumber')
    },
    'inv_inventory': {
        'list_price': lambda d: str(get_nested_value(d, 'Pricing.List')),
        'cost_price': lambda d: str(get_nested_value(d, 'Pricing.Cost')),
        'fuel_type': lambda d: get_nested_value(d, 'VehicleInfo.EngineFuelType'),
        'exterior_color': lambda d: get_nested_value(d, 'VehicleInfo.ExteriorColor'),
        'interior_color': lambda d: get_nested_value(d, 'VehicleInfo.InteriorColor'),
        'doors': lambda d: get_nested_value(d, 'VehicleInfo.DoorCount'),
        'transmission': lambda d: get_nested_value(d, 'VehicleInfo.Transmission'),
        'photo_url': lambda d: get_nested_value(d, 'ListOfPhotos.0.PhotoUrl'),
        'comments': lambda d: get_nested_value(d, 'VehicleInfo.Comments'),
        'drive_train': lambda d: get_nested_value(d, 'VehicleInfo.Drivetrain'),
        'body_style': lambda d: get_nested_value(d, 'VehicleInfo.BodyStyle'),
        'vin': lambda d: get_nested_value(d, 'VehicleInfo.VIN'),
        'interior_material': lambda d: get_nested_value(d, 'VehicleInfo.InteriorMaterial'),
        'transmission_speed': lambda d: get_nested_value(d, 'VehicleInfo.TransmissionSpeed'),
        'highway_mpg': lambda d: int(get_nested_value(d, 'VehicleInfo.HwyMPG')),
        'city_mpg': lambda d: int(get_nested_value(d, 'VehicleInfo.CityMPG')),
        'vdp': lambda d: get_nested_value(d, 'VehicleInfo.VDPLink'),
        'trim': lambda d: get_nested_value(d, 'VehicleInfo.Trim'),
        'special_price': lambda d: str(get_nested_value(d, 'Pricing.Special')),
        'engine': lambda d: get_nested_value(d, 'VehicleInfo.Engine'),
        'engine_displacement': lambda d: get_nested_value(d, 'VehicleInfo.EngineDisplacement'),
        'factory_certified': lambda d: get_nested_value(d, 'VehicleInfo.IsCertified'),
        'options': lambda d: get_options(d),
        'priority_options': lambda d: get_priority_options(d),
    },
    'inv_dealer_integration_partner': {
        'provider_dealer_id': lambda d: get_nested_value(d, 'VehicleInfo.ExportDealerID')
    }
}
