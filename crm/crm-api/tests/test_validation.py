import pytest
import copy
from pydantic import ValidationError
from models.create_lead import CreateLeadRequest, VehicleOfInterest, Salesperson, Metadata, VehicleMetadata

# Test data for the models
VALID_CREATE_LEAD_REQUEST = {
    "consumer_id": 2,
    "crm_lead_id": "D973DAF4-D0F2",
    "lead_ts": "2023-09-22T14:00:00Z",
    "lead_status": "ACTIVE",
    "lead_substatus": "Appointment Set",
    "lead_comment": "Does this car have a sunroof?",
    "lead_origin": "INTERNET",
    "lead_source": "cars.com",
    "lead_source_detail": "Inventory Leads Website",
    "vehicles_of_interest": [
        {
            "vin": "1HGBH41JXMN109186",
            "stock_number": "SN12345",
            "type": "SUV",
            "class": "Compact",
            "mileage": 50000,
            "year": 2023,
            "make": "Ford",
            "model": "F-150",
            "oem_name": "Ford",
            "trim": "GT",
            "body_style": "SUV",
            "transmission": "Manual",
            "interior_color": "Black",
            "exterior_color": "White",
            "price": 25000,
            "status": "Available",
            "condition": "Used",
            "odometer_units": "miles",
            "vehicle_comments": "Has a scratch",
            "trade_in_vin": "1HGBH41JXMN109186",
            "trade_in_year": 2021,
            "trade_in_make": "Ford",
            "trade_in_model": "F-150",
            "metadata": {
                "min_price": 25000,
                "max_price": 30000
            },
            "crm_vehicle_id": "8EEAD89E-E87"
        }
    ],
    "salespersons": [
        {
            "crm_salesperson_id": "123abc-1234ab-123abc",
            "first_name": "John",
            "last_name": "Smith",
            "email": "john.smith@example.com",
            "phone": "123-456-7890",
            "position_name": "Sales Manager",
            "is_primary": True
        }
    ],
    "metadata": {
        "crmLeadStatus": "Active",
        "appraisalLink": "https://abc.com"
    }
}


# Test for valid CreateLeadRequest
def test_create_lead_request_valid():
    lead_request = CreateLeadRequest(**VALID_CREATE_LEAD_REQUEST)
    assert lead_request.consumer_id == 2
    assert lead_request.lead_status == "ACTIVE"
    assert lead_request.vehicles_of_interest[0].vin == "1HGBH41JXMN109186"
    assert lead_request.salespersons[0].first_name == "John"
    assert lead_request.metadata.crmLeadStatus == "Active"


# Test missing required fields
def test_create_lead_request_missing_required_field():
    invalid_data = copy.deepcopy(VALID_CREATE_LEAD_REQUEST)
    invalid_data.pop("consumer_id")  # Remove a required field
    with pytest.raises(ValidationError) as exc_info:
        CreateLeadRequest(**invalid_data)
    assert "consumer_id" in str(exc_info.value)


# Test invalid data type
def test_create_lead_request_invalid_data_type():
    invalid_data = copy.deepcopy(VALID_CREATE_LEAD_REQUEST)
    invalid_data["consumer_id"] = "not-an-integer"  # Invalid type
    with pytest.raises(ValidationError) as exc_info:
        CreateLeadRequest(**invalid_data)
    assert "Input should be a valid integer" in str(exc_info.value)


# Test for invalid nested data in vehicles_of_interest
def test_invalid_vehicle_of_interest():
    invalid_data = copy.deepcopy(VALID_CREATE_LEAD_REQUEST)
    invalid_data["vehicles_of_interest"][0]["vin"] = None  # Invalid VIN
    with pytest.raises(ValidationError) as exc_info:
        CreateLeadRequest(**invalid_data)
    assert "vin" in str(exc_info.value)


# Test for invalid nested data in salespersons
def test_invalid_salesperson():
    invalid_data = copy.deepcopy(VALID_CREATE_LEAD_REQUEST)
    invalid_data["salespersons"][0]["crm_salesperson_id"] = None  # Invalid ID
    with pytest.raises(ValidationError) as exc_info:
        CreateLeadRequest(**invalid_data)
    assert "crm_salesperson_id" in str(exc_info.value)


# Test for valid VehicleOfInterest model
def test_vehicle_of_interest_valid():
    vehicle_data = VALID_CREATE_LEAD_REQUEST["vehicles_of_interest"][0]
    vehicle = VehicleOfInterest(**vehicle_data)
    assert vehicle.vin == "1HGBH41JXMN109186"
    assert vehicle.metadata.min_price == 25000


# Test for valid Salesperson model
def test_salesperson_valid():
    salesperson_data = VALID_CREATE_LEAD_REQUEST["salespersons"][0]
    salesperson = Salesperson(**salesperson_data)
    assert salesperson.first_name == "John"
    assert salesperson.is_primary is True


# Test for Metadata model
def test_metadata_valid():
    metadata_data = VALID_CREATE_LEAD_REQUEST["metadata"]
    metadata = Metadata(**metadata_data)
    assert metadata.crmLeadStatus == "Active"
    assert metadata.appraisalLink == "https://abc.com"


# Test for optional fields
def test_optional_fields():
    partial_data = {
        "consumer_id": 2,
        "lead_status": "ACTIVE",
    }
    lead_request = CreateLeadRequest(**partial_data)
    assert lead_request.consumer_id == 2
    assert lead_request.lead_status == "ACTIVE"
    assert lead_request.metadata is None
    assert lead_request.vehicles_of_interest is None
    assert lead_request.salespersons is None
