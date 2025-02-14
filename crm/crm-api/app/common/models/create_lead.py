from typing import List, Optional
from pydantic import BaseModel, Field
from datetime import datetime, timezone


class VehicleMetadata(BaseModel):
    min_price: Optional[float] = Field(
        None, description="Minimum price for the vehicle"
    )
    max_price: Optional[float] = Field(
        None, description="Maximum price for the vehicle"
    )


class VehicleOfInterest(BaseModel):
    vin: Optional[str] = Field(
        None, max_length=20, description="Vehicle Identification Number"
    )
    stock_number: Optional[str] = Field(
        None, max_length=50, description="Stock number of the vehicle"
    )
    type: Optional[str] = Field(
        None, max_length=100, description="Type of the vehicle (e.g., SUV, Sedan)"
    )
    class_: Optional[str] = Field(
        None,
        alias="class",
        max_length=80,
        description="Class of the vehicle (e.g., Compact)",
    )
    mileage: Optional[int] = Field(
        None, description="Mileage of the vehicle in odometer units"
    )
    year: Optional[int] = Field(
        None, gt=-2147483649, lt=2147483648, description="Year of manufacture"
    )
    make: Optional[str] = Field(
        None, max_length=80, description="Manufacturer of the vehicle"
    )
    model: Optional[str] = Field(
        None, max_length=100, description="Model of the vehicle"
    )
    oem_name: Optional[str] = Field(None, max_length=80, description="OEM name")
    trim: Optional[str] = Field(
        None, max_length=100, description="Trim level of the vehicle"
    )
    body_style: Optional[str] = Field(
        None, max_length=100, description="Body style of the vehicle"
    )
    transmission: Optional[str] = Field(
        None, max_length=80, description="Transmission type"
    )
    interior_color: Optional[str] = Field(
        None, max_length=80, description="Interior color of the vehicle"
    )
    exterior_color: Optional[str] = Field(
        None, max_length=80, description="Exterior color of the vehicle"
    )
    price: Optional[float] = Field(
        None, gt=-2147483649, lt=2147483648, description="Price of the vehicle"
    )
    status: Optional[str] = Field(
        None, max_length=50, description="Current status of the vehicle"
    )
    condition: Optional[str] = Field(
        None, max_length=20, description="Condition of the vehicle (e.g., New, Used)"
    )
    odometer_units: Optional[str] = Field(
        None, max_length=20, description="Units for odometer readings (e.g., miles)"
    )
    vehicle_comments: Optional[str] = Field(
        None, max_length=5000, description="Additional comments about the vehicle"
    )
    trade_in_vin: Optional[str] = Field(
        None, max_length=20, description="VIN for the trade-in vehicle"
    )
    trade_in_year: Optional[int] = Field(
        None, gt=-2147483649, lt=2147483648, description="Year of the trade-in vehicle"
    )
    trade_in_make: Optional[str] = Field(
        None, max_length=80, description="Make of the trade-in vehicle"
    )
    trade_in_model: Optional[str] = Field(
        None, max_length=80, description="Model of the trade-in vehicle"
    )
    metadata: Optional[VehicleMetadata] = Field(
        None, description="Metadata associated with the vehicle"
    )
    crm_vehicle_id: Optional[str] = Field(
        None, max_length=50, description="CRM vehicle identifier"
    )


class Salesperson(BaseModel):
    crm_salesperson_id: str = Field(
        ..., description="CRM identifier for the salesperson"
    )
    first_name: Optional[str] = Field(
        "", max_length=50, description="First name of the salesperson"
    )
    last_name: Optional[str] = Field(
        "", max_length=50, description="Last name of the salesperson"
    )
    email: Optional[str] = Field(
        None, max_length=50, description="Email address of the salesperson"
    )
    phone: Optional[str] = Field(
        None, max_length=20, description="Phone number of the salesperson"
    )
    position_name: Optional[str] = Field(
        None, max_length=50, description="Position or role of the salesperson"
    )
    is_primary: Optional[bool] = Field(
        False, description="Whether this salesperson is the primary contact"
    )


class Metadata(BaseModel):
    crmLeadStatus: Optional[str] = Field(None, description="CRM status of the lead")
    appraisalLink: Optional[str] = Field(
        None, description="Link to the appraisal details"
    )


class CreateLeadRequest(BaseModel):
    consumer_id: int = Field(
        ..., description="Unique identifier for the associated consumer"
    )
    crm_lead_id: Optional[str] = Field(None, description="CRM lead identifier")
    lead_ts: Optional[str] = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        description="Timestamp for the lead creation",
    )
    lead_status: Optional[str] = Field(
        None,
        max_length=50,
        examples=["ACTIVE", "BAD"],
        description="A status that can be used to group leads that are in a similar state",
    )
    lead_substatus: Optional[str] = Field(
        "",
        max_length=50,
        examples=["Appointment Set"],
        description="Current substatus of the lead",
    )
    lead_comment: Optional[str] = Field(
        "",
        max_length=10000,
        examples=["Does this car have a sunroof?"],
        description="Comment about the lead or generated text from the source on behalf of the lead",
    )
    lead_origin: Optional[str] = Field(
        None,
        max_length=100,
        examples=["INTERNET"],
        description="The first point of contact or channel through which the lead is generated",
    )
    lead_source: Optional[str] = Field(
        None,
        max_length=200,
        examples=["cars.com"],
        description="The specific channel through which the lead was generated.",
    )
    lead_source_detail: Optional[str] = Field(
        None, description="Detailed source information of the lead"
    )
    vehicles_of_interest: Optional[List[VehicleOfInterest]] = Field(
        None, description="An array of vehicles the lead was interested in"
    )
    salespersons: Optional[List[Salesperson]] = Field(
        None, description="List of salespersons involved in the lead"
    )
    metadata: Optional[Metadata] = Field(
        None, description="Metadata associated with the lead"
    )
