"""Dataclass models for lambda request payloads."""

from dataclasses import dataclass
from typing import Optional


@dataclass
class AppointmentSlots:
    request_id: str
    integration_dealer_id: str
    dealer_timezone: str
    op_code: str
    start_time: str
    end_time: str
    vin: Optional[str] = None
    year: Optional[int] = None
    make: Optional[str] = None
    model: Optional[str] = None


@dataclass
class CreateAppointment:
    request_id: str
    source_product: str
    integration_dealer_id: str
    dealer_timezone: str
    op_code: str
    timeslot: str
    duration: int
    comment: str
    first_name: str
    last_name: str
    email_address: str
    phone_number: str
    vin: Optional[str] = None
    year: Optional[int] = None
    make: Optional[str] = None
    model: Optional[str] = None


@dataclass
class GetAppointments:
    request_id: str
    integration_dealer_id: str
    dealer_timezone: str
    first_name: str
    last_name: str
    email_address: str
    phone_number: str
    vin: str
    year: Optional[int] = None
    make: Optional[str] = None
    model: Optional[str] = None


@dataclass
class UpdateAppointment:
    request_id: str
    source_product: str
    integration_dealer_id: str
    dealer_timezone: str
    integration_appointment_id: str
    op_code: str
    timeslot: str
    first_name: str
    last_name: str
    duration: Optional[int] = 15
    comment: Optional[str] = None
    email_address: Optional[str] = 'not-available@noemail.com'
    phone_number: Optional[str] = '5550000000'
    vin: Optional[str] = None
    year: Optional[int] = None
    make: Optional[str] = None
    model: Optional[str] = None
