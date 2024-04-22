from dataclasses import dataclass

@dataclass
class AppointmentSlots:
    request_id: str
    integration_dealer_id: str
    dealer_timezone: str
    op_code: str
    start_time: str
    end_time: str
    vin: str
    year: int
    make: str
    model: str

@dataclass
class CreateAppointment:
    request_id: str
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
    vin: str
    year: int
    make: str
    model: str

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