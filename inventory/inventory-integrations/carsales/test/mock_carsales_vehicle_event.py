from uuid import uuid4
from random import randint

def get_event():
    vin = str(uuid4())
    dealer = ["dealer-1", "dealer-2", "dealer-3"][randint(0, 2)]

    return dealer, vin, {
      "Type": "CAR",
      "Identifier": str(uuid4()),
      "ListingType": ["Used", "New"][randint(0, 1)],
      "SaleStatus": ["For Sale", "Withdrawn", "Sold"][randint(0, 2)],
      "SaleType": "Retail",
      "Description": "Introducing the 2021 Mazda BT-50 TFR40J XTR Utility Dual Cab, built to tackle every challenge with confidence. This versatile 4x2 automatic features a powerful and reliable 3.0L turbo diesel engine, ensuring robust performance for work and play.\\n\\nWith a thorough 150-point safety check completed, it's ready to hit the road with assurance. Boasting a single owner and a great service history, this BT-50 stands out as a testament to its reliability and meticulous upkeep.\\n\\nIdeal for business or pleasure, the spacious Dual Cab offers ample room and practicality, making it perfect for both work tasks and family adventures. Experience the blend of strength, reliability, and comfort in the 2021 Mazda BT-50 XTR Utility Dual Cab.\\n\\n home in a vehicle that's more than capable?it's exceptional.\\n\\nEach of our approved used cars comes standard with:\\n 7 days love it or swap it*\\n 6 month / 8,000 km warranty^\\n 12 months roadside assistance*\\n 150 point safety check\\n\\nPlus, experience the simplicity of our haggle-free pricing* ? what you see is what you pay. Buy with confidence knowing you?re receiving our best price upfront.\\n\\nOur aim is to provide a smooth and hassle-free purchasing experience every time. Whether you decide to visit us in person at one of our dealerships or prefer the convenience of online or phone communication (available 7 days a week from 8:30am to 8:00pm), our highly trained teams are ready to deliver exceptional service. \\n\\nWe look forward to hearing from you soon!\\n\\n About Us \\n\\nWe?re part of a leading global retail group, partnering with many of the world's most-recognised brands including Subaru, Audi, BMW, Jaguar, Peugeot, Citroen, Land Rover, Mercedes-Benz, Toyota, and Volkswagen.\\n\\n* Terms and conditions apply. \\n\\n^ Warranty is valid for vehicles up to 10 years old or 160,000 km, whichever occurs first, and which commences on the date of delivery refer to our warranty booklet.\\nAdditional Comments (will appear on 3rd party websites)\\r\\n\\n",
      "Registration": { "Number": "EOZ90W", "ExpiresUtc": "2024-10-07T00:00:00Z" },
      "Identification": [
        { "Type": "NetworkId", "Value": "OAG-AD-23405796" },
        { "Type": "StockNumber", "Value": str(uuid4()) },
        { "Type": "VIN", "Value": vin }
      ],
      "Colours": [
        { "Location": "Exterior", "Generic": "White", "Name": "Ice White" },
        { "Location": "Interior", "Generic": "Black", "Name": "Black" }
      ],
      "OdometerReadings": [{ "Type": "", "Value": 45887.0, "UnitOfMeasure": "KM" }],
      "Seller": {
        "Identifier": dealer,
        "Type": "Dealer",
        "Name": "bravoauto Penrith",
        "Addresses": [
          {
            "Line1": "14 Jack Williams Drive",
            "Suburb": "Penrith",
            "State": "New South Wales",
            "Postcode": "2750"
          }
        ],
        "Identification": [
          { "Type": "LegacyId", "Value": "54618" },
          { "Type": "LMCT", "Value": "14550" }
        ]
      },
      "Specification": {
        "Identifier": "c492be4f-a4cc-4a95-899b-4f65e0b1a488",
        "SpecificationSource": "REDBOOK",
        "SpecificationCode": "AUVMAZD2021AEDC",
        "CountryCode": "AU",
        "Make": "Mazda",
        "Model": "BT-50",
        "Series": "TF",
        "ReleaseDate": { "Year": 2021 },
        "Title": "2021 Mazda BT-50 XTR TF Auto 4x2 Dual Cab",
        "Attributes": [
          { "Name": "Badge", "Value": "XTR" },
          { "Name": "EngineType", "Value": "Piston" },
          { "Name": "EngineSize", "Value": "2999" },
          { "Name": "Cylinders", "Value": "4" },
          { "Name": "FuelType", "Value": "Diesel" },
          { "Name": "Transmission", "Value": "Sports Automatic" },
          { "Name": "Gears", "Value": "6" },
          { "Name": "Drive", "Value": "Rear Wheel Drive" },
          { "Name": "BodyStyle", "Value": "Ute" },
          { "Name": "Doors", "Value": "4" },
          { "Name": "Seats", "Value": "5" }
        ]
      },
      "GeoLocation": { "Latitude": -33.753418, "Longitude": 150.673447 },
      "Media": {
        "Photos": [
          {
            "Url": "https://carsales.pxcrush.net/car/dealer/e6eccb23038c7d88dded2639eafd4627.jpg?pxc_expires=20240902000008&pxc_clear=1&pxc_signature=9108abc6acba8103b9b01a682a1e29d9&pxc_size=1920,1080&pxc_method=limit",
            "LastModifiedUtc": "2024-08-26T00:00:03Z",
            "Order": 0
          }
        ]
      },
      "ComplianceDate": { "Month": 6, "Year": 2021 },
      "BuildDate": { "Month": 5, "Year": 2021 },
      "Warranty": {},
      "PublishingDestinations": [{ "Name": "SPINCAR" }],
      "PriceList": [
        { "Type": "EGC", "Currency": "AUD", "Amount": 36900.0 },
        { "Type": "DAP", "Currency": "AUD", "Amount": 37990.0 }
      ],
      "Certifications": [],
      "LastModifiedUtc": "2024-08-26T00:00:04Z",
      "CreatedUtc": "2024-06-26T03:06:19Z",
      "options": ["12V Socket(s) - Auxiliary", "18 Alloy Wheels", "8 Speaker Stereo", "ABS (Antilock Brakes)", "Adjustable Steering Col. - Tilt & Reach", "Airbag - Driver", "Airbag - Front Centre", "Airbag - Knee Driver", "Airbag - Passenger", "Airbags - Head for 1st Row Seats (Front)", "Airbags - Head for 2nd Row Seats", "Airbags - Side for 1st Row Occupants (Front)", "Air Cond. - Climate Control 2 Zone", "Air Conditioning", "Alarm", "Armrest - Front Centre (Shared)", "Armrest - Rear Centre (Shared)", "Audio - AAC Decoder", "Audio - Aux Input Socket (MP3/CD/Cassette)", "Audio - Aux Input USB Socket", "Audio Decoder - WMA ", "Audio - Input for iPod", "Audio - MP3 Decoder", "Blind Spot Sensor", "Bluetooth System", "Body Colour - Door Handles", "Body Colour - Exterior Mirrors Partial", "Brake Assist", "Brake Emergency Display - Hazard/Stoplights", "Brakes - Rear Drum", "Camera - Rear Vision", "Carpeted - Cabin Floor", "Central Locking - Key Proximity", "Central Locking - Remote/Keyless", "Clock - Digital", "Collision Mitigation - Forward (High speed)", "Collision Mitigation - Forward (Low speed)", "Collision Mitigation - Post Collision Steer/Brake", "Collision Warning - Forward", "Control - Electronic Stability", "Control - Hill Descent", "Control - Pedestrian Avoidance with Braking", "Control - Rollover Stability", "Control - Traction", "Control - Trailer Sway", "Cross Traffic Alert - Front", "Cruise Control - Distance Control", "Cruise Control - with Brake Function (limiter)", "Cup Holders - 1st Row", "Daytime Running Lamps - LED", "Demister - Rear Windscreen with Timer", "Diff lock(s)", "Disc Brakes Front Ventilated", "Door Pockets - 1st row (Front)", "Door Pockets - 2nd row (rear)", "Drive By Wire (Electronic Throttle Control)", "Driver Attention Detection", "EBD (Electronic Brake Force Distribution)", "Electronic Differential Lock", "Engine Immobiliser", "Fog Lamps - Front LED", "Footrest - Drivers", "Gloveboxes - Upper & Lower", "GPS (Satellite Navigation)", "Grab Handle - Passengers Side", "Handbrake - Fold Down", "Headlamp - High Beam Auto Dipping", "Headlamps Automatic (light sensitive)", "Headlamps - Electric Level Adjustment", "Headlamps - LED", "Headrests - Adjustable 1st Row (Front)", "Headrests - Adjustable 2nd Row x3", "Hill Holder", "Illuminated - Entry/Exit with Fade", "Illuminated - Switch Panel (Window/ Locking)", "Independent Front Suspension", "Intermittent Wipers - Variable", "Keyless Start - Key/FOB Proximity related", "Lane Departure Warning", "Lane Keeping - Active Assist", "Leather Gear Knob", "Leather Steering Wheel", "Map/Reading Lamps - for 1st Row", "Mudflaps - front", "Multi-function Control Screen - Colour", "Multi-function Steering Wheel", "Parking Assist - Graphical Display", "Power Door Mirrors", "Power Door Mirrors - Folding", "Power Steering", "Power Steering - Electric Assist", "Power Windows - Front & Rear", "Radio - Digital (DAB+)", "Rain Sensor (Auto wipers)", "Rear View Mirror - Electric Anti Glare", "Remote Fuel Lid Release", "Seatback Pocket - Front Passenger Seat", "Seatbelt - Adjustable Height 1st Row", "Seatbelt - Load Limiters 1st Row (Front)", "Seatbelt - Load Limiters 2nd Row(Rear Outer seats)", "Seatbelt - Pretensioners 1st Row (Front)", "Seatbelt - Pretensioners 2nd Row(Rear Outer seats)", "Seatbelts - Lap/Sash for 5 seats", "Seat - Drivers Lumbar Adjustment Manual", "Seat - Height Adjustable Driver", "Seats - 2nd Row Split Fold", "Seats - Bucket (Front)", "Side Steps", "Skid Plate - Front", "Skid Plate - Middle (Transmission case)", "Smart Device App Display/Control", "Smart Device Integration - Android Auto", "Smart Device Integration - Apple CarPlay", "Smart Device Integration - Apple Carplay Wireless", "Spare Wheel - Full Size Steel", "Speed Limiter", "Speed Zone Reminder - Road Sign Recognition", "Storage Compartment - Centre Console 1st Row", "Storage Compartment - Under 2nd Row (Rear)  Seat", "Sunglass Holder", "Sunvisor - Vanity Mirror for Driver", "Sunvisor - Vanity Mirror for Passenger", "Tacho", "Trim - Cloth", "Trip Computer", "USB Socket(s) - Charging", "Warning - Rear Cross Traffic (when reversing)"]
    }
