from crm import CRMSmokeTest
from dms import DMSSmokeTest
from appointment import AppointmentSmokeTest


CRMSmokeTest().run()
DMSSmokeTest().run()
AppointmentSmokeTest().run()

