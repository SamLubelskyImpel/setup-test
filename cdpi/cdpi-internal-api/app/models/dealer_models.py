from typing import Optional
from pydantic import BaseModel, Field, Extra

class DealerCreateRequest(BaseModel):
    dealer_name: str = Field(..., max_length=80, description="Name of the dealer")
    sfdc_account_id: str = Field(..., max_length=40, description="Salesforce Account ID")
    salesai_dealer_id: str = Field(..., max_length=80, description="SalesAI Dealer ID")
    serviceai_dealer_id: str = Field(..., max_length=80, description="ServiceAI Dealer ID")
    cdp_dealer_id: str = Field(..., max_length=80, description="CDP Dealer ID")
    impel_integration_partner_name: str = Field(..., max_length=80, examples=["FORD_DIRECT"])
    is_active: bool = Field(False, description="Is the dealer active?")


class DealerUpdateRequest(BaseModel):
    dealer_id: int
    dealer_name: Optional[str] = None
    sfdc_account_id: Optional[str] = None
    salesai_dealer_id: Optional[str] = None
    serviceai_dealer_id: Optional[str] = None
    is_active: Optional[bool] = None


class DealerRetrieveRequest(BaseModel):
    dealer_id: Optional[int] = None
    dealer_name: Optional[str] = None
    sfdc_account_id: Optional[str] = None
    salesai_dealer_id: Optional[str] = None
    serviceai_dealer_id: Optional[str] = None
    is_active: Optional[bool] = None
    impel_integration_partner_name: Optional[str] = None
    page: Optional[int] = Field(1, ge=1, description="Page number for pagination")
    limit: Optional[int] = Field(100, ge=1, le=1000, description="Number of records per page")

    class Config:
        extra = Extra.forbid