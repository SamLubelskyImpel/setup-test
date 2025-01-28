from pydantic import BaseModel, Field

class CreateConsumerRequest(BaseModel):
    name: str = Field(..., description="The name of the consumer")
    email: str = Field(..., regex=r"[^@]+@[^@]+\.[^@]+", description="The email of the consumer")
