from datetime import datetime
from pydantic import BaseModel

class Request(BaseModel):
    location: str 
    begintime: str 
    endtime: str 
