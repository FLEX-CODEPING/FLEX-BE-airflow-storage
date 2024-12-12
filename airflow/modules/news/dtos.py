from pydantic import BaseModel
from datetime import datetime

class NewsArticleDTO(BaseModel):
    title: str
    published_date: datetime
    url: str
    content: str
    keyword: str
    press: str