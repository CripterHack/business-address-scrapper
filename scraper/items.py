from scrapy import Item, Field
from typing import Optional
from dataclasses import dataclass
from datetime import datetime

class BusinessItem(Item):
    """Define the structure of scraped business data"""
    business_name = Field()
    address = Field()
    city = Field()
    state = Field()
    zip_code = Field()
    violation_type = Field()
    nsl_published_date = Field()
    nsl_effective_date = Field()
    remediated_date = Field()
    verified = Field()
    source_url = Field()
    relevance_score = Field()
    scraped_at = Field()

    @classmethod
    def from_business_data(cls, data: 'BusinessData') -> 'BusinessItem':
        """Create a BusinessItem from a BusinessData instance"""
        item = cls()
        for field in data.__dataclass_fields__:
            item[field] = getattr(data, field)
        return item

    def to_dict(self) -> dict:
        """Convert item to dictionary"""
        return {key: self.get(key) for key in self.fields} 