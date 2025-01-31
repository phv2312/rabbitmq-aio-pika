from pydantic import BaseModel


class Message(BaseModel):
    content: str
    
    @classmethod
    def from_str(cls, content: str) -> "Message":
        return cls(content=content)
