from pydantic import BaseModel, EmailStr
from datetime import datetime


class Post_Request(BaseModel):
    title: str
    content: str
    published: bool = True


class Post_Response(BaseModel):
    title: str
    content: str
    published: bool = True
    created_at = datetime

    class Config:
        orm_mode = True


class User_Request(BaseModel):
    password: str
    email: EmailStr


class User_Response(BaseModel):
    id: int
    email: str
    created_at: datetime

    class Config:
        orm_mode = True

class UserLogin(BaseModel):
    email: EmailStr
    password : str

