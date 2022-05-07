from fastapi import FastAPI, Request, Response, Depends, HTTPException, status, APIRouter
from ..database import get_db
from sqlalchemy.orm import Session
from .. import schemas,models,database,utils
router = APIRouter(
    tags=['Authentication']
)

@router.post("/login")
def userlogin(user_cred: schemas.UserLogin , db : Session = Depends(get_db) ):
    user = db.query(models.User).filter(models.User.email == user_cred.email).first()
    if user is None:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,detail=f"User Does not exist")

    if not utils.verify_password(user_cred.password,user.password):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN,detail=f"Invalid Credentials")

    return {"Token": "Valid Token"}




