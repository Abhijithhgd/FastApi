from .. import models, schemas , utils
from typing import List, Optional
from fastapi import FastAPI, Request, Response, Depends, HTTPException, status, APIRouter
from ..database import get_db
from sqlalchemy.orm import Session

router = APIRouter(
    prefix="/users",
    tags=['Users']
)


@router.get("/", response_model=List[schemas.User_Response])
def get_users(db: Session = Depends(get_db)):
    users = db.query(models.User).all()
    return users


@router.get("/{id}", response_model=schemas.User_Response)
def get_user(id: int, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.id == id).first()
    print(type(user))
    if not user:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"user with id : {id} does not exist")
    return user


@router.post("/", response_model=schemas.User_Response, status_code=status.HTTP_201_CREATED)
def create_user(user: schemas.User_Request, db: Session = Depends(get_db)):
    # hash the password - user.password
    hashed_password = utils.get_password_hash(user.password)
    user.password = hashed_password
    new_user = models.User(**user.dict())
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return new_user


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_user(id: int, db: Session = Depends(get_db)):
    print(id)
    print("inside delete")
    user = db.query(models.User).filter(models.User.id == id)
    if user.first() == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"user with id : {id} does not exist")
    user.delete(synchronize_session=False)
    db.commit()
    return {"data": "Successfull Deletion"}


@router.put("/{id}", response_model=schemas.User_Response)
def update_user(id: int, update_user: schemas.User_Request, db: Session = Depends(get_db)):
    print(update_user.dict())
    user = db.query(models.User).filter(models.User.id == id)
    if user.first() == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"user with id : {id} does not exist")
    print(user.update(update_user.dict(), synchronize_session=False))
    db.commit()
    res = user.first()
    return res
