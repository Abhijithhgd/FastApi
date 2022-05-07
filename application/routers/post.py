from .. import models, schemas
from typing import List, Optional
from fastapi import FastAPI, Request, Response, Depends, HTTPException, status, APIRouter
from ..database import get_db
from sqlalchemy.orm import Session

# from ..dependencies import get_token_header

router = APIRouter(
    prefix="/posts",
    tags=["posts"],
    # dependencies=[Depends(get_token_header)],
    responses={404: {"description": "Not found"}},
)


@router.get("/", response_model=List[schemas.Post_Response])
def get_posts(db: Session = Depends(get_db)):
    posts = db.query(models.Post).all()
    return posts


@router.get("/{id}", response_model=schemas.Post_Response)
def get_post(id: int, db: Session = Depends(get_db)):
    post = db.query(models.Post).filter(models.Post.id == id).first()
    print(type(post))
    if not post:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"post with id : {id} does not exist")
    return post


@router.post("/", response_model=schemas.Post_Response, status_code=status.HTTP_201_CREATED)
def create_post(post: schemas.Post_Request, db: Session = Depends(get_db)):
    new_post = models.Post(**post.dict())
    db.add(new_post)
    db.commit()
    db.refresh(new_post)
    # posts.append(req_info)
    # print(posts)
    return new_post


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_post(id: int, db: Session = Depends(get_db)):
    print(id)
    print("inside delete")
    post = db.query(models.Post).filter(models.Post.id == id)
    # print(post.first())
    if post.first() == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"post with id : {id} does not exist")
    post.delete(synchronize_session=False)
    db.commit()
    return {"data": "Successfull Deletion"}


@router.put("/{id}", response_model=schemas.Post_Response)
def update_post(id: int, update_post: schemas.Post_Request, db: Session = Depends(get_db)):
    print(update_post.dict())
    post = db.query(models.Post).filter(models.Post.id == id)
    if post.first() == None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"post with id : {id} does not exist")
    print(post.update(update_post.dict(), synchronize_session=False))
    db.commit()
    res = post.first()
    return res
