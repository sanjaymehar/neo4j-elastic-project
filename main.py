from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from py2neo import Graph, Node, NodeMatcher, Relationship
from elasticsearch import Elasticsearch
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI()


NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")

graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
matcher = NodeMatcher(graph)

# Elasticsearch connection
ES_HOST = os.getenv("ES_HOST", "http://localhost:9200")
es = Elasticsearch([ES_HOST])

class User(BaseModel):
    name: str
    age: int
    city: str

class FriendRequest(BaseModel):
    user_name: str
    friend_name: str

@app.post("/users/")
async def create_user(user: User):
    node = Node("User", name=user.name, age=user.age, city=user.city)
    graph.create(node)
    # Index user in Elasticsearch
    es.index(index="users", id=user.name, document=user.dict())
    return {"message": "User created successfully"}

@app.get("/users/{name}")
async def read_user(name: str):
    user = matcher.match("User", name=name).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return dict(user)

@app.put("/users/{name}")
async def update_user(name: str, user: User):
    existing_user = matcher.match("User", name=name).first()
    if existing_user is None:
        raise HTTPException(status_code=404, detail="User not found")
    existing_user.update(age=user.age, city=user.city)
    graph.push(existing_user)
    # Update user in Elasticsearch
    es.index(index="users", id=user.name, document=user.dict())
    return {"message": "User updated successfully"}

@app.delete("/users/{name}")
async def delete_user(name: str):
    user = matcher.match("User", name=name).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    graph.delete(user)
    # Delete user from Elasticsearch
    es.delete(index="users", id=name)
    return {"message": "User deleted successfully"}

@app.post("/friends/")
async def add_friend(friend_request: FriendRequest):
    user = matcher.match("User", name=friend_request.user_name).first()
    friend = matcher.match("User", name=friend_request.friend_name).first()
    if user is None or friend is None:
        raise HTTPException(status_code=404, detail="User or Friend not found")
    relationship = Relationship(user, "FRIENDS_WITH", friend)
    graph.create(relationship)
    return {"message": f"{friend_request.user_name} is now friends with {friend_request.friend_name}"}

@app.get("/friends/{user_name}")
async def get_friends(user_name: str):
    user = matcher.match("User", name=user_name).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    friends = list(graph.match((user,), r_type="FRIENDS_WITH"))
    return [{"user_name": user_name, "friend_name": friend.end_node["name"]} for friend in friends]

@app.delete("/friends/")
async def remove_friend(friend_request: FriendRequest):
    user = matcher.match("User", name=friend_request.user_name).first()
    friend = matcher.match("User", name=friend_request.friend_name).first()
    if user is None or friend is None:
        raise HTTPException(status_code=404, detail="User or Friend not found")
    relationship = graph.match_one((user, friend), r_type="FRIENDS_WITH")
    if relationship:
        graph.separate(relationship)
        return {"message": f"{friend_request.user_name} is no longer friends with {friend_request.friend_name}"}
    else:
        raise HTTPException(status_code=404, detail="Friendship not found")

@app.get("/search/")
async def search_users(query: str):
    response = es.search(index="users", query={"multi_match": {"query": query, "fields": ["name", "city"]}})
    results = [hit["_source"] for hit in response["hits"]["hits"]]
    return results
