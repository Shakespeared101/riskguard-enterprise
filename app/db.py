# app/db.py
# Author: Shakthi Balasubramanian
# Purpose: Neo4j database connection handler
# Reference: Neo4j Python Driver: https://neo4j.com/docs/python-manual/current/

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
)

def get_driver():
    return driver