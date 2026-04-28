from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

URI = os.getenv("NEO4J_URI")
USER = os.getenv("NEO4J_USER")
PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

def test_connection(driver):
    with driver.session() as session:
        result = session.run("MATCH (n) RETURN count(n) AS node_count")
        record = result.single()
        print(f"Connection successful! Total nodes: {record['node_count']}")

test_connection(driver)
driver.close()
