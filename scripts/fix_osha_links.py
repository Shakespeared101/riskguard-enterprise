# fix_osha_links.py
# Author: Srivatsav Vijayaraghavan
# Purpose: Re-link OSHA violations to matching CFPB company names
# Reference: rapidfuzz docs: https://rapidfuzz.github.io/RapidFuzz/

from neo4j import GraphDatabase
from rapidfuzz import process, fuzz
from dotenv import load_dotenv
import os

load_dotenv()
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
)

def fix_osha_links(driver):
    with driver.session() as session:

        # Get all company names that have complaints (CFPB names)
        cfpb_companies = session.run("""
            MATCH (c:Company)-[:RECEIVED_COMPLAINT]->()
            RETURN DISTINCT c.name AS name
        """).data()
        cfpb_names = [r["name"] for r in cfpb_companies]

        # Get all company names that have violations (OSHA names)
        osha_companies = session.run("""
            MATCH (c:Company)-[:HAS_VIOLATION]->(o:OshaViolation)
            RETURN DISTINCT c.name AS name
        """).data()
        osha_names = [r["name"] for r in osha_companies]

        matched = 0
        for osha_name in osha_names:
            # Find best matching CFPB company name
            result = process.extractOne(
                osha_name,
                cfpb_names,
                scorer=fuzz.token_sort_ratio,
                score_cutoff=80
            )
            if result:
                best_match, score, _ = result
                if best_match != osha_name:
                    # Move violations from OSHA node to CFPB node
                    session.run("""
                        MATCH (osha_co:Company {name: $osha_name})-[:HAS_VIOLATION]->(o:OshaViolation)
                        MATCH (cfpb_co:Company {name: $cfpb_name})
                        MERGE (cfpb_co)-[:HAS_VIOLATION]->(o)
                    """, osha_name=osha_name, cfpb_name=best_match)
                    matched += 1
                    print(f"Matched: {osha_name} → {best_match} (score: {score})")

        print(f"Total matched: {matched}")

fix_osha_links(driver)
driver.close()