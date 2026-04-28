# fix_osha_manual.py
# Author: Srivatsav Vijayaraghavan
# Purpose: Manually map OSHA company names to CFPB company names
# and re-link HAS_VIOLATION relationships to the correct company node

from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()
driver = GraphDatabase.driver(
    os.getenv("NEO4J_URI"),
    auth=(os.getenv("NEO4J_USER"), os.getenv("NEO4J_PASSWORD"))
)

# Manual mapping: OSHA name -> exact CFPB name in your database
MANUAL_MAP = {
    "JPMorgan Chase Operations":  "JPMorgan Chase & Co.",
    "Bank of America Corp":       "BANK OF AMERICA, NATIONAL ASSOCIATION",
    "Wells Fargo Bank":           "WELLS FARGO & COMPANY",
    "Citibank NA":                "CITIBANK, N.A.",
    "Goldman Sachs Group":        "GOLDMAN SACHS BANK USA",
    "Morgan Stanley":             "MORGAN STANLEY SMITH BARNEY LLC",
    "American Express Co":        "AMERICAN EXPRESS COMPANY",
    "Capital One Financial":      "CAPITAL ONE FINANCIAL CORPORATION",
    "US Bancorp":                 "U.S. BANCORP",
}

def fix_links(driver):
    with driver.session() as session:

        # First print all CFPB company names so we can verify mappings
        print("--- All CFPB company names in DB ---")
        cfpb = session.run("""
            MATCH (c:Company)-[:RECEIVED_COMPLAINT]->()
            RETURN DISTINCT c.name AS name
            ORDER BY name
        """).data()
        for r in cfpb:
            print(r["name"])

        print("\n--- Applying manual mappings ---")
        matched = 0
        for osha_name, cfpb_name in MANUAL_MAP.items():
            # Check if OSHA company node exists
            osha_exists = session.run("""
                MATCH (c:Company {name: $name})-[:HAS_VIOLATION]->()
                RETURN count(c) AS cnt
            """, name=osha_name).single()["cnt"]

            # Check if CFPB company node exists
            cfpb_exists = session.run("""
                MATCH (c:Company {name: $name})
                RETURN count(c) AS cnt
            """, name=cfpb_name).single()["cnt"]

            if osha_exists > 0 and cfpb_exists > 0:
                # Move violations to CFPB node
                result = session.run("""
                    MATCH (osha_co:Company {name: $osha_name})
                          -[:HAS_VIOLATION]->(o:OshaViolation)
                    MATCH (cfpb_co:Company {name: $cfpb_name})
                    MERGE (cfpb_co)-[:HAS_VIOLATION]->(o)
                    RETURN count(o) AS moved
                """, osha_name=osha_name, cfpb_name=cfpb_name).single()["moved"]

                print(f"✓ {osha_name} → {cfpb_name} ({result} violations moved)")
                matched += result

            elif osha_exists == 0:
                print(f"✗ OSHA node not found: {osha_name}")
            elif cfpb_exists == 0:
                print(f"✗ CFPB node not found: {cfpb_name} — check exact name above")

        print(f"\nTotal violations re-linked: {matched}")

fix_links(driver)
driver.close()