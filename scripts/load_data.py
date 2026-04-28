# load_data.py
# Authors: Akanksha Bhardwaj, Srivatsav Vijayaraghavan, Shakthi Balasubramanian
# Purpose: Load CFPB and OSHA data into Neo4j
# Reference: Neo4j Python Driver Docs: https://neo4j.com/docs/python-manual/current/

import pandas as pd
from neo4j import GraphDatabase
from dotenv import load_dotenv
import os

load_dotenv()

URI      = os.getenv("NEO4J_URI")
USER     = os.getenv("NEO4J_USER")
PASSWORD = os.getenv("NEO4J_PASSWORD")

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

# -------------------------------------------------------
# LOAD CFPB COMPLAINTS
# Author: Akanksha Bhardwaj
# -------------------------------------------------------

def load_complaints(driver, filepath, limit=500):
    df = pd.read_csv(filepath, nrows=limit)

    # Normalize column names
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]

    # Keep only rows with a company name
    df = df[df["company"].notna()]

    with driver.session() as session:
        for _, row in df.iterrows():
            session.run("""
                MERGE (c:Company {name: $company_name})
                  ON CREATE SET c.cik = "UNKNOWN-" + $company_name,
                                c.sic_code = "0000",
                                c.state = $state,
                                c.fiscal_year_end = "12-31"
                MERGE (comp:Complaint {complaint_id: $complaint_id})
                  ON CREATE SET comp.date_received   = date($date_received),
                                comp.product_type    = $product,
                                comp.issue_category  = $issue,
                                comp.company_response = $response,
                                comp.timely_response  = $timely
                MERGE (c)-[:RECEIVED_COMPLAINT]->(comp)
            """, {
                "company_name": str(row.get("company", "Unknown")),
                "state":        str(row.get("state", "Unknown")),
                "complaint_id": str(row.get("complaint_id", row.name)),
                "date_received": str(row.get("date_received", "2020-01-01"))[:10],
                "product":      str(row.get("product", "Unknown")),
                "issue":        str(row.get("issue", "Unknown")),
                "response":     str(row.get("company_response", "Unknown")),
                "timely":       str(row.get("timely_response?", "Unknown")) == "Yes"
            })
    print(f"Loaded {len(df)} complaints")

# -------------------------------------------------------
# LOAD OSHA VIOLATIONS
# Author: Srivatsav Vijayaraghavan
# -------------------------------------------------------

def load_osha(driver, inspections_path, violations_path, limit=500):
    insp = pd.read_csv(inspections_path, nrows=limit, low_memory=False)
    viol = pd.read_csv(violations_path,  nrows=limit, low_memory=False)

    insp.columns = [c.strip().lower().replace(" ", "_") for c in insp.columns]
    viol.columns = [c.strip().lower().replace(" ", "_") for c in viol.columns]

    # Join on activity number
    merged = pd.merge(viol, insp, on="activity_nr", how="left")
    merged = merged[merged["estab_name"].notna()]

    with driver.session() as session:
        for _, row in merged.iterrows():
            session.run("""
                MERGE (c:Company {name: $company_name})
                  ON CREATE SET c.cik = "UNKNOWN-" + $company_name,
                                c.sic_code = $sic_code,
                                c.state = $state,
                                c.fiscal_year_end = "12-31"
                MERGE (o:OshaViolation {
                    activity_number: $activity_nr,
                    citation_id: $citation_id
                })
                  ON CREATE SET o.inspection_date  = date($open_date),
                                o.violation_type   = $viol_type,
                                o.penalty_amount   = toFloat($penalty),
                                o.establishment_name = $estab_name,
                                o.state            = $state,
                                o.sic_code         = $sic_code
                MERGE (c)-[:HAS_VIOLATION]->(o)
            """, {
                "company_name": str(row.get("estab_name", "Unknown")),
                "sic_code":     str(row.get("sic_code_x", "0000")),
                "state":        str(row.get("site_state", "Unknown")),
                "activity_nr":  str(row.get("activity_nr", "")),
                "citation_id":  str(row.get("citation_id", "NA")),
                "open_date":    str(row.get("open_date",   "2020-01-01"))[:10],
                "viol_type":    str(row.get("serious",     "Unknown")),
                "penalty":      row.get("current_penalty", 0) or 0,
                "estab_name":   str(row.get("estab_name",  "Unknown"))
            })
    print(f"Loaded {len(merged)} OSHA violations")

# -------------------------------------------------------
# RUN
# -------------------------------------------------------

if __name__ == "__main__":
    print("Loading CFPB complaints...")
    load_complaints(driver, "data/complaints.csv", limit=500)

    print("Loading OSHA violations...")
    load_osha(driver, "data/inspections.csv", "data/violations.csv", limit=500)

    driver.close()
    print("Done.")