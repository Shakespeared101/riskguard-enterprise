# app/routes.py
# Authors: Akanksha Bhardwaj, Srivatsav Vijayaraghavan, Shakthi Balasubramanian
# Purpose: All Flask routes for RiskGuard Enterprise
# Reference: Flask Docs: https://flask.palletsprojects.com/

from flask import Blueprint, render_template, request, redirect, url_for, flash
from app.db import get_driver

main = Blueprint("main", __name__)

# -------------------------------------------------------
# HOME — Dashboard with node counts
# Author: Shakthi Balasubramanian
# -------------------------------------------------------
@main.route("/")
def index():
    driver = get_driver()
    with driver.session() as session:
        counts = session.run("""
            MATCH (n)
            RETURN labels(n)[0] AS type, count(n) AS count
        """).data()

        recent_complaints = session.run("""
            MATCH (c:Company)-[:RECEIVED_COMPLAINT]->(comp:Complaint)
            RETURN c.name AS company,
                   comp.product_type AS product,
                   comp.date_received AS date
            ORDER BY comp.date_received DESC
            LIMIT 5
        """).data()

    return render_template("index.html",
                           counts=counts,
                           recent_complaints=recent_complaints)

# -------------------------------------------------------
# LIST COMPANIES — Read
# Author: Akanksha Bhardwaj
# -------------------------------------------------------
@main.route("/companies")
def companies():
    driver = get_driver()
    with driver.session() as session:
        results = session.run("""
            MATCH (c:Company)
            OPTIONAL MATCH (c)-[:RECEIVED_COMPLAINT]->(comp:Complaint)
            OPTIONAL MATCH (c)-[:HAS_VIOLATION]->(o:OshaViolation)
            RETURN c.name AS name,
                   c.ticker AS ticker,
                   c.sic_code AS sic_code,
                   c.state AS state,
                   count(DISTINCT comp) AS complaint_count,
                   count(DISTINCT o) AS violation_count
            ORDER BY complaint_count DESC
            LIMIT 50
        """).data()
    return render_template("companies.html", companies=results)

# -------------------------------------------------------
# COMPANY DETAIL — Read single company
# Author: Akanksha Bhardwaj
# -------------------------------------------------------
@main.route("/companies/<name>")
def company_detail(name):
    driver = get_driver()
    with driver.session() as session:
        company = session.run("""
            MATCH (c:Company {name: $name})
            RETURN c
        """, name=name).single()

        complaints = session.run("""
            MATCH (c:Company {name: $name})-[:RECEIVED_COMPLAINT]->(comp:Complaint)
            RETURN comp.complaint_id AS id,
                   comp.date_received AS date,
                   comp.product_type AS product,
                   comp.issue_category AS issue,
                   comp.company_response AS response
            LIMIT 10
        """, name=name).data()

        violations = session.run("""
            MATCH (c:Company {name: $name})-[:HAS_VIOLATION]->(o:OshaViolation)
            RETURN o.activity_number AS id,
                   o.inspection_date AS date,
                   o.violation_type AS type,
                   o.penalty_amount AS penalty,
                   o.state AS state
            LIMIT 10
        """, name=name).data()

    return render_template("company_detail.html",
                           company=company,
                           complaints=complaints,
                           violations=violations,
                           name=name)

# -------------------------------------------------------
# ADD COMPANY — Create
# Author: Srivatsav Vijayaraghavan
# -------------------------------------------------------
@main.route("/companies/add", methods=["GET", "POST"])
def add_company():
    if request.method == "POST":
        driver = get_driver()
        with driver.session() as session:
            session.run("""
                MERGE (c:Company {name: $name})
                SET c.ticker = $ticker,
                    c.cik = $cik,
                    c.sic_code = $sic_code,
                    c.state = $state,
                    c.fiscal_year_end = $fiscal_year_end
            """, {
                "name": request.form["name"],
                "ticker": request.form["ticker"],
                "cik": request.form["cik"],
                "sic_code": request.form["sic_code"],
                "state": request.form["state"],
                "fiscal_year_end": request.form["fiscal_year_end"]
            })
        flash("Company added successfully", "success")
        return redirect(url_for("main.companies"))
    return render_template("add_company.html")

# -------------------------------------------------------
# UPDATE COMPANY — Update
# Author: Srivatsav Vijayaraghavan
# -------------------------------------------------------
@main.route("/companies/update/<name>", methods=["GET", "POST"])
def update_company(name):
    driver = get_driver()
    if request.method == "POST":
        with driver.session() as session:
            session.run("""
                MATCH (c:Company {name: $name})
                SET c.ticker = $ticker,
                    c.sic_code = $sic_code,
                    c.state = $state
            """, {
                "name": name,
                "ticker": request.form["ticker"],
                "sic_code": request.form["sic_code"],
                "state": request.form["state"]
            })
        flash("Company updated successfully", "success")
        return redirect(url_for("main.company_detail", name=name))

    with driver.session() as session:
        company = session.run("""
            MATCH (c:Company {name: $name}) RETURN c
        """, name=name).single()
    return render_template("update_company.html", company=company, name=name)

# -------------------------------------------------------
# DELETE COMPANY — Delete
# Author: Shakthi Balasubramanian
# -------------------------------------------------------
@main.route("/companies/delete/<name>", methods=["POST"])
def delete_company(name):
    driver = get_driver()
    with driver.session() as session:
        session.run("""
            MATCH (c:Company {name: $name})
            DETACH DELETE c
        """, name=name)
    flash(f"{name} deleted successfully", "warning")
    return redirect(url_for("main.companies"))

# -------------------------------------------------------
# RISK SCORE PAGE — per company with contributing factors
# Author: Akanksha Bhardwaj
# -------------------------------------------------------
@main.route("/risk/<name>")
def risk_score(name):
    driver = get_driver()
    with driver.session() as session:

        # Get this company's risk score
        score = session.run("""
            MATCH (c:Company {name: $name})-[:HAS_RISK_SCORE]->(rs:RiskScore)
            RETURN rs.composite_score AS composite,
                   rs.complaint_risk_score AS complaint_risk,
                   rs.safety_risk_score AS safety_risk,
                   rs.financial_risk_score AS financial_risk,
                   rs.complaint_count AS complaint_count,
                   rs.violation_count AS violation_count,
                   rs.total_penalty AS total_penalty
        """, name=name).single()

        # Get top 10 companies for comparison
        comparison = session.run("""
            MATCH (c:Company)-[:HAS_RISK_SCORE]->(rs:RiskScore)
            RETURN c.name AS company,
                   rs.composite_score AS score
            ORDER BY rs.composite_score DESC
            LIMIT 10
        """).data()

        # Get complaint breakdown by product type
        complaint_breakdown = session.run("""
            MATCH (c:Company {name: $name})-[:RECEIVED_COMPLAINT]->(comp:Complaint)
            RETURN comp.product_type AS product,
                   count(comp) AS count
            ORDER BY count DESC LIMIT 6
        """, name=name).data()

        # Get violation breakdown by type
        violation_breakdown = session.run("""
            MATCH (c:Company {name: $name})-[:HAS_VIOLATION]->(o:OshaViolation)
            RETURN o.violation_type AS type,
                   count(o) AS count,
                   sum(o.penalty_amount) AS penalty
            ORDER BY penalty DESC
        """, name=name).data()

    return render_template("risk_score.html",
                           name=name,
                           score=score,
                           comparison=comparison,
                           complaint_breakdown=complaint_breakdown,
                           violation_breakdown=violation_breakdown)


# -------------------------------------------------------
# KNOWLEDGE GRAPH PAGE
# Author: Srivatsav Vijayaraghavan
# Reference: vis.js network: https://visjs.github.io/vis-network/docs/
# -------------------------------------------------------
@main.route("/graph")
def knowledge_graph():
    driver = get_driver()
    with driver.session() as session:

        # Get top 15 companies by complaint count for the graph
        companies = session.run("""
            MATCH (c:Company)-[:RECEIVED_COMPLAINT]->(comp:Complaint)
            WITH c, count(comp) AS complaint_count
            ORDER BY complaint_count DESC LIMIT 15
            OPTIONAL MATCH (c)-[:HAS_VIOLATION]->(o:OshaViolation)
            OPTIONAL MATCH (c)-[:HAS_RISK_SCORE]->(rs:RiskScore)
            RETURN c.name AS name,
                   complaint_count,
                   count(DISTINCT o) AS violation_count,
                   rs.composite_score AS risk_score
        """).data()

        # Get product types linked to top companies
        edges = session.run("""
            MATCH (c:Company)-[:RECEIVED_COMPLAINT]->(comp:Complaint)
            WITH c, count(comp) AS complaint_count
            ORDER BY complaint_count DESC LIMIT 15
            MATCH (c)-[:RECEIVED_COMPLAINT]->(comp:Complaint)
            RETURN DISTINCT c.name AS company,
                            comp.product_type AS product
            LIMIT 60
        """).data()

    return render_template("knowledge_graph.html",
                           companies=companies,
                           edges=edges)


# -------------------------------------------------------
# ENHANCED ANALYTICS PAGE — replaces old analytics route
# Author: Akanksha Bhardwaj
# -------------------------------------------------------
@main.route("/analytics")
def analytics():
    driver = get_driver()
    with driver.session() as session:

        complaint_data = session.run("""
            MATCH (c:Company)-[:RECEIVED_COMPLAINT]->(comp:Complaint)
            RETURN c.name AS company, count(comp) AS count
            ORDER BY count DESC LIMIT 10
        """).data()

        penalty_data = session.run("""
            MATCH (c:Company)-[:HAS_VIOLATION]->(o:OshaViolation)
            WHERE o.penalty_amount > 0
            RETURN o.state AS state, sum(o.penalty_amount) AS total_penalty
            ORDER BY total_penalty DESC LIMIT 10
        """).data()

        product_data = session.run("""
            MATCH (c:Company)-[:RECEIVED_COMPLAINT]->(comp:Complaint)
            RETURN comp.product_type AS product, count(comp) AS count
            ORDER BY count DESC LIMIT 8
        """).data()

        # NEW: Risk score distribution
        risk_data = session.run("""
            MATCH (c:Company)-[:HAS_RISK_SCORE]->(rs:RiskScore)
            WHERE rs.composite_score > 0
            RETURN c.name AS company,
                   rs.composite_score AS score
            ORDER BY score DESC LIMIT 10
        """).data()

        # NEW: Complaints over time
        timeline_data = session.run("""
            MATCH (c:Company)-[:RECEIVED_COMPLAINT]->(comp:Complaint)
            WHERE comp.date_received IS NOT NULL
            RETURN comp.date_received.year AS year,
                   count(comp) AS count
            ORDER BY year ASC
        """).data()

    return render_template("analytics.html",
                           complaint_data=complaint_data,
                           penalty_data=penalty_data,
                           product_data=product_data,
                           risk_data=risk_data,
                           timeline_data=timeline_data)