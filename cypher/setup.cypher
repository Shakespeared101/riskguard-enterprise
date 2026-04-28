// 3A: Database Setup and Initial Data
// ============================================================
// RiskGuard Enterprise - Graph Database Setup
// Authors: Akanksha Bhardwaj, Srivatsav Vijayaraghavan, Shakthi Balasubramanian
// ============================================================

// ------------------------------------------------------------
// CONSTRAINTS & INDEXES
// Author: Akanksha Bhardwaj
// ------------------------------------------------------------

// Unique constraint on Company CIK
CREATE CONSTRAINT company_cik_unique IF NOT EXISTS
FOR (c:Company) REQUIRE c.cik IS UNIQUE;

// Unique constraint on Complaint ID
CREATE CONSTRAINT complaint_id_unique IF NOT EXISTS
FOR (comp:Complaint) REQUIRE comp.complaint_id IS UNIQUE;

// Unique constraint on RegulatoryBody acronym
CREATE CONSTRAINT agency_acronym_unique IF NOT EXISTS
FOR (r:RegulatoryBody) REQUIRE r.acronym IS UNIQUE;

// Index on Company name for fuzzy matching lookups
CREATE INDEX company_name_index IF NOT EXISTS
FOR (c:Company) ON (c.name);

// Index on Financial fiscal_year for time-series queries
CREATE INDEX financial_year_index IF NOT EXISTS
FOR (f:Financial) ON (f.fiscal_year);

// Index on Complaint date for trend analysis
CREATE INDEX complaint_date_index IF NOT EXISTS
FOR (comp:Complaint) ON (comp.date_received);

// Index on OshaViolation inspection_date
CREATE INDEX osha_date_index IF NOT EXISTS
FOR (o:OshaViolation) ON (o.inspection_date);

// ------------------------------------------------------------
// SEED REGULATORY BODIES
// Author: Srivatsav Vijayaraghavan
// ------------------------------------------------------------

MERGE (sec:RegulatoryBody {
  agency_id: "REG001",
  name: "U.S. Securities and Exchange Commission",
  acronym: "SEC",
  jurisdiction: "Federal",
  data_portal_url: "https://www.sec.gov/cgi-bin/browse-edgar"
});

MERGE (cfpb:RegulatoryBody {
  agency_id: "REG002",
  name: "Consumer Financial Protection Bureau",
  acronym: "CFPB",
  jurisdiction: "Federal",
  data_portal_url: "https  ://www.consumerfinance.gov/data-research/consumer-complaints"
});

MERGE (osha:RegulatoryBody {
  agency_id: "REG003",
  name: "Occupational Safety and Health Administration",
  acronym: "OSHA",
  jurisdiction: "Federal",
  data_portal_url: "https://www.osha.gov/pls/imis/establishment.html"
});

// ------------------------------------------------------------
// SAMPLE COMPANY NODE
// Author: Shakthi Balasubramanian
// ------------------------------------------------------------

MERGE (c:Company {
  cik: "0000051143",
  name: "JPMorgan Chase & Co.",
  ticker: "JPM",
  sic_code: "6022",
  state: "DE",
  fiscal_year_end: "12-31"
});

// ------------------------------------------------------------
// SAMPLE FINANCIAL NODE AND RELATIONSHIP
// Author: Akanksha Bhardwaj
// ------------------------------------------------------------

MATCH (c:Company {cik: "0000051143"})
MERGE (f:Financial {
  fiscal_year: 2022,
  total_assets: 3954687000000,
  total_liabilities: 3617498000000,
  shareholders_equity: 292329000000,
  revenue: 128695000000,
  net_income: 37676000000
})
MERGE (c)-[:HAS_FINANCIAL]->(f);

// ------------------------------------------------------------
// SAMPLE COMPLAINT NODE AND RELATIONSHIP
// Author: Srivatsav Vijayaraghavan
// ------------------------------------------------------------

MATCH (c:Company {cik: "0000051143"})
MERGE (comp:Complaint {
  complaint_id: "CFPB-2022-001",
  date_received: date("2022-03-15"),
  product_type: "Mortgage",
  issue_category: "Loan modification",
  company_response: "Closed with explanation",
  timely_response: true
})
MERGE (c)-[:RECEIVED_COMPLAINT]->(comp);

// ------------------------------------------------------------
// SAMPLE OSHA VIOLATION NODE AND RELATIONSHIP
// Author: Shakthi Balasubramanian
// ------------------------------------------------------------

MATCH (c:Company {cik: "0000051143"})
MERGE (o:OshaViolation {
  activity_number: "OSHA-2022-789",
  citation_id: "CIT-001",
  inspection_date: date("2022-06-10"),
  violation_type: "Serious",
  penalty_amount: 15000,
  establishment_name: "JPMorgan Operations Center",
  state: "OH",
  sic_code: "6022"
})
MERGE (c)-[:HAS_VIOLATION]->(o);

// ------------------------------------------------------------
// REGULATED_BY RELATIONSHIP (many-to-many with properties)
// Author: Akanksha Bhardwaj
// ------------------------------------------------------------

MATCH (c:Company {cik: "0000051143"})
MATCH (sec:RegulatoryBody {acronym: "SEC"})
MERGE (c)-[:REGULATED_BY {
  first_interaction_year: 1995,
  total_interactions: 120
}]->(sec);

MATCH (c:Company {cik: "0000051143"})
MATCH (cfpb:RegulatoryBody {acronym: "CFPB"})
MERGE (c)-[:REGULATED_BY {
  first_interaction_year: 2012,
  total_interactions: 430
}]->(cfpb);

// ------------------------------------------------------------
// SAMPLE RISK SCORE NODE AND RELATIONSHIP
// Author: Srivatsav Vijayaraghavan
// ------------------------------------------------------------

MATCH (c:Company {cik: "0000051143"})
MERGE (rs:RiskScore {
  fiscal_year: 2022,
  financial_risk_score: 62.5,
  complaint_risk_score: 74.3,
  safety_risk_score: 45.1,
  composite_score: 60.6,
  created_at: datetime()
})
MERGE (c)-[:HAS_RISK_SCORE]->(rs);
// 3B: CRUD Operations
// ============================================================
// CRUD OPERATIONS - RiskGuard Enterprise
// ============================================================

// ------------------------------------------------------------
// CREATE - Add a new company
// Author: Shakthi Balasubramanian
// ------------------------------------------------------------

CREATE (c:Company {
  cik: "0000200406",
  name: "Bank of America Corp.",
  ticker: "BAC",
  sic_code: "6022",
  state: "NC",
  fiscal_year_end: "12-31"
});

// ------------------------------------------------------------
// READ 1 - Research Question 1:
// Find companies with high debt ratio that also have
// high complaint volume over the past 3 years
// Author: Akanksha Bhardwaj
// ------------------------------------------------------------

MATCH (c:Company)-[:HAS_FINANCIAL]->(f:Financial)
MATCH (c)-[:RECEIVED_COMPLAINT]->(comp:Complaint)
WHERE f.fiscal_year >= 2020
  AND f.total_liabilities / f.total_assets > 0.85
  AND comp.date_received >= date("2020-01-01")
WITH c, f.fiscal_year AS year,
     f.total_liabilities / f.total_assets AS debt_ratio,
     COUNT(comp) AS complaint_count
RETURN c.name, year, debt_ratio, complaint_count
ORDER BY debt_ratio DESC, complaint_count DESC;

// ------------------------------------------------------------
// READ 2 - Research Question 2:
// Find companies with OSHA violations in year N
// and check if their financial health declined in year N+1
// Author: Srivatsav Vijayaraghavan
// ------------------------------------------------------------

MATCH (c:Company)-[:HAS_VIOLATION]->(o:OshaViolation)
MATCH (c)-[:HAS_FINANCIAL]->(f1:Financial)
MATCH (c)-[:HAS_FINANCIAL]->(f2:Financial)
WHERE o.inspection_date.year = f1.fiscal_year
  AND f2.fiscal_year = f1.fiscal_year + 1
WITH c,
     f1.fiscal_year AS violation_year,
     SUM(o.penalty_amount) AS total_penalties,
     f1.net_income AS income_year_n,
     f2.net_income AS income_year_n1
RETURN c.name,
       violation_year,
       total_penalties,
       income_year_n,
       income_year_n1,
       income_year_n1 - income_year_n AS income_change
ORDER BY total_penalties DESC;

// ------------------------------------------------------------
// READ 3 - Research Question 3:
// Sector-level complaint volume vs composite risk score
// Author: Akanksha Bhardwaj
// ------------------------------------------------------------

MATCH (c:Company)-[:RECEIVED_COMPLAINT]->(comp:Complaint)
MATCH (c)-[:HAS_RISK_SCORE]->(rs:RiskScore)
WITH c.sic_code AS sector,
     COUNT(comp) AS complaint_volume,
     AVG(rs.composite_score) AS avg_risk_score
RETURN sector, complaint_volume, avg_risk_score
ORDER BY avg_risk_score DESC;

// ------------------------------------------------------------
// UPDATE - Update composite risk score for a company
// Author: Shakthi Balasubramanian
// ------------------------------------------------------------

MATCH (c:Company {cik: "0000051143"})-[:HAS_RISK_SCORE]->(rs:RiskScore)
WHERE rs.fiscal_year = 2022
SET rs.composite_score = 65.8,
    rs.updated_at = datetime()
RETURN c.name, rs.fiscal_year, rs.composite_score;

// ------------------------------------------------------------
// UPDATE - Update total interactions on REGULATED_BY relationship
// Author: Srivatsav Vijayaraghavan
// ------------------------------------------------------------

MATCH (c:Company {cik: "0000051143"})-[r:REGULATED_BY]->(rb:RegulatoryBody {acronym: "CFPB"})
SET r.total_interactions = r.total_interactions + 1
RETURN c.name, rb.acronym, r.total_interactions;

// ------------------------------------------------------------
// DELETE - Remove a specific complaint record
// Author: Akanksha Bhardwaj
// ------------------------------------------------------------

MATCH (c:Company)-[rel:RECEIVED_COMPLAINT]->(comp:Complaint {complaint_id: "CFPB-2022-001"})
DELETE rel, comp;

// ------------------------------------------------------------
// DELETE - Remove a company and all its related nodes
// Author: Shakthi Balasubramanian
// Use with caution — cascades to all connected nodes
// ------------------------------------------------------------

MATCH (c:Company {cik: "0000200406"})
DETACH DELETE c;
// 3C: Analytical Queries
// ============================================================
// ANALYTICAL QUERIES - RiskGuard Enterprise
// ============================================================

// ------------------------------------------------------------
// ANALYTICAL QUERY 1 - Research Question 1:
// Do companies with high debt show a significant increase
// in regulatory fines or consumer complaints over 3 years?
// Author: Akanksha Bhardwaj
// ------------------------------------------------------------

MATCH (c:Company)-[:HAS_FINANCIAL]->(f:Financial)
MATCH (c)-[:RECEIVED_COMPLAINT]->(comp:Complaint)
MATCH (c)-[:HAS_VIOLATION]->(o:OshaViolation)
WHERE f.fiscal_year IN [2020, 2021, 2022]
  AND o.inspection_date.year = f.fiscal_year
WITH c,
     f.fiscal_year AS year,
     f.total_liabilities / f.total_assets AS debt_ratio,
     COUNT(DISTINCT comp) AS complaint_count,
     SUM(o.penalty_amount) AS total_fines
WHERE debt_ratio > 0.85
RETURN c.name,
       c.sic_code,
       year,
       ROUND(debt_ratio * 100, 2) AS debt_ratio_pct,
       complaint_count,
       total_fines
ORDER BY c.name, year ASC;

// ------------------------------------------------------------
// ANALYTICAL QUERY 1B - Year-over-year change version
// Shows whether complaints and fines INCREASED alongside debt
// Author: Akanksha Bhardwaj
// ------------------------------------------------------------

MATCH (c:Company)-[:HAS_FINANCIAL]->(f1:Financial)
MATCH (c)-[:HAS_FINANCIAL]->(f2:Financial)
MATCH (c)-[:RECEIVED_COMPLAINT]->(comp1:Complaint)
MATCH (c)-[:RECEIVED_COMPLAINT]->(comp2:Complaint)
WHERE f1.fiscal_year = 2020
  AND f2.fiscal_year = 2022
  AND comp1.date_received.year = 2020
  AND comp2.date_received.year = 2022
  AND f1.total_liabilities / f1.total_assets > 0.85
WITH c,
     COUNT(DISTINCT comp1) AS complaints_2020,
     COUNT(DISTINCT comp2) AS complaints_2022,
     f1.total_liabilities / f1.total_assets AS debt_ratio
RETURN c.name,
       ROUND(debt_ratio * 100, 2) AS debt_ratio_pct,
       complaints_2020,
       complaints_2022,
       complaints_2022 - complaints_2020 AS complaint_increase
ORDER BY complaint_increase DESC;


// ------------------------------------------------------------
// ANALYTICAL QUERY 2 - Research Question 2:
// Are OSHA violations predictive of declining financial
// health in the following fiscal year?
// Author: Srivatsav Vijayaraghavan
// ------------------------------------------------------------

MATCH (c:Company)-[:HAS_VIOLATION]->(o:OshaViolation)
MATCH (c)-[:HAS_FINANCIAL]->(f_current:Financial)
MATCH (c)-[:HAS_FINANCIAL]->(f_next:Financial)
WHERE o.inspection_date.year = f_current.fiscal_year
  AND f_next.fiscal_year = f_current.fiscal_year + 1
WITH c,
     f_current.fiscal_year AS violation_year,
     COUNT(DISTINCT o) AS violation_count,
     SUM(o.penalty_amount) AS total_penalties,
     f_current.net_income AS net_income_current,
     f_next.net_income AS net_income_next,
     f_current.total_liabilities / f_current.total_assets
       AS debt_ratio_current,
     f_next.total_liabilities / f_next.total_assets
       AS debt_ratio_next
RETURN c.name,
       c.sic_code,
       violation_year,
       violation_count,
       total_penalties,
       net_income_current,
       net_income_next,
       ROUND(net_income_next - net_income_current, 2)
         AS net_income_change,
       ROUND(debt_ratio_next - debt_ratio_current, 4)
         AS debt_ratio_change,
       CASE
         WHEN net_income_next < net_income_current
         THEN "Declined"
         ELSE "Stable or Improved"
       END AS financial_health_next_year
ORDER BY total_penalties DESC;


// ------------------------------------------------------------
// ANALYTICAL QUERY 3 - Research Question 3:
// Which sectors show the strongest correlation between
// complaint volume and composite risk score?
// Author: Shakthi Balasubramanian
// ------------------------------------------------------------

MATCH (c:Company)-[:RECEIVED_COMPLAINT]->(comp:Complaint)
MATCH (c)-[:HAS_RISK_SCORE]->(rs:RiskScore)
MATCH (c)-[:HAS_FINANCIAL]->(f:Financial)
WHERE rs.fiscal_year = f.fiscal_year
  AND comp.date_received.year = f.fiscal_year
WITH c.sic_code AS sector,
     rs.fiscal_year AS year,
     COUNT(DISTINCT comp) AS complaint_volume,
     AVG(rs.composite_score) AS avg_composite_score,
     AVG(rs.complaint_risk_score) AS avg_complaint_risk,
     AVG(rs.financial_risk_score) AS avg_financial_risk,
     COUNT(DISTINCT c) AS company_count
RETURN sector,
       year,
       company_count,
       complaint_volume,
       ROUND(avg_composite_score, 2) AS avg_composite_score,
       ROUND(avg_complaint_risk, 2) AS avg_complaint_risk,
       ROUND(avg_financial_risk, 2) AS avg_financial_risk
ORDER BY avg_composite_score DESC, complaint_volume DESC;

// ------------------------------------------------------------
// ANALYTICAL QUERY 3B - Top 5 highest risk sectors overall
// Aggregated across all years, for dashboard heatmap
// Author: Shakthi Balasubramanian
// ------------------------------------------------------------

MATCH (c:Company)-[:RECEIVED_COMPLAINT]->(comp:Complaint)
MATCH (c)-[:HAS_RISK_SCORE]->(rs:RiskScore)
WITH c.sic_code AS sector,
     COUNT(DISTINCT comp) AS total_complaints,
     AVG(rs.composite_score) AS avg_risk_score,
     COUNT(DISTINCT c) AS company_count
RETURN sector,
       company_count,
       total_complaints,
       ROUND(avg_risk_score, 2) AS avg_risk_score
ORDER BY avg_risk_score DESC
LIMIT 5;
