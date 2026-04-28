# RiskGuard Enterprise

A corporate risk analysis dashboard integrating CFPB consumer complaints,
OSHA workplace violations, and SEC financial data using Neo4j and Flask.

## Team
- Akanksha Bhardwaj
- Srivatsav Vijayaraghavan
- Shakthi Balasubramanian

## Tech Stack
- Database: Neo4j (graph database)
- Backend: Python 3, Flask
- Frontend: HTML, CSS, Bootstrap 5, Chart.js, vis.js
- Data: CFPB Complaints, OSHA Violations, SEC EDGAR

## Setup Instructions

### 1. Install dependencies
pip install -r requirements.txt

### 2. Configure environment
Create a .env file:
NEO4J_URI=bolt://localhost:7687
NEO4J_USER=neo4j
NEO4J_PASSWORD=your_password

### 3. Start Neo4j Desktop
- Open Neo4j Desktop
- Start the riskguard-db database

### 4. Load data
python scripts/load_data.py
python scripts/fix_osha_manual.py

### 5. Run risk score computation
Open Neo4j Browser and run cypher/setup.cypher

### 6. Start the app
python run.py
Open http://127.0.0.1:5000

## Features
- Dashboard with live node counts and recent complaints
- Company directory with complaint and violation counts
- Company detail page with complaints and OSHA violations
- Risk profile page with composite score and contributing factors
- Company comparison chart
- Interactive knowledge graph (vis.js)
- Analytics dashboard with 5 charts

## Note on Hosting
This application runs locally only. Neo4j Desktop does not support
free cloud hosting without a paid AuraDB subscription.

## AI Assistance
Portions of this project were developed with assistance from
Claude (Anthropic, claude.ai), accessed April 2026, for schema
design, query templates, and Flask scaffolding. All code was
reviewed and adapted manually by the team.