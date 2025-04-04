# FastAPI NoSQL & Neo4j Integration Project

This project demonstrates a FastAPI application that connects to both a MongoDB database (using Motor) and a Neo4j graph database. The API provides endpoints to retrieve and aggregate data from these databases. The application is containerized using Docker.

## Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Running the Application](#running-the-application)
  - [Local Development](#local-development)
  - [Using Docker](#using-docker)
- [API Endpoints](#api-endpoints)
- [Project Structure](#project-structure)
- [Troubleshooting](#troubleshooting)
- [License](#license)

## Features

- **MongoDB Integration:**  
  Aggregation pipelines to group employees, vendors, policies, and top claims by various criteria.
  
- **Neo4j Integration:**  
  Cypher queries to perform similar operations on graph data.
  
- **Containerized:**  
  Dockerfile provided to build and run the application in a lightweight Alpine-based container.

## Prerequisites

- **Python 3.13** or later.
- **MongoDB:** A running MongoDB instance (Atlas or local).
- **Neo4j:** A running Neo4j instance (you can use Aura or a local Neo4j database).
- **Docker:** For containerized deployment (optional).

## Installation
**Clone the repository:**

 ```bash
 git clone <repository-url>
 cd FastAPIProject
 python -m venv .venv
 source .venv/bin/activate  # On Windows use .venv\Scripts\activate
 pip install --upgrade pip
 pip install -r requirements.txt
 uvicorn main:app --reload
 docker build -t fastapi-app .
 docker run --name dw-nosql -d -p 8000:8000 fastapi-app
 ```
## API Endpoints

### MongoDB Endpoints

- GET /mongo/employees_by_company : Groups employees by insurance company (based on AGENT_ID and joined with the insurance collection) and returns up to 20 records.

- GET /mongo/vendors_by_company : Joins the insurance collection with vendor documents and groups vendors by insurance company.

- GET /mongo/policies_by_company_and_type : Groups insurance policies by company and policy type.

- GET /mongo/top_5_claims : Retrieves the top 5 claims by amount for each company and policy category.

### Neo4j Endpoints
- GET /ne04j/employees_by_company : Groups employees by insurance company using Neo4j and returns a list of employee details.
- GET /ne04j/vendors_by_company : Joins employees, insurance, and vendor nodes to return vendors associated with each employee’s insurance.
- GET /ne04j/policies_by_company_and_type : Groups insurance nodes by company and policy type.
- GET /ne04j/top_5_claims : Retrieves the top 5 claims by amount for each (company, policy type) pair.

## File structure
  ```bash
  FastAPIProject/
  ├── main.py             # Main FastAPI application with MongoDB and Neo4j endpoints
  ├── requirements.txt    # List of required Python packages
  ├── Dockerfile          # Dockerfile for containerizing the application
  └── README.md           # This file
  ```
## License
This project is licensed under the MIT License.
