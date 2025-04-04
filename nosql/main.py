import neo4j
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Any, List, Dict
from fastapi.encoders import jsonable_encoder
from bson import ObjectId
from neo4j import GraphDatabase
app = FastAPI()

MONGO_DETAILS = "mongodb+srv://xxxxx:xxxxxx@hiruzen.yzkuzxj.mongodb.net/?appName=Hiruzen"
client = AsyncIOMotorClient(MONGO_DETAILS)
db = client["insurance"]

employee_collection = db["employee"]
vendor_collection = db["vendor"]
insurance_collection = db["insurance"]
@app.get("/mongo/employees_by_company", response_model=List[dict])
async def employees_by_company() -> List[dict]:
    """
    Groups employees by AGENT_ID (treating AGENT_ID as the insurance company).
    Returns a list of JSON objects, where each object has:
      company: the insurance company (derived from AGENT_ID)
      employees: array of employee documents
    """
    pipeline = [
        {
            "$group": {
                "_id": "$AGENT_ID",
                "employees": {"$push": "$$ROOT"}
            }
        },
        {
            "$project": {
                "_id": 0,
                "company": "$_id",
                "employees": 1
            }
        },
        {"$limit": 20}
    ]
    results = employee_collection.aggregate(pipeline)
    raw_data = [r async for r in results]
    return jsonable_encoder(raw_data, custom_encoder={ObjectId: str})

@app.get("/mongo/vendors_by_company")
async def vendors_by_company() -> List[Any]:
    """
    Joins the insurance collection with vendor documents by VENDOR_ID,
    then groups vendors by AGENT_ID.
    """
    pipeline = [
        {
            "$lookup": {
                "from": "vendor",
                "localField": "VENDOR_ID",
                "foreignField": "VENDOR_ID",
                "as": "vendor_info"
            }
        },
        {
            "$group": {
                "_id": "$AGENT_ID",
                "vendors": {"$addToSet": "$vendor_info"}
            }
        },
        {"$limit": 20}
    ]
    results = insurance_collection.aggregate(pipeline)
    return jsonable_encoder([r async for r in results], custom_encoder={ObjectId: str})

@app.get("/mongo/policies_by_company_and_type")
async def policies_by_company_and_type() -> List[Any]:
    """
    Groups policies in the insurance collection by:
      - AGENT_ID (the insurance company)
      - INSURANCE_TYPE (the category)
    Returns a list of documents with:
      _id: { company: <AGENT_ID>, insurance_type: <INSURANCE_TYPE> }
      policies: array of the matched policy documents
    """
    pipeline = [
        {
            "$group": {
                "_id": {
                    "company": "$AGENT_ID",
                    "insurance_type": "$INSURANCE_TYPE"
                },
                "policies": {"$push": "$$ROOT"}
            }
        },
        {"$limit": 20}
    ]
    results = insurance_collection.aggregate(pipeline)
    return jsonable_encoder([r async for r in results], custom_encoder={ObjectId: str})

@app.get("/mongo/top_5_claims")
async def top_5_claims_by_company_and_category() -> List[Any]:
    """
    For each (AGENT_ID, INSURANCE_TYPE) pair, find the top 5 claims by CLAIM_AMOUNT.
    """
    pipeline = [
        {
            "$group": {
                "_id": {
                    "company": "$AGENT_ID",
                    "insurance_type": "$INSURANCE_TYPE"
                },
                "claims": {
                    "$push": {
                        "doc": "$$ROOT",
                        "amount": "$CLAIM_AMOUNT"
                    }
                }
            }
        },
        {"$unwind": "$claims"},
        {"$sort": {"claims.amount": -1}},
        {
            "$group": {
                "_id": "$_id",
                "top_claims": {"$push": "$claims.doc"}
            }
        },
        {
            "$project": {
                "top_claims": {"$slice": ["$top_claims", 5]}
            }
        }
    ]
    results = insurance_collection.aggregate(pipeline)
    return jsonable_encoder([r async for r in results], custom_encoder={ObjectId: str})


NEO4J_URI = "neo4j+s://72386924.databases.neo4j.io"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "xxxxxxxxxxx"

driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))

@app.get("/ne04j/employees_by_company", response_model=List[Dict[str, Any]])
async def employees_by_company_neo4j():
    """
    Groups employees by insurance company (AGENT_ID).
    Returns a list of dicts:
      [
        {
          "company": <AGENT_ID>,
          "employees": [ <properties from each Employee node> ]
        }, ...
      ]
    """

    cypher_query = """
    MATCH (e:Employee)
    RETURN e.agentId AS company, collect(e) AS employees
    LIMIT 20
    """

    with driver.session() as session:
        result = session.run(cypher_query)
        data = []
        for record in result:
            data.append({
                "company": record["company"],
                "employees": [
                    emp_node._properties for emp_node in record["employees"]
                ]
            })
        return jsonable_encoder(data, custom_encoder={neo4j.time.DateTime: lambda dt: dt.iso_format()})

@app.get("/ne04j/vendors_by_company", response_model=List[Dict[str, Any]])
async def vendors_by_company_neo4j():
    """
    Joins the insurance company with vendor nodes, grouped by AGENT_ID.
    Returns a list of dicts:
      [
        {
          "company": <AGENT_ID>,
          "vendors": [ <properties from each Vendor node> ]
        }, ...
      ]
    """

    cypher_query = """
    MATCH (e:Employee)<-[:HAS_EMPLOYEE]-(i:Insurance)-[:HAS_VENDOR]->(v:Vendor) 
    RETURN e.agentId AS agent, e.agentName AS employeeName, collect(DISTINCT v) AS vendors
    LIMIT 20
    """

    with driver.session() as session:
        result = session.run(cypher_query)
        data = []
        for record in result:
            data.append({
                "agent": record["agent"],
                "vendors": [
                    vendor_node._properties for vendor_node in record["vendors"]
                ]
            })
        return jsonable_encoder(data, custom_encoder={neo4j.time.DateTime: lambda dt: dt.iso_format()})


@app.get("/ne04j/policies_by_company_and_type", response_model=List[Dict[str, Any]])
async def policies_by_company_and_type_neo4j():
    """
    Groups insurance nodes by (AGENT_ID, INSURANCE_TYPE).
    Returns a list of dicts:
      [
        {
          "company": <AGENT_ID>,
          "insurance_type": <INSURANCE_TYPE>,
          "policies": [ <all matching Insurance nodes> ]
        }, ...
      ]
    """

    cypher_query = """
    MATCH (p:Insurance)-[:HAS_EMPLOYEE]->(e:Employee)
    WITH e.agentId AS company, p.insuranceType AS category, collect(p) AS policies
    RETURN company, category, policies
    LIMIT 20
    """

    with driver.session() as session:
        result = session.run(cypher_query)
        data = []
        for record in result:
            data.append({
                "company": record["company"],
                "insurance_type": record["category"],
                "policies": [pol_node._properties for pol_node in record["policies"]]
            })
        return jsonable_encoder(data, custom_encoder={neo4j.time.DateTime: lambda dt: dt.iso_format()})

@app.get("/ne04j/top_5_claims", response_model=List[Dict[str, Any]])
async def top_5_claims_by_company_and_category_neo4j():
    """
    For each (AGENT_ID, INSURANCE_TYPE) pair, find the top 5 claims by CLAIM_AMOUNT.
    """

    cypher_query = """
    MATCH (p:Insurance)-[:HAS_EMPLOYEE]->(e:Employee)
    WHERE p.claimAmount IS NOT NULL
    WITH e.agentId AS company, p.insuranceType AS category, p ORDER BY p.claimAmount DESC
    WITH company, category, collect(p) AS claims
    RETURN company, category, claims[0..5] AS top_claims
    """

    with driver.session() as session:
        result = session.run(cypher_query)
        data = []
        for record in result:
            data.append({
                "company": record["company"],
                "insurance_type": record["category"],
                "top_claims": [
                    claim_node._properties for claim_node in record["top_claims"]
                ]
            })
        return jsonable_encoder(data, custom_encoder={neo4j.time.DateTime: lambda dt: dt.iso_format()})