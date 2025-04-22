flowchart TB
  %% styling
  classDef source    fill:#3182ce,stroke:#2c5282,color:#fff,stroke-width:2px;
  classDef storage   fill:#38a169,stroke:#276749,color:#fff,stroke-width:2px;
  classDef etl       fill:#805ad5,stroke:#553c9a,color:#fff,stroke-width:2px;
  classDef warehouse fill:#2b6cb0,stroke:#2c5282,color:#fff,stroke-width:2px;
  classDef eda       fill:#dd6b20,stroke:#9c4221,color:#fff,stroke-width:2px;
  classDef viz       fill:#319795,stroke:#2c7a7b,color:#fff,stroke-width:2px;

  subgraph "🏁 Data Ingestion"
    direction TB
    PhysioNet["📡 PhysioNet<br/>MIMIC‑IV CSVs"]:::source
    S3Raw["☁️ AWS S3 (Raw)<br/>CSV Layer"]:::storage
    PhysioNet -->|Download| S3Raw
  end

  subgraph "🛠️ ETL Pipeline"
    direction TB
    Clean["🧹 Data Cleaning & Transform<br/>(PySpark)"]:::etl
    Parq["📦 Parquet Landing<br/>S3 Columnar"]:::storage
    GlueJob["🔧 AWS Glue Job<br/>Crawl · Transform · Load"]:::etl
    S3Raw --> Clean --> Parq --> GlueJob
  end

  subgraph "🏛️ Data Warehouse"
    direction TB
    Redshift["🌐 Amazon Redshift<br/>Star Schema"]:::warehouse
    GlueJob -->|COPY & LOAD| Redshift
  end

  subgraph "🔎 Analytics & Dashboards"
    direction TB
    EDA["🔍 Exploratory Analysis"]:::eda
    Dash["📊 Dashboards<br/>Matplotlib · Seaborn"]:::viz
    Redshift --> EDA --> Dash
  end
