# Petcare Analytics MLOps Pipeline

This is a hands-on project simulating an online pet consultation service. Data generated from the web application is ingested using `Apache Kafka`, then processed and transformed in an ELT workflow with `dbt`. Machine learning workflows are integrated using `MLflow` to track experiments and add predictive insights, creating a comprehensive analytics and MLOps pipeline.

## 🏗️ Architecture

```
┌─────────────┐
│   Web App   │──┐
└─────────────┘  │
                 │
                 ▼
         ┌──────────────┐
         │    Kafka     │
         │  (Streaming) │
         └──────────────┘
                 │
                 ▼
         ┌──────────────┐
         │  Consumer    │
         └──────────────┘
                 │
                 ▼
         ┌──────────────┐
         │  PostgreSQL  │
         │  (Warehouse) │
         └──────────────┘
                 │
        ┌────────┴────────┐
        │                 │
        ▼                 ▼
   ┌─────────┐      ┌──────────┐
   │   dbt   │      │  MLflow  │
   │(Analytics)│    │   (ML)   │
   └─────────┘      └──────────┘
```

## 📁 Project Structure

```text
petcare-data-pipeline/
├── airflow/dags/          # Airflow DAGs (dbt runs, ML predictions)
├── dbt/                    # dbt project (staging, intermediate, marts, features)
├── mlflow/                 # MLflow training notebook & artifacts
├── scripts/                # Kafka consumer scripts
├── webapp/                 # Web application (Kafka producer)
├── data/                   # Sample data files
└── docker-compose.yaml     # Infrastructure services
```

## 🚀 Quick Start

### Prerequisites

- Docker & Docker Compose
- Python 3.11+
- dbt-core
- MLflow (optional, for local ML training)

### Start Services

```bash
# Start all services (Postgres, Kafka, MLflow, Airflow, Webapp, Consumer)
docker-compose up -d

# Check service status
docker-compose ps
```

### Access Services

- **Airflow UI**: <http://localhost:8080> (admin/admin)
- **MLflow UI**: <http://localhost:5002>
- **Webapp**: <http://localhost:8000>

### Run dbt Transformations

```bash
cd dbt
dbt deps
dbt run
dbt test
```

### Train ML Model

The ML model training is done via the Jupyter notebook in `mlflow/churning_prediction.ipynb`. Access MLflow UI to track experiments and manage models.

## 📊 Data Pipeline

### Data Flow

1. **Streaming**: Webapp → Kafka → Consumer → `consultations_landing`
2. **Batch**: CSV files → PostgreSQL landing tables
3. **Staging**: Landing → Cleaned staging models (`stg_*`)
4. **Intermediate**: Join & enrich (`int_*`)
5. **Marts**: Business-ready tables (`mart_*`)
6. **Features**: ML-ready features (`features_*`)
7. **ML**: Features → MLflow → Churn predictions

### dbt Models

- **Staging**: `stg_consultations` (from Kafka stream)
- **Intermediate**: `int_pet_profiles`, `int_consultation_metrics`
- **Marts**: `mart_pet_demographics`, `mart_expert_performance`, `mart_owner_engagement`
- **Features**: `features_owner_activity`, `features_pet_info`, `features_churn_dataset`
- **Snapshots**: `snap_owners` (historical tracking)

## 🤖 Machine Learning

### Churn Prediction

- **Features**: Consultation frequency, recency, pet info, engagement metrics
- **Models**: Logistic Regression & XGBoost
- **Training**: Jupyter notebook in `mlflow/churning_prediction.ipynb`
- **Orchestration**: Airflow DAG `predict_user_churning` (weekly schedule)

## 🔄 Airflow DAGs

- **`update_data_models`**: Runs dbt transformations (weekly)
- **`predict_user_churning`**: Loads ML model and predicts churn (weekly)
- **`test_dbt`**: Tests dbt models

## 🐳 Docker Services

| Service        | Port  | Description              |
| -------------- | ----- | ------------------------ |
| postgres       | 5432  | PostgreSQL 16 database   |
| kafka          | 9092  | Apache Kafka broker      |
| mlflow         | 5002  | MLflow tracking server   |
| airflow        | 8080  | Apache Airflow webserver |
| webapp         | 8000  | Web application          |
| kafka-consumer | -     | Kafka consumer service   |

**Database**: `dbt_db` | **User**: `dbt_user` | **Password**: `dbt_pass`

## 📝 Environment Variables

- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `MLFLOW_TRACKING_URI` (default: `http://mlflow:5000`)
- `KAFKA_BOOTSTRAP_SERVERS` (default: `kafka-broker:29092`)

## 🧪 Testing

```bash
cd dbt
dbt test                    # Run all tests
dbt test --select stg_*     # Test specific models
```

## 📚 Documentation

```bash
cd dbt
dbt docs generate
dbt docs serve              # Access at <http://localhost:8080>
```

## 🔧 Development

### Adding New dbt Models

1. Create SQL file in appropriate folder (`staging/`, `intermediate/`, `marts/`, `features/`)
2. Add documentation to `schema.yml`
3. Test: `dbt run --select <model_name> && dbt test --select <model_name>`

### Adding New Airflow DAGs

Create Python file in `airflow/dags/` following the existing DAG patterns.

## 🎯 Key Features

- **Streaming Data**: Real-time consultation ingestion via Kafka
- **Batch Processing**: ETL with dbt for analytics
- **Feature Engineering**: ML-ready features from raw data
- **Model Training**: MLflow for experiment tracking
- **Orchestration**: Airflow for workflow automation
- **Data Quality**: dbt tests for validation

## 📄 License

MIT License
