from airflow.sdk import task
import mlflow
import os
from airflow import DAG
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Set MLflow tracking URI (can be overridden by environment variable)
mlflow_tracking_uri = os.getenv('MLFLOW_TRACKING_URI', 'http://mlflow:5000')
mlflow.set_tracking_uri(mlflow_tracking_uri)

def load_latest_churn_model(model_name="ChurnPredictionModel", alias="champion"):
    try:
        model_uri = f"models:/{model_name}@{alias}"
        model = mlflow.sklearn.load_model(model_uri)
        return model
    except OSError as os_err:
        print(os_err)
        raise

with DAG(
    dag_id='predict_user_churning',
    default_args=default_args,
    description='A simple DAG to predict user churning',
    schedule='@weekly',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['ml'],
) as dag:
    @task
    def check_owner_churning():
        DB_CONFIG = {
            "host": "postgres",
            "port": 5432,
            "database": "dbt_db",
            "user": "dbt_user",
            "password": "dbt_pass"
        }

        # Get the dataset from the database
        engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
        df = pd.read_sql_query("""
                            SELECT o.avg_consultation_duration, o.total_consultation_minutes, o.consultations_per_month, o.avg_minutes_per_consultation, o.num_pets, p.avg_pet_age, p.num_unique_breeds
                            FROM petcare_features.features_owner_activity o 
                            JOIN petcare_features.features_pet_info p
                            ON o.owner_id = p.owner_id
                            """, engine)
        df = df[['avg_consultation_duration', 'total_consultation_minutes', 'consultations_per_month', 'avg_minutes_per_consultation', 'num_pets', 'avg_pet_age', 'num_unique_breeds']]
        df = df.fillna(0)
        
        model = load_latest_churn_model()

        # Predict on entire DataFrame at once (more efficient and preserves column names)
        predictions = model.predict(df)
        
        # Add predictions to the dataframe
        df['churn_prediction'] = predictions
        
        # Print predictions
        for index, prediction in enumerate(predictions):
            print(f"Owner index {index}: churn_prediction = {prediction}")

        # df.to_csv("churned_owners.csv", index=False)
        return df


    check_owner_churning()

