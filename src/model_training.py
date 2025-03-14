import pandas as pd
import os
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
from sklearn.linear_model import LogisticRegression
from datetime import datetime
import pytz
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, classification_report
import mlflow
import joblib

def load_train_and_test_data():
    df_traning = pd.read_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_traning_feature_data.csv')
    y_train = df_traning["Churn"]
    X_train = df_traning.drop(columns=["Churn", "CustomerID"])
   
    df_testing = pd.read_csv('/opt/airflow/storage/processed/staging/customer_churn_dataset_traning_feature_data.csv')
    y_test = df_testing["Churn"]
    X_test = df_testing.drop(columns=["Churn", "CustomerID"])
    # Split data
    #X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    return X_train, X_test, y_train, y_test

def train_model(X_train, X_test, y_train, y_test):
    """Train and evaluate machine learning models."""
 

    models = {
        "Logistic Regression": LogisticRegression(),
        "Random Forest": RandomForestClassifier(n_estimators=100)
    }

    best_model = None
    best_score = 0
    print("mlflow start 1 ")
    for model_name, model in models.items():
        with mlflow.start_run():
            model.fit(X_train, y_train)
            y_pred = model.predict(X_test)
            print("mlflow start 2 ")
            acc = accuracy_score(y_test, y_pred)
            prec = precision_score(y_test, y_pred)
            rec = recall_score(y_test, y_pred)
            f1 = f1_score(y_test, y_pred)
            mlflow.sklearn.log_model(model, model_name)
            mlflow.log_param("model", model_name)
            mlflow.log_metric("accuracy", acc)
            mlflow.log_metric("precision", prec)
            mlflow.log_metric("recall", rec)
            mlflow.log_metric("f1_score", f1)

            #logging.info(f"{model_name}: Accuracy={acc}, Precision={prec}, Recall={rec}, F1 Score={f1}")

            if f1 > best_score:
                best_model = model
                best_score = f1
    print("mlflow end 2 ")
    mlflow.end_run()
    return best_model

def save_model(model):
    """Save the best model and scaler."""
    mlflow.set_tracking_uri("http://mlflow:5000")
    MODEL_DIR = "/opt/airflow/storage/model"
    os.makedirs(MODEL_DIR, exist_ok=True)
    timestamp = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%m%d_%H%M%S')
    model_path = os.path.join(MODEL_DIR, f"best_model_{timestamp}.pkl")
    #scaler_path = os.path.join(MODEL_DIR, f"scaler_{timestamp}.pkl")
    
    joblib.dump(model, model_path)
    #joblib.dump(scaler, scaler_path)

    #logging.info(f"Model saved at: {model_path}")
    #logging.info(f"Scaler saved at: {scaler_path}")

    return model_path  
  
def build_and_train_model():
    # Load feature store
    X_train, X_test, y_train, y_test = load_train_and_test_data()
    best_model = train_model(X_train, X_test, y_train, y_test)
    save_model(best_model)
    print("Model trained and logged to MLflow!")
