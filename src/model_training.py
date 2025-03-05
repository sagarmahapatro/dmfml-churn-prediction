import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import mlflow
import mlflow.sklearn

def build_and_train_model():
    # Load feature store
    df = pd.read_csv('/opt/airflow/src/feature_store.csv')
    X = df.drop(columns=["Churn", "customerID"])
    y = df["Churn"]

    # Train-test split
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Train the model
    model = RandomForestClassifier(n_estimators=100, random_state=42)
    model.fit(X_train, y_train)

    # Evaluate the model
    y_pred = model.predict(X_test)
    print(classification_report(y_test, y_pred))

    # Log the model to MLflow
    mlflow.start_run()
    mlflow.sklearn.log_model(model, "churn_model")
    mlflow.end_run()
    print("Model trained and logged to MLflow!")
