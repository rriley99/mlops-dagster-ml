# Dagster Iris Classification Project

This project demonstrates a simple ML pipeline for the classic Iris dataset classification using Dagster. The pipeline includes assets for fetching, transforming, training, and evaluating a machine learning model.

## Project Structure

### Assets
- `get_data_to_s3`: Fetches the Iris dataset and uploads it to an S3 bucket as a CSV file.
- `split_data`: Splits the dataset into training and testing sets and uploads them to S3.
- `train_model`: Trains a Decision Tree classifier on the training set and uploads the trained model to S3.
- `evaluate_model`: Evaluates the trained model on the testing set and logs the accuracy score.

## Prerequisites

- Python 3.8 or higher
- Dagster and Dagit
- Access to an AWS S3 bucket
- Sklearn for model training and evaluation

## Setup

1. Install the required Python packages:
   ```bash
   pip install dagster dagit boto3 sklearn pandas
   ```
2. Run dagster
   ```bash
   dagster dev
   ```
3. View assets in Dagster UI 
   ```http://localhost:3000/```
4. Materialize and sample data from the Global Assets view.