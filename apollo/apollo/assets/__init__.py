from sklearn import datasets, tree
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from dagster import asset, MaterializeResult, AssetKey, EnvVar
import pandas as pd
import pickle
import io

# s3 vars
target_bucket = EnvVar("TARGET_BUCKET")
raw_target_key = EnvVar("RAW_TARGET_KEY")
transformed_target_path = EnvVar("TRANSFORMED_TARGET_PATH")
model_key = EnvVar("MODEL_KEY")


# reused functions
def write_to_s3(client, bucket, key, data):
    """
    Function to write a dataframe to s3 as a csv.

    Args:
        client: Boto3 client (derived from dagster).
        bucket: Bucket to store the csv.
        key: S3 Key to store the csv.
        data: DataFrame that is to be written to S3.

    Returns:
        None
    """
    buffer = io.StringIO()
    data.to_csv(buffer, index=False)
    buffer.seek(0)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue()
    )


def pickle_model_to_s3(client, bucket, key, model):
    """
    Function to pickle a model and store in s3

    Args:
        client: Boto3 client (derived from dagster)
        bucket: Bucket to store the pickled model
        key: S3 Key to store the pickled model
        model: the model you want to pick before serialization

    Returns:
        None
    """
    buffer = io.BytesIO()
    pickle.dump(model, buffer)
    buffer.seek(0)
    client.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer
    )


def get_metadata(instance, asset_name, key):
    """
    Function to receive metadata from an asset in the execution context

    Args:
        instance: Dagster instance to reference
        asset_name: Name of the asset that you are fetching metadata from
        key: The metadata key of the metadata you are attempting to fetch

    Returns:
        metadata: text of the fetched metadata
    """
    return instance.get_latest_materialization_event(
        AssetKey([asset_name])
    ).asset_materialization.metadata[key].text


@asset(
    group_name="raw_data",
    required_resource_keys={'dev_s3'}
)
def get_data_to_s3(context):
    """
    Asset to retrieve the Iris dataset and upload it as a CSV to S3.

    Args:
        context: Dagster context for accessing resources and logging.

    Returns:
        MaterializeResult with metadata containing the S3 path of the uploaded data.
    """
    # named dict from sk-learn
    iris = datasets.load_iris()

    # the dataset in df form
    features = pd.DataFrame.from_dict(iris.data)
    labels = pd.DataFrame.from_dict(iris.target)
    data = pd.concat([features.reset_index(drop=True), labels.reset_index(drop=True)], axis=1)

    # dagster s3 components
    s3_client = context.resources.dev_s3

    # write file to memory
    csv_buffer = io.StringIO()
    data.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)

    # write to s3
    write_to_s3(s3_client, target_bucket, raw_target_key, data)

    return MaterializeResult(
        metadata={
            's3_path': f"s3://{target_bucket}/{raw_target_key}"
        }
    )


@asset(
    group_name="transformed_data",
    required_resource_keys={'dev_s3'},
    deps={'get_data_to_s3'}
)
def split_data(context):
    """
    Asset to split the Iris dataset into training and testing sets and upload them to S3.

    Args:
        context: Dagster context for accessing resources and logging.

    Returns:
        MaterializeResult with metadata containing the S3 paths of the transformed data sets.
    """
    # dagster s3 components
    s3_client = context.resources.dev_s3

    # fetch s3 path for raw features from previous tasks metadata
    raw_features_s3_path = get_metadata(context.instance, 'get_data_to_s3', 's3_path')
    context.log.info(f"pulling file from {raw_features_s3_path}")

    # fetch data from s3
    response = s3_client.get_object(Bucket=target_bucket, Key=raw_features_s3_path.split(f's3://{target_bucket}/')[1])
    raw_data = pd.read_csv(io.BytesIO(response['Body'].read()))

    # split data
    x = raw_data.iloc[:, :3]
    y = raw_data.iloc[:, 4:]
    context.log.info("Splitting data")
    x_train, x_test, y_train, y_test = train_test_split(x, y, test_size=.3)

    # Write the transformed data sets back to S3
    x_train_key = transformed_target_path + 'iris_feature_train.csv'
    context.log.info(f"x_train.head {x_train.head()}")
    write_to_s3(s3_client, target_bucket, x_train_key, x_train)

    x_test_key = transformed_target_path + 'iris_feature_test.csv'
    write_to_s3(s3_client, target_bucket, x_test_key, x_test)
    context.log.info(f"x_test.head {x_test.head()}")

    y_train_key = transformed_target_path + 'iris_label_train.csv'
    write_to_s3(s3_client, target_bucket, y_train_key, y_train)
    context.log.info(f"y_train.head {y_train.head()}")

    y_test_key = transformed_target_path + 'iris_label_test.csv'
    write_to_s3(s3_client, target_bucket, y_test_key, y_test)
    context.log.info(f"y_test.head {y_test.head()}")

    # Return metadata about the transformed data path
    return MaterializeResult(
        metadata={
            'iris_feature_train': f"s3://{target_bucket}/{x_train_key}",
            'iris_feature_test': f"s3://{target_bucket}/{x_test_key}",
            'iris_label_train': f"s3://{target_bucket}/{y_train_key}",
            'iris_label_test': f"s3://{target_bucket}/{y_test_key}"
        }
    )


@asset(
    group_name="train",
    required_resource_keys={'dev_s3'},
    deps={'split_data'}
)
def train_model(context):
    """
    Asset to train a Decision Tree classifier on the Iris training dataset.

    Args:
        context: Dagster context for accessing resources and logging.

    Returns:
        MaterializeResult with metadata containing the S3 path of the pickled model.
    """
    # dagster s3 components
    s3_client = context.resources.dev_s3

    # fetch s3 path for features from previous tasks metadata
    features_s3_path = get_metadata(context.instance, 'split_data', 'iris_feature_train')
    context.log.info(f"pulling file from {features_s3_path}")
    # fetch data from s3
    response = s3_client.get_object(Bucket=target_bucket, Key=features_s3_path.split(f's3://{target_bucket}/')[1])
    iris_feature_train = pd.read_csv(io.BytesIO(response['Body'].read()))

    # fetch s3 path for raw labels from previous tasks metadata
    labels_s3_path = get_metadata(context.instance, 'split_data', 'iris_label_train')
    context.log.info(f"pulling file from {labels_s3_path}")
    # fetch data from s3
    response = s3_client.get_object(Bucket=target_bucket, Key=labels_s3_path.split(f's3://{target_bucket}/')[1])
    iris_feature_labels = pd.read_csv(io.BytesIO(response['Body'].read()))

    # Train model
    context.log.info('training model')
    classifier = tree.DecisionTreeClassifier()
    classifier.fit(iris_feature_train, iris_feature_labels)

    # pickle and write to s3
    context.log.info('pickling model')
    pickle_model_to_s3(s3_client, target_bucket, model_key, classifier)

    return MaterializeResult(
        metadata={
            's3_path': f"s3://{target_bucket}/{model_key}"
        }
    )


@asset(
    group_name="eval",
    required_resource_keys={'dev_s3'},
    deps={'split_data', 'train_model'}
)
def evaluate_model(context):
    """
    Asset to evaluate the trained Decision Tree classifier on the Iris testing dataset.

    Args:
        context: Dagster context for accessing resources and logging.

    Returns:
        MaterializeResult with metadata containing the accuracy score of the model.
    """
    # dagster s3 components
    s3_client = context.resources.dev_s3

    # fetch s3 path for model pickle from previous tasks metadata
    model_s3_path = get_metadata(context.instance, 'train_model', 's3_path')
    context.log.info(f"pulling file from {model_s3_path}")
    model_bytes = s3_client.get_object(
        Bucket=target_bucket, Key=model_s3_path.split(f's3://{target_bucket}/')[1]
    )['Body'].read()

    # fetch data from s3 for test features
    features_s3_path = get_metadata(context.instance, 'split_data', 'iris_feature_test')
    context.log.info(f"pulling file from {features_s3_path}")
    response = s3_client.get_object(Bucket=target_bucket, Key=features_s3_path.split(f's3://{target_bucket}/')[1])
    iris_feature_test = pd.read_csv(io.BytesIO(response['Body'].read()))

    # fetch data from s3 for test labels
    labels_s3_path = get_metadata(context.instance, 'split_data', 'iris_label_test')
    context.log.info(f"pulling file from {labels_s3_path}")
    response = s3_client.get_object(Bucket=target_bucket, Key=labels_s3_path.split(f's3://{target_bucket}/')[1])
    iris_label_test = pd.read_csv(io.BytesIO(response['Body'].read()))

    # Test model
    model = pickle.load(io.BytesIO(model_bytes))
    context.log.info(f"iris_feature_test: {iris_feature_test}")
    predictions = model.predict(iris_feature_test)

    # Score model
    context.log.info('scoring model')
    score = accuracy_score(iris_label_test, predictions)
    context.log.info(f"accuracy_score: {score}")

    return MaterializeResult(
        metadata={
            'accuracy': score
        }
    )

