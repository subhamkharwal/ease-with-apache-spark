import pytest
from common import remove_extra_spaces, filter_senior_citizen

# Test Case 1 - Remove Single Space
def test_single_space(spark_session):
    sample_data = [{"name": "John    D.", "age": 30},
                   {"name": "Alice   G.", "age": 25},
                   {"name": "Bob  T.", "age": 35},
                   {"name": "Eve   A.", "age": 28}]

    # Create a Spark DataFrame
    original_df = spark_session.createDataFrame(sample_data)

    # Apply the transformation function from before
    transformed_df = remove_extra_spaces(original_df, "name")

    expected_data = [{"name": "John D.", "age": 30},
    {"name": "Alice G.", "age": 25},
    {"name": "Bob T.", "age": 35},
    {"name": "Eve A.", "age": 28}]

    expected_df = spark_session.createDataFrame(expected_data)

    assert transformed_df.collect() == expected_df.collect()
    
# Test Case 2 - Row count    
def test_row_count(spark_session):
    sample_data = [{"name": "John    D.", "age": 30},
                   {"name": "Alice   G.", "age": 25},
                   {"name": "Bob  T.", "age": 35},
                   {"name": "Eve   A.", "age": 28}]

    # Create a Spark DataFrame
    original_df = spark_session.createDataFrame(sample_data)

    # Apply the transformation function from before
    transformed_df = remove_extra_spaces(original_df, "name")

    expected_data = [{"name": "John D.", "age": 30},
    {"name": "Alice G.", "age": 25},
    {"name": "Bob T.", "age": 35},
    {"name": "Eve A.", "age": 28}]

    expected_df = spark_session.createDataFrame(expected_data)
    print(expected_df.count())

    assert transformed_df.count() == expected_df.count()
    
# Test Case 3 - Senior Citizen count   
def test_senior_citizen_count(spark_session):
    sample_data = [{"name": "John D.", "age": 60},
                   {"name": "Alice G.", "age": 25},
                   {"name": "Bob T.", "age": 65},
                   {"name": "Eve A.", "age": 28}]

    # Create a Spark DataFrame
    original_df = spark_session.createDataFrame(sample_data)

    # Apply the filter function from before
    filtered_df = filter_senior_citizen(original_df, "age")

    expected_data = [{"name": "John D.", "age": 60},
                     {"name": "Bob T.", "age": 65}]

    expected_df = spark_session.createDataFrame(expected_data)
    print(expected_df.count())

    assert filtered_df.count() == expected_df.count()

# Test Case 4 - Senior Citizen count Negetive case  
def test_senior_citizen_count_negative(spark_session):
    sample_data = [{"name": "John D.", "age": 60},
                   {"name": "Alice G.", "age": 25},
                   {"name": "Bob T.", "age": 65},
                   {"name": "Eve A.", "age": 66}]

    # Create a Spark DataFrame
    original_df = spark_session.createDataFrame(sample_data)

    # Apply the filter function from before
    filtered_df = filter_senior_citizen(original_df, "age")

    expected_data = [{"name": "John D.", "age": 60},
                     {"name": "Bob T.", "age": 65}]

    expected_df = spark_session.createDataFrame(expected_data)
    print(expected_df.count())

    assert filtered_df.count() == expected_df.count()
    
