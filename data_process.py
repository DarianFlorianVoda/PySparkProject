from pyspark.sql.functions import col


def transform_data(data):
    tf_data = data
    tf_data = tf_data.groupBy("School_Unit_Name", "Gender").agg(
        {'Elementary_school_cases': 'mean', 'Middle_school_cases': 'mean', 'High_school_cases': 'mean'})
    tf_data = tf_data.withColumnRenamed("avg(Elementary_school_cases)", "Elementary_school_cases_Average_Day_1") \
        .withColumnRenamed("avg(High_school_cases)", "High_school_cases_Average_Day_1") \
        .withColumnRenamed("avg(Middle_school_cases)", "Middle_school_cases_Average_Day_1")

    tf_data.show()
    tf_data.printSchema()
    return tf_data


def transform_data_second(data):
    tf_data2 = data
    tf_data2 = tf_data2.groupBy("School_Unit_Name", "Gender").agg(
        {'Elementary_school_cases': 'mean', 'Middle_school_cases': 'mean', 'High_school_cases': 'mean'})
    tf_data2 = tf_data2.withColumnRenamed("avg(Elementary_school_cases)", "Elementary_school_cases_Average_Day_2") \
        .withColumnRenamed("avg(High_school_cases)", "High_school_cases_Average_Day_2") \
        .withColumnRenamed("avg(Middle_school_cases)", "Middle_school_cases_Average_Day_2")

    tf_data2.show()
    tf_data2.printSchema()
    return tf_data2


def join_dfs(df1, df2):
    joined_data = df1.join(df2, ['School_Unit_Name', 'Gender'], 'outer')
    joined_data = joined_data.na.fill(0)
    joined_data = joined_data.withColumn("Elementary_school_cases", (
            col("Elementary_school_cases_Average_Day_1") + col("Elementary_school_cases_Average_Day_2")) / 2) \
        .withColumn("Middle_school_cases",
                    (col("Middle_school_cases_Average_Day_1") + col("Middle_school_cases_Average_Day_2")) / 2) \
        .withColumn("High_school_cases",
                    (col("High_school_cases_Average_Day_1") + col("High_school_cases_Average_Day_2")) / 2)
    cols = ("Elementary_school_cases_Average_Day_1", "Elementary_school_cases_Average_Day_2",
            "Middle_school_cases_Average_Day_2", "Middle_school_cases_Average_Day_1", "High_school_cases_Average_Day_2",
            "High_school_cases_Average_Day_1")
    joined_data = joined_data.drop(*cols)
    return joined_data
