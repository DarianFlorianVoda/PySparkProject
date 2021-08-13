import data_process
import dbactions
import output
import start_session

spark = start_session.session()
data = output.dfprint(spark)
tf_data = data_process.transform_data(data)
dbactions.write_table(tf_data)

data_second = output.dfprint_second(spark)
tf_data_second = data_process.transform_data_second(data_second)
dbactions.write_table_second(tf_data_second)

first_df = dbactions.read_table(spark)
joined_table = data_process.join_dfs(first_df, tf_data_second)
joined_table.show()
joined_table.printSchema()
joined_table.filter(joined_table.School_Unit_Name == "SCOALA VIETI FARA NR. FERENTARI").show()



dbactions.write_table_second(joined_table)
