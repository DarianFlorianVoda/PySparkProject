def write_table(df):
    jdbcHostname = '127.0.0.1'
    jdbcDatabase = 'ibm'
    jdbcPort = 3306
    jdbcUsername = 'root'
    jdbcPassword = 'admin'
    jdbcUrl = 'jdbc:mysql://{0}:{1}/{2}'.format(jdbcHostname, jdbcPort, jdbcDatabase)

    # jdbc: mysql: // 127.0.0.1: 3306 /?user = root

    connectionProperties = {
        'user': jdbcUsername,
        'password:': jdbcPassword,
        'driver': 'com.mysql.cj.jdbc.Driver'
    }

    # VERSION 2 - WORKING
    df.write.format('jdbc').option("truncate","false").options( \
        url=jdbcUrl, \
        driver="com.mysql.cj.jdbc.Driver", \
        dbtable="ibm.schoolcases", \
        user=jdbcUsername, \
        password=jdbcPassword \
        ).mode("overwrite") \
        .save()

    # data.write \
    # .jdbc(jdbcUrl, 'ibm.schoolcases', properties=connectionProperties)

    # data.write.mode("overwrite").saveAsTable("SchoolCases")


def write_table_second(df):
    jdbcHostname = '127.0.0.1'
    jdbcDatabase = 'ibm'
    jdbcPort = 3306
    jdbcUsername = 'root'
    jdbcPassword = 'admin'
    jdbcUrl = 'jdbc:mysql://{0}:{1}/{2}'.format(jdbcHostname, jdbcPort, jdbcDatabase)

    # jdbc: mysql: // 127.0.0.1: 3306 /?user = root

    connectionProperties = {
        'user': jdbcUsername,
        'password:': jdbcPassword,
        'driver': 'com.mysql.cj.jdbc.Driver'
    }

    # VERSION 2 - WORKING
    df.write.format('jdbc').options( \
        url=jdbcUrl, \
        driver="com.mysql.cj.jdbc.Driver", \
        dbtable="ibm.schoolcases_second", \
        user=jdbcUsername, \
        password=jdbcPassword \
        ).mode("overwrite") \
        .save()

    # data.write \
    # .jdbc(jdbcUrl, 'ibm.schoolcases', properties=connectionProperties)

    # data.write.mode("overwrite").saveAsTable("SchoolCases")

def read_table(df):
    jdbcHostname = '127.0.0.1'
    jdbcDatabase = 'ibm'
    jdbcPort = 3306
    jdbcUsername = 'root'
    jdbcPassword = 'admin'
    jdbcUrl = 'jdbc:mysql://{0}:{1}/{2}'.format(jdbcHostname, jdbcPort, jdbcDatabase)
    jdbcDF2 = df.read \
        .jdbc(f"{jdbcUrl}", "ibm.schoolcases",
              properties={"user": f"{jdbcUsername}", "password": f"{jdbcPassword}"})

    return jdbcDF2