def dfprint(session):
    # Reading CSV file
    data = session.read.option('header', 'true').option('inferSchema', 'true').csv(
        'IBMData - Main.csv'
    )
    data.show()
    data.printSchema()
    return data


def dfprint_second(session):
    data2 = session.read.option('header', 'true').option('inferSchema', 'true').csv(
        'IBMData - Second.csv'
    )

    data2.show()
    data2.printSchema()
    return data2
