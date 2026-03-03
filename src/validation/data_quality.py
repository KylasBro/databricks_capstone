def check_nulls(df):
    return df.filter("year IS NULL OR enrollment IS NULL").count() == 0