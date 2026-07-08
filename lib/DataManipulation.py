from pyspark.sql.functions import *

def filter_closed_orders(orders_df):
    return orders_df.filter("order_status = 'CLOSED'")

def join_orders_customers(orders_df, customers_df):
    return orders_df.join(customers_df, "customer_id")

def count_orders_state(joined_df):
    return joined_df.groupBy('state').count()

def count_orders_state_on_state(joined_df,param):
    return joined_df.filter(col("state") == param).count()

def create_temp_view(df,view_name):
    return df.createOrReplaceTempView(view_name)