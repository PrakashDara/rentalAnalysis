import sys
from lib import DataManipulation, DataReader, Utils
from pyspark.sql.functions import *

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)


job_run_env = sys.argv[1]

print("Creating Spark Session")

spark = Utils.get_spark_session(job_run_env) #job_run_env argument is LOCAL

print("Created Spark Session")

orders_df = DataReader.read_orders(spark,job_run_env)

print("Diplsaying Orders")
orders_df.show(10)

print("Filtering the Closed Orders only")
orders_filtered = DataManipulation.filter_closed_orders(orders_df)

print("Filtered Data Show")
orders_filtered.show(10)

customers_df = DataReader.read_customers(spark,job_run_env)

print("Dispaying Customers")
customers_df.show(10)

print("Joing the orders with customers on customer_id")
joined_df = DataManipulation.join_orders_customers(orders_filtered,customers_df)

print("Aggregated Results")
aggregated_results = DataManipulation.count_orders_state(joined_df)

aggregated_results.show()

single_state_based_result = DataManipulation.count_orders_state_on_state(joined_df,'CA')

print("State of CA-",single_state_based_result)

managed_table_orders = DataManipulation.create_temp_view(orders_df,'orders')

managed_table_customers = DataManipulation.create_temp_view(orders_df,'customers')

print("Reading Data from SQL Managed Table Semi Join")
spark.sql("""
        select * from customers 
        semi join orders on customers.customer_id = orders.customer_id
 """).show(10)

print("Reading Data from SQL Managed Table Anti Join")
spark.sql("""
        select * from customers 
        anti join orders on customers.customer_id = orders.customer_id
 """).show(10)

print("Print Orders Date Wise")
spark.sql("""
        select Date(order_date),count(*) as cnt from orders
        group by Date(order_date) 
        having cnt > 250
        order by cnt desc
""").show()


print("end of main")