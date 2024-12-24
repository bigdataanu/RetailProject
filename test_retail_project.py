import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers
from lib.DataReader import read_orders

def test_read_customers():
    spark = get_spark_session("LOCAL")
    customer_count = read_customers(spark,"LOCAL").count()
    assert customer_count == 12435

def test_read_orders():
    spark = get_spark_session("LOCAL")
    orders_count = read_orders(spark,"LOCAL").count()
    assert orders_count == 68883