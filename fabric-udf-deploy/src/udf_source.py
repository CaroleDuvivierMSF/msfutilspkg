# src/udf_source.py

from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

@udf.function(returnType=IntegerType())
def add_two_numbers(a: int, b: int) -> int:
    """Spark UDF to add two integers and return the result."""
    return a + b

@udf.function(returnType='string')
def simple_echo(value: str) -> str:
    """Spark UDF that simply echoes the input string."""
    return f"Echo: {value.upper()}"