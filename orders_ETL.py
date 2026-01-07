from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName('test').getOrCreate()
df = spark.read.option('multiLine',True).json('orders_dirty_data.json')
print('Rearrange the schema')
df=df.select('order_id','customer','order_date','items','total_amount')
df.show(truncate=False)

print('Cleaning the order_id')
df = df.withColumn(
    'order_id',
    when(col('order_id').rlike(r'^[0-9]+$'), col('order_id').cast(IntegerType()))\
    .otherwise(None)
)
df.show(truncate=False)
df.printSchema()

print('Aligning the customer')
df = df.select(
    col('order_id'),
    col('customer.id').alias('customer_id'),
    col('customer.email').alias('customer_email'),
    col('order_date').alias('order_date'),
    col('items').alias('items'),
    col('total_amount').alias('total_amount'),
).drop('customer')
df.show(truncate=False)

print('Cleaning the customer_email')
df = df.withColumn(
    'customer_email',
    when(trim(col('customer_email')).rlike(r'^[a-z.]+@[a-z.]+$'), trim(col('customer_email')))\
    .otherwise(None)
)
df.show(truncate=False)
print('Cleaning the customer_id')
df = df.withColumn(
    'customer_id',
    when(trim(col('customer_id')).rlike(r'^C[0-9]+$'), trim(col('customer_id')))
)
df.show(truncate=False)
print('Cleaning the order_date')
df = df.withColumn(
    'order_date',
    coalesce(
        try_to_timestamp(col('order_date'), lit('yyyy-MM-dd')),
        try_to_timestamp(col('order_date'), lit('dd-MM-yyyy')),
        try_to_timestamp(col('order_date'), lit('yyyy/MM/dd'))
    ).cast(DateType())
)
df.show(truncate=False)
df.printSchema()

print('Clean total amount')
df = df.withColumn(
    'total_amount',
    when(col('total_amount').rlike(r'^[0-9]+$'),
         col('total_amount').cast(IntegerType()))
    .otherwise(None)
)
df.show(truncate=False)
print('Explode the items')
df_items = df.select(
    col('order_id'),
    explode('items').alias('items')
)
df_items.show(truncate=False)

print('New dataframe items')
df_items = df_items.select(
    col('order_id'),
    col('items.product_id').alias('product_id'),
    col('items.qty').alias('qty'),
    col('items.price').alias('price')
)
df_items.show(truncate=False)
df_items.printSchema()

print('Cleaning the product_id')

df_items = df_items.withColumn(
    'product_id',
    when(trim(col('product_id')).rlike(r'^P[0-9]+$'), trim(col('product_id')))\
    .otherwise(None)
)
df_items.show(truncate=False)

print('Cleaning the qty')
df_items = df_items.withColumn(
    'qty',
    when(trim(col('qty')).rlike(r'^[0-9]+$'), trim(col('qty')).cast(IntegerType()))\
    .otherwise(None)
)
df_items.show(truncate=False)

print('Cleaning the price')
df_items = df_items.withColumn(
    'price',
    when(trim(col('price')).rlike(r'^[0-9]+$'), trim(col('price')).cast(IntegerType()))\
    .otherwise(None)
)
df_items.show(truncate=False)
df_items.printSchema()

print('Drop row if product id is null and qty less than 0 or 0')
df_items = df_items.filter(col('product_id').isNotNull())
df_items = df_items.filter(col('qty')>0)
df_items.show(truncate=False)

print('Add amount_mismatch')
df_items = df_items.withColumn(
    'line_amount',
    col('qty') * col('price')
)
df_items.show(truncate=False)

df = df.select(
    col('order_id'),
    col('customer_id'),
    col('customer_email'),
    col('order_date'),
    col('total_amount')
)
df.show(truncate=False)

df_agg = df_items.groupBy('order_id').agg(sum('line_amount').alias('calculated_amount'))
df_agg.show(truncate=False)
o = df.alias('o')
a = df_agg.alias('a')
df = o.join(a, o.order_id == a.order_id, 'left')
df.show(truncate=False)

print('Calculated_total')
df = df.withColumn(
    'amount_mismatched',
    when(col('total_amount').isNull() | col('calculated_amount').isNull(), lit(True))\
    .when(col('total_amount') != col('calculated_amount'), lit(True))\
    .otherwise(lit(False))
)
df.show(truncate=False)

df = df.select(
    col('o.order_id'),
    col('customer_id'),
    col('customer_email'),
    col('order_date'),
    col('total_amount'),
    col('calculated_amount'),
    col('amount_mismatched')
)



df = df.filter(col('o.order_id').isNotNull() & col('o.order_date').isNotNull())
df.show(truncate=False)
df_items.show(truncate=False)
