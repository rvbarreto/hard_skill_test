from pyspark.sql import SparkSession
from pyspark.sql.functions import split, regexp_replace
from pyspark.sql.types import TimestampType, StringType, FloatType

spark = SparkSession.builder \
                    .master("local[1]") \
                    .appName("ShapeTest") \
                    .getOrCreate()

# read data
match = spark.read.option("header", "true") \
                  .option("delimiter", ";") \
                  .csv("equipment_sensors.csv")
equip = spark.read.option("multiline", "true") \
                  .json("equipment.json")
fail = spark.read.text("equipment_failure_sensors.log")

# remove descriptive texts from failure data and clear deleted fields
fail = fail.withColumn('value', regexp_replace(
    'value', "[A-Za-z\[\]\(\),]:*", "")) \
    .withColumn('value', regexp_replace(
        'value', "\t+", "\t"))

# enforce data schema to failure data
fail_schema = {
    'datetime': [0, TimestampType()],
    'sensor_id': [1, StringType()],
    'temperature': [2, FloatType()],
    'vibration': [3, FloatType()]
}
for k, v in fail_schema.items():
    fail = fail \
        .withColumn(k, split(fail['value'], "\t")
                    .getItem(v[0])
                    .cast(v[1]))
fail = fail.drop('value')

# select desired period
start = "2020-01-01 00:00:00"
end = "2020-01-31 23:59:59"
fail_jan = fail.filter("datetime between '%s' and '%s'" % (start, end))

# join data
df = fail_jan.join(match, on=['sensor_id'], how='left') \
             .join(equip, on=['equipment_id'], how='left')


# 1) Total equipment failures that happened?
total_fails = df.groupby("datetime", "code").count()
ans_1 = total_fails.count()

# 2) Which equipment code had most failures?
code_fails = total_fails.groupby(['code']).count().orderBy('count')
ans_2 = code_fails.tail(1)

# 3) Average amount of failures across equipment group, ordered by the number of failures in ascending order?
group_fails = code_fails.join(equip, on=['code'], how='left')
ans_3 = group_fails.groupby('group_name').avg('count').orderBy('avg(count)')

print('1) Total equipment failures that happened?')
print(ans_1)
print('2) Which equipment code had most failures?')
print(ans_2[0]['code'])
print('3) Average amount of failures across equipment group, ordered by the number of failures in ascending order?')
ans_3.show()
