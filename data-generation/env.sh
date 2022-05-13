export ENV_VARIABLES_ARE_SET=whatever
export SparkConfigurationMaster=spark://spark:7077
# = local[*] //Prod when writing to files
# = local[2] // Locally
export THRESHOLD=40
export DATA_SOURCE=2
# Ia-hypercontacts THRESHOLD 40, DATA SOURCE 2
export DISTRIBUTION_TYPE=2
# 1 Uniform
# 2 LogNormal
# 3 Guassian
# 4 Zipf
export DISTRIBUTION_PARAM1=1
export DISTRIBUTION_PARAM2=0.4
export TIMESTAMP=10
export INTERVAL_DELTA=1000
export VERTEX_ID=1

export SparkDriverMemory=10g
export SparkExecutorMemory=10g
