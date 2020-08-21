# reading-sos-reports
This repository provides scripts to read SOS Reports and generate insights on them for performing actions like data sanity and status checks.

## Why do we need this repo?
While re-partitioning the SOS reports and appending new data to the existing partitions, we want to ensure that there has been no data loss and that we have made some progress, specifically in reducing the object count per partitions. This repository provides various ways of doing that, to ease your ad-hoc processes.

## Notebooks concerned:
### Re-partition the historical data
`multiple_buckets_repartitioning.ipynb`
### Check whether there was any data loss
`check_distinct_count.ipynb`

## File information:
```
.
├── calculate_object_count_difference.py # calculates the difference between initial and final object count difference
├── check_distinct_count.ipynb # notebook to check whether there was any data loss while re-partitioning the SOS reports
├── failed_buckets.py # a Python module that contains a temp list of the buckets failed.
├── get_sosreports_metadata.py # generate metadata (object count, size) for the SOS reports
├── granular_sos_reports_metadata.py # generate metadata at each granularity of the S3 path. Useful while determining the skewed data, identifying ill-partitioned data.
├── list-buckets.py # Obsolete as of now, maybe useful in the future. 
├── list_failed_buckets.py # List all the buckets that were marked as failed, Spark as the source of truth. Note: This does not mean that the buckets listed here were not failed. For complete sanity check, use the `check_distinct_count.ipynb` notebook.
├── multiple_buckets_repartitioning.ipynb # re-partition all the buckets. Change the mode to 'append' if using for incoming data, 'overwrite' for re-partitioning the historical data from scratch.
├── Pipfile
├── Pipfile.lock
├── README.md
└── source_buckets.py # list of all the 240 buckets that contain SOS reports.

```

## Important Links:
https://unraveldata.com/common-failures-slowdowns-part-ii/

https://medium.com/@mrpowers/managing-spark-partitions-with-coalesce-and-repartition-4050c57ad5c4

https://stackoverflow.com/questions/49705277/spark-parquet-partitioning-how-to-choose-a-key

https://stackoverflow.com/questions/32621990/what-are-workers-executors-cores-in-spark-standalone-cluster#:~:text=The%20driver%20is%20the%20process,the%20tasks%20on%20the%20executors.&text=Executors%20are%20worker%20nodes'%20processes,in%20a%20given%20Spark%20job.&text=Once%20they%20have%20run%20the,the%20results%20to%20the%20driver.

https://stackoverflow.com/questions/27181737/how-to-deal-with-executor-memory-and-driver-memory-in-spark

https://stackoverflow.com/questions/36723963/in-spark-is-counting-the-records-in-an-rdd-expensive-task

https://blog.cloudera.com/how-to-tune-your-apache-spark-jobs-part-2/

https://stackoverflow.com/questions/37871194/how-to-tune-spark-executor-number-cores-and-executor-memory

https://stackoverflow.com/questions/32621990/what-are-workers-executors-cores-in-spark-standalone-cluster

https://stackoverflow.com/questions/42576661/diskpressure-crashing-the-node

https://medium.com/airbnb-engineering/on-spark-hive-and-small-files-an-in-depth-look-at-spark-partitioning-strategies-a9a364f908
