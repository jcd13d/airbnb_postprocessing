# airbnb_postprocessing
## ID Batch Configuration
* get ids from indexed listings
* based on batch configuration:
  * create batch sumbission json and id config

## Occupancy
* How to parition?
  * select num partitions and modulo id
  * may need to repartition after a while, we might be writing lots of small 
    "part" files as we postprocess
* Drop duplicates keep changes in occupancy

## Pricing
* How to partition?