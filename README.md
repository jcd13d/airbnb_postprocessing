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

TODO
* extract lambda function configuration into json
  * pull at runtime
* make input output stuff configurable from config dir, master config

Setup
* Update function.py with correct configuration
* Update zip call, choose where to store zip for lambda
* Create policy for full EMR access
  * Create role with EMR policy, AmazonElasticMapReduceFullAccess, AWSLambdaExecute, 
    AWSLambdaBasicExecutionRole
* Create lambda function



Couldnt get to bootstrap, made a new bucket and now the shit works? wtf?
maybe is the . in the filepath?
