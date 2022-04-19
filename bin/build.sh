zip -r build/dependencies/dependencies.zip postprocessor
aws s3 cp ./build/dependencies/dependencies.zip s3://airbnb-scraper-bucket-0-0-1/postprocessing_files/dependencies.zip
aws s3 cp ./postprocessing.py s3://airbnb-scraper-bucket-0-0-1/postprocessing_files/postprocessing.py
aws s3 cp ./aws/set_up_cluster.sh s3://airbnb-scraper-bucket-0-0-1/postprocessing_files/set_up_cluster.sh
