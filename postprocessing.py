from postprocessor.postprocessor import PysparkPostProcessor, ListingIndexerPostProcessor


def main(config):
    pp_price = PysparkPostProcessor(**config['price_postprocess']).run()
    pp_occ = PysparkPostProcessor(**config['occupancy_postprocess']).run()
    # TODO: remove for loop (processing should occur on all regions)
    for region, region_config in config['listing_indexer_postprocess'].items():
        pp_listing_indexer = ListingIndexerPostProcessor(**region_config).run()

    # TODO need to not depend on ID column for these
    # pp_price_meta = PysparkPostProcessor(**config['price_metadata_postprocess']).run()
    # pp_occ_meta = PysparkPostProcessor(**config['occupancy_metadata_postprocess']).run()


if __name__ == "__main__":
    # config_loc = "config/config.json"
    # with open(config_loc, "r") as f:
    #     config = json.load(f)

    config = {
        "occupancy_postprocess": {
            "data_loc": "s3://airbnb-scraper-bucket-0-0-1/data/occupancy_beta_20220418/",
            "out_path": "s3://airbnb-scraper-bucket-0-0-1/data/occupancy_beta_20220418_postprocessed/"
        },
        "price_postprocess": {
            "data_loc": "s3://airbnb-scraper-bucket-0-0-1/data/price_beta_20220418/",
            "out_path": "s3://airbnb-scraper-bucket-0-0-1/data/price_beta_20220418_postprocessed/"
        },
        "price_metadata_postprocess": {
            "data_loc": "s3://airbnb-scraper-bucket-0-0-1/data/price_beta_metadata/",
            "out_path": "s3://airbnb-scraper-bucket-0-0-1/data/price_beta_metadata_postprocessed/"

        },
        "occupancy_metadata_postprocess": {
            "data_loc": "s3://airbnb-scraper-bucket-0-0-1/data/occupancy_beta_metadata/",
            "out_path": "s3://airbnb-scraper-bucket-0-0-1/data/occupancy_beta_metadata_postprocessed/"

        },
        "listing_indexer_postprocess": {
            "VT_NH": {
                "data_loc": "s3://airbnb-scraper-bucket-0-0-1/data/listings_beta_20220423/",
                "out_path": "s3://airbnb-scraper-bucket-0-0-1/master_configs/ids/ids_vt_nh.json",
                "num_samples": 2000,
                "seed": 42
            },
            "north_carolina": {
                "data_loc": "s3://airbnb-scraper-bucket-0-0-1/data/listings_beta_20220423/",
                "out_path": "s3://airbnb-scraper-bucket-0-0-1/master_configs/ids/ids_north_carolina.json",
                "num_samples": 2000,
                "seed": 42
            }

        }
    }
    main(config)
