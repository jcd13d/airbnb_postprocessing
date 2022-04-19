from postprocessor.postprocessor import PysparkPostProcessor


def main(config):
    pp_price = PysparkPostProcessor(**config['price_postprocess']).run()
    pp_occ = PysparkPostProcessor(**config['occupancy_postprocess']).run()

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

        }
    }
    main(config)
