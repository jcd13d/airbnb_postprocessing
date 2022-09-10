from postprocessor.postprocessor import PysparkPostProcessor
from postprocessor.price_postprocessor import PricePostprocessor
from postprocessor.occupancy_postprocessor import OccupancyPostprocessor


def main(config):
    pp_price = PricePostprocessor(**config['price_postprocess']).run()
    pp_occ = OccupancyPostprocessor(**config['occupancy_postprocess']).run()
    pp_review = PysparkPostProcessor(**config['review_postprocess']).run()
    # pp_review = PysparkPostProcessor(**config['listings_postprocess']).run()

    # TODO need to not depend on ID column for these
    # pp_price_meta = PysparkPostProcessor(**config['price_metadata_postprocess']).run()
    # pp_occ_meta = PysparkPostProcessor(**config['occupancy_metadata_postprocess']).run()


if __name__ == "__main__":
    # config_loc = "config/config.json"
    # with open(config_loc, "r") as f:
    #     config = json.load(f)

    config = {
        "occupancy_postprocess": {
            "data_loc": "s3://airbnb-scraper-bucket-0-1-1/data/id_scraper_data/occupancy_beta_20220809/",
            "out_path": "s3://airbnb-scraper-bucket-0-1-1/data/prod_data_tables/occupancy/20220809_postprocessed/",
            "schema": [
                "index",
                "date",
                "available",
                "min_nights",
                "max_nights",
                "available_for_checkin",
                "available_for_checkout",
                "bookable",
                "id",
                "trigger_time",
                "pulled",
                "parition_col"
            ],
            "keys": [
                "id",
                "date",
                "pulled"
            ]
        },
        "price_postprocess": {
            "data_loc": "s3://airbnb-scraper-bucket-0-1-1/data/id_scraper_data/price_beta_20220809/",
            "out_path": "s3://airbnb-scraper-bucket-0-1-1/data/prod_data_tables/price/20220809_postprocessed/",
            "schema": [
                "index",
                "cleaning_fee",
                "service_fee",
                "total_price",
                "check_in",
                "check_out",
                "total_price_description",
                "currency",
                "id",
                "trigger_time",
                "pulled",
                "parition_col"
            ],
            "keys": [
                "id",
                "check_in",
                "check_out",
                "pulled"
            ]
        },
        "review_postprocess": {
            "data_loc": "s3://airbnb-scraper-bucket-0-1-1/data/id_scraper_data/review_beta_20220809/",
            "out_path": "s3://airbnb-scraper-bucket-0-1-1/data/prod_data_tables/reviews/20220809_postprocessed/",
            "write_type": "delta"
        },
        "price_metadata_postprocess": {
            "data_loc": "s3://airbnb-scraper-bucket-0-1-1/data/price_beta_metadata/",
            "out_path": "s3://airbnb-scraper-bucket-0-1-1/data/price_beta_metadata_postprocessed/"
        },
        "occupancy_metadata_postprocess": {
            "data_loc": "s3://airbnb-scraper-bucket-0-1-1/data/occupancy_beta_metadata/",
            "out_path": "s3://airbnb-scraper-bucket-0-1-1/data/occupancy_beta_metadata_postprocessed/"

        }
    }
    main(config)
