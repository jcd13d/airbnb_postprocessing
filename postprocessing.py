from postprocessor.postprocessor import PysparkPostProcessor
import json


def main(config):
    pp_price = PysparkPostProcessor(**config['price_postprocess']).run()
    # pp_occ = PysparkPostProcessor(**config['occupancy_postprocess']).run()


if __name__ == "__main__":
    # config_loc = "config/config.json"
    # with open(config_loc, "r") as f:
    #     config = json.load(f)

    config = {
        "occupancy_postprocess": {
            "data_loc": "s3://jd-s3-test-bucket9/data/occupancy_beta_20220411/",
            "out_path": "s3://jd-s3-test-bucket9/data/occupancy_beta_20220411_post/"
        },
        "price_postprocess": {
            "data_loc": "s3://jd-s3-test-bucket9/data/price_beta_20220411/",
            "out_path": "s3://jd-s3-test-bucket9/data/price_beta_20220411_post/"
        }
    }
    main(config)
