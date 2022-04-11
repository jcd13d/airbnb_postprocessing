from postprocessor.postprocessor import PysparkPostProcessor
import json


def main(config):
    pp_price = PysparkPostProcessor(**config['price_postprocess']).run()
    # pp_occ = PysparkPostProcessor(**config['occupancy_postprocess']).run()


if __name__ == "__main__":
    config_loc = "config/config.json"
    with open(config_loc, "r") as f:
        config = json.load(f)

    main(config)