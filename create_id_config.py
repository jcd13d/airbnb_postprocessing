import argparse

import numpy as np
import json
import os

CONFIG_LOC = "config/config.json"


def id_list_to_config(ids, num_ids_per_container):
    ids = np.array(ids)

    total_listings = len(ids)
    num_containers = (np.ceil(total_listings/num_ids_per_container))

    ids2 = np.pad(ids, (0, int(num_containers*num_ids_per_container) - total_listings))
    ids2 = ids2.reshape(-1, num_ids_per_container)
    print(f"Resulting configuration: {ids2.shape[0]} containers running {ids2.shape[1]} IDs each.")

    ids2 = ids2.tolist()
    ids2 = [list(filter((0.0).__ne__, x)) for x in ids2] # filter out pad zeros
    return ids2, num_containers


def create_batch_submission_config(batch_job_template, num_containers):
    batch_job_template["arrayProperties"]["size"] = int(num_containers)
    return batch_job_template


def main(num_ids_per_container, batch_job_template, id_list_location, out_loc):
    with open(id_list_location, "r") as f:
        ids = json.load(f)["ids"]

    list_of_id_lists, num_containers = id_list_to_config(ids, num_ids_per_container)

    sub_config = create_batch_submission_config(batch_job_template, num_containers)

    with open(os.path.join(out_loc, "batch_array_job_sub.json"), "w") as f:
        json.dump(sub_config, f, indent=4)

    with open(os.path.join(out_loc, "id_config.json"), "w") as f:
        json.dump({"id_configs": list_of_id_lists}, f, indent=4)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='ID configuration creator')
    parser.add_argument('-n', '--num-ids-per-container', type=int, help="Number of IDs run in one container on AWS", required=True)
    args = parser.parse_args()

    with open(CONFIG_LOC, "r") as f:
        config = json.load(f)

    main(args.num_ids_per_container, **config)




