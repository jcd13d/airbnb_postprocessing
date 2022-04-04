from postprocessor import PostProcessor
import pandas as pd
import s3fs

config = {
    "data_loc": "s3://jd-s3-test-bucket9/data/occupancy_beta/",
    # "data_loc": "s3://jd-s3-test-bucket9/data/occupancy_beta/array_0/20220323220546",
    "partition": ["partition_col"],
    "out_path": "s3://jd-s3-test-bucket9/data/occupancy_beta_full/",
    "num_partitions": 20
}


class OccupancyPostProcessing(PostProcessor):
    def __init__(self, num_partitions, data_loc, out_path, partition=[]):
        super().__init__(num_partitions=num_partitions, data_loc=data_loc, partition=partition, out_path=out_path)

    def postprocess_logic(self):
        self.data['partition_col'] = self.data['id'] % 20
        print(self.data)


if __name__ == "__main__":
    processor = OccupancyPostProcessing(**config)
    processor.run()