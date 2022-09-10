from postprocessor.postprocessor import PysparkPostProcessor


class PricePostprocessor(PysparkPostProcessor):
    def __init__(self, data_loc, out_path, partition=[], num_partitions=20, write_type="parquet",
                 dir_level=2, schema=None, keys=None):
        super(PricePostprocessor, self).__init__(data_loc, out_path, partition, num_partitions, write_type, dir_level, schema, keys)

    def postprocess_logic(self):
        super().postprocess_logic()
        # trigger_time and pulled handled in parent
        # for price, we have specifically the need for to_datetime for check_in, check_out

        # change check_in
        date_format_check = "yyyy-MM-dd"
        self.to_datetime_wrapper("check_in", date_format_check)
        self.to_datetime_wrapper("check_out", date_format_check)


