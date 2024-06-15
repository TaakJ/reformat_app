import re
import xlrd
import logging.config
import chardet
from io import StringIO
from pathlib import Path
import pandas as pd
import numpy as np


class method_files:

    def customize_data(
        self, select_date: list, target_df: pd.DataFrame, tmp_df: pd.DataFrame
    ) -> dict:

        logging.info("Customize Data to Target..")
        self.logging[-1].update({"function": "customize_data"})

        try:
            ## unique_date.
            unique_date = target_df[
                target_df["CreateDate"].isin(select_date)
            ].reset_index(drop=True)
            ## other_date.
            other_date = (
                target_df[~target_df["CreateDate"].isin(select_date)]
                .iloc[:, :-1]
                .to_dict("index")
            )
            max_rows = max(other_date, default=0)
            ## compare data target / tmp.
            compare_data = self.validation_data(unique_date, tmp_df)

            ## add value to other_date.
            other_date = other_date | {
                max_rows
                + key: (
                    {**values, **{"mark_rows": key}}
                    if key in self.change_rows or key in self.remove_rows
                    else values
                )
                for key, values in compare_data.items()
            }

            ## sorted date order.
            start_row = 2
            merge_data = {
                start_row + idx: values
                for idx, values in enumerate(
                    sorted(other_date.values(), key=lambda x: x["CreateDate"])
                )
            }
            i = 0
            for rows, columns in merge_data.items():
                if columns.get("mark_rows"):
                    if columns["mark_rows"] in self.change_rows:
                        self.change_rows[f"{rows}"] = self.change_rows.pop(
                            columns["mark_rows"]
                        )
                    elif columns["mark_rows"] in self.remove_rows:
                        self.remove_rows[i] = rows
                        i += 1
                    columns.pop("mark_rows")

        except Exception as err:
            raise Exception(err)

        return merge_data
