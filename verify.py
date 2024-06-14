import re
import xlrd
import logging.config
import chardet
from io import StringIO
from pathlib import Path
import pandas as pd
import numpy as np

class method_files:


    def validation_data(self, valid_df: pd.DataFrame, new_df: pd.DataFrame) -> dict:

        logging.info("Verify Changed information..")
        self.logging[-1].update({'function': "validation_data"})
        
        if len(valid_df.index) > len(new_df.index):
            self.skip_rows = [idx for idx in list(valid_df.index) if idx not in list(new_df.index)]

        ## reset index data.
        union_index = np.union1d(valid_df.index, new_df.index)
        ## target / tmp data.
        valid_df = valid_df.reindex(index=union_index, columns=valid_df.columns).iloc[:,:-1]
        ## new data.
        new_df = new_df.reindex(index=union_index, columns=new_df.columns).iloc[:,:-1]

        # compare data rows by rows.
        valid_df['count_change'] = pd.DataFrame(np.where(valid_df.ne(new_df), True, False), index=valid_df.index, columns=valid_df.columns)\
            .apply(lambda x: (x==True).sum(), axis=1)

        def format_record(recorded):
            return  "{" + "\n".join("{!r}: {!r},".format(columns, values) for columns, values in recorded.items()) + "}"

        start_rows = 2
        for idx in union_index:
            if idx not in self.skip_rows:

                recorded = {}
                for old_data, new_data in zip(valid_df.items(), new_df.items()):
                    if valid_df.loc[idx, 'count_change'] != 14:
                        if valid_df.loc[idx, 'count_change'] <= 1:
                            ## No_changed rows.
                            valid_df.at[idx, old_data[0]] = old_data[1].iloc[idx]
                            valid_df.loc[idx, 'remark'] = "No_changed"
                        else:
                            if old_data[1][idx] != new_data[1][idx]:
                                recorded.update({old_data[0]: f"{old_data[1][idx]} -> {new_data[1][idx]}"})
                            ## Updated rows.
                            valid_df.at[idx, old_data[0]] = new_data[1].iloc[idx]
                            valid_df.loc[idx, 'remark'] = "Updated"
                    else:
                        recorded.update({old_data[0]: new_data[1][idx]})
                        ## Inserted rows.
                        valid_df.at[idx, old_data[0]] = new_data[1].iloc[idx]
                        valid_df.loc[idx, 'remark'] = "Inserted"

                if recorded != {}:
                    self.upsert_rows[start_rows + idx] = format_record(recorded)
            else:
                ## Removed rows.
                valid_df.loc[idx, 'remark'] = "Removed"
        self.skip_rows = [idx + start_rows for idx in self.skip_rows]

        valid_df = valid_df.drop(['count_change'], axis=1)
        valid_df.index += start_rows
        compare_data = valid_df.to_dict('index')
        
        self.logging[-1].update({'status': "verify"})
        return compare_data

    def customize_data(self, select_date: list, target_df: pd.DataFrame, tmp_df: pd.DataFrame) -> dict:

        logging.info("Customize Data to Target..")
        self.logging[-1].update({'function': "customize_data"})
        
        try:
            ## unique_date.
            unique_date = target_df[target_df['CreateDate'].isin(select_date)].reset_index(drop=True)
            ## other_date.
            other_date = target_df[~target_df['CreateDate'].isin(select_date)].iloc[:, :-1].to_dict('index')
            max_rows = max(other_date, default=0)
            ## compare data target / tmp.
            compare_data = self.validation_data(unique_date, tmp_df)

            ## add value to other_date.
            other_date = other_date | {max_rows + key:  {**values, **{'mark_rows': key}} \
                if key in self.upsert_rows or key in self.skip_rows \
                    else values for key, values in compare_data.items()}

            ## sorted date order.
            start_row = 2
            merge_data = {start_row + idx : values for idx, values in enumerate(sorted(other_date.values(), key=lambda x: x['CreateDate']))}
            i = 0
            for rows, columns in merge_data.items():
                if columns.get('mark_rows'):
                    if columns['mark_rows'] in self.upsert_rows:
                        self.upsert_rows[f"{rows}"] = self.upsert_rows.pop(columns['mark_rows'])
                    elif columns['mark_rows'] in self.skip_rows:
                        self.skip_rows[i] = rows
                        i += 1
                    columns.pop('mark_rows')

        except Exception as err:
            raise Exception(err)
        
        return merge_data