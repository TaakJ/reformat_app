from abc import (
    ABC,
    abstractmethod,
)
from datetime import (
    datetime,
)
from os.path import (
    join,
)
from module import (
    convert_2_files,
)
from setup import (
    CONFIG,
    PARAMS,
)
import logging
import re


class collect_log(ABC):
    def __init__(
        self,
    ):
        self._log = []

    @property
    def logging(
        self,
    ) -> list:
        return self._log

    @logging.setter
    def logging(
        self,
        log: list,
    ) -> None:
        self.log_setter(log)

    @abstractmethod
    def log_setter(
        self,
        log: list,
    ):
        pass


class collect_params:
    def params_setter(
        self,
        module: str,
    ) -> None:

        for (
            key,
            value,
        ) in PARAMS.items():
            setattr(
                self,
                key,
                value,
            )

        self.module = module
        self.fmt_batch_date = self.batch_date
        self.date = datetime.now()
        self.input_dir = [
            join(
                CONFIG[self.module]["input_dir"],
                CONFIG[self.module]["input_file"],
            )
        ]
        # for i in CONFIG[self.module]["require"]:
        #     self.input_dir += [join(CONFIG[i]["input_dir"], CONFIG[i]["input_file"])]
        self.output_dir = CONFIG[self.module]["output_dir"]
        self.output_file = CONFIG[self.module]["output_file"]


class collect_data:

    def extract_adm_data(
        self,
        i,
        line,
    ):

        logging.info("Extract Data for ADM Module.")

        state = "failed"
        self.logging[i].update(
            {
                "function": "extract_adm_data",
                "state": state,
            }
        )

        data = []
        for l in line:
            regex = re.compile(r"\w+.*")
            find_word = regex.findall(l)
            if find_word != []:
                data += [
                    re.sub(
                        r"\W\s+",
                        "||",
                        "".join(find_word).strip(),
                    ).split("||")
                ]

        state = "succeed"
        self.logging[i].update({"state": state})
        return {"ADM": data}

    def extract_doc_data(
        self,
        i,
        line,
    ):

        logging.info("Extract Data for DOC Module.")

        state = "failed"
        self.logging[i].update(
            {
                "function": "extract_doc_data",
                "state": state,
            }
        )

        data = []
        for l in line:
            regex = re.compile(r"\w+.*")
            find_word = regex.findall(l)
            if find_word != []:
                data += [
                    re.sub(
                        r"\W\s+",
                        "||",
                        "".join(find_word).strip(),
                    ).split("||")
                ]

        fix_data = []
        for (
            rows,
            value,
        ) in enumerate(data):
            if rows == 0:
                continue
            elif rows == 1:
                ## header
                fix_data += [" ".join(value).split(" ")]
            else:
                ## value
                fix_column = []
                for (
                    idx,
                    column,
                ) in enumerate(
                    value,
                    1,
                ):
                    if idx == 4:
                        l = re.sub(
                            r"\s+",
                            ",",
                            column,
                        ).split(",")
                        fix_column.extend(l)
                    else:
                        fix_column.append(column)
                fix_data.append(fix_column)

        state = "succeed"
        self.logging[i].update({"state": state})
        return {"DOC": fix_data}

    def extract_lds_data(
        self,
        i,
        line,
    ):

        logging.info("Extract Data for LDS Module.")

        state = "failed"
        self.logging[i].update(
            {
                "function": "extract_lds_data",
                "state": state,
            }
        )

        data = []
        for l in line:
            regex = re.compile(r"\w+.*")
            find_word = regex.findall(l)
            if find_word != []:
                data += [
                    re.sub(
                        r"\W\s+",
                        ",",
                        "".join(find_word).strip(),
                    ).split(",")
                ]

        fix_data = []
        for (
            rows,
            value,
        ) in enumerate(data):
            if rows == 0:
                ## header
                fix_data += [" ".join(value).split(" ")]
            else:
                ## value
                fix_column = []
                for (
                    idx,
                    column,
                ) in enumerate(
                    value,
                    1,
                ):
                    if idx == 1:
                        l = re.sub(
                            r"\s+",
                            ",",
                            column,
                        ).split(",")
                        fix_column.extend(l)
                    elif idx == 32:
                        continue
                    else:
                        fix_column.append(column)
                fix_data.append(fix_column)

        state = "succeed"
        self.logging[i].update({"state": state})
        return {"LDS": fix_data}

    def extract_bos_data(
        self,
        i,
        workbook,
    ):

        logging.info("Extract Data for BOS Module.")

        state = "failed"
        self.logging[i].update(
            {
                "function": "extract_bos_data",
                "state": state,
            }
        )

        sheet_list = [sheet for sheet in workbook.sheet_names()]

        data = {}
        for sheets in sheet_list:
            cells = workbook.sheet_by_name(sheets)
            for row in range(
                0,
                cells.nrows,
            ):
                by_sheets = [
                    cells.cell(
                        row,
                        col,
                    ).value
                    for col in range(cells.ncols)
                ]
                if sheets not in data:
                    data[sheets] = [by_sheets]
                else:
                    data[sheets].append(by_sheets)

        state = "succeed"
        self.logging[i].update({"state": state})
        return data

    def extract_cum_data(
        self,
        i,
        workbook,
    ):

        logging.info("Extract Data for CUM Module.")

        state = "failed"
        self.logging[i].update(
            {
                "function": "extract_cum_data",
                "state": state,
            }
        )

        sheet_list = [sheet for sheet in workbook.sheet_names()]

        data = {}
        for sheets in sheet_list:
            cells = workbook.sheet_by_name(sheets)
            for row in range(
                0,
                cells.nrows,
            ):
                by_sheets = [
                    cells.cell(
                        row,
                        col,
                    ).value
                    for col in range(cells.ncols)
                ][1:]
                if not all(empty == "" for empty in by_sheets):
                    if sheets not in data:
                        data[sheets] = [by_sheets]
                    else:
                        data[sheets].append(by_sheets)

        state = "succeed"
        self.logging[i].update({"state": state})
        return data

    def extract_ica_data(
        self,
        i,
        workbook,
    ):

        logging.info("Extract Data for ICA Module.")

        state = "failed"
        self.logging[i].update(
            {
                "function": "extract_ica_data",
                "state": state,
            }
        )

        sheet_list = [sheet for sheet in workbook.sheet_names()]

        data = {}
        for sheets in sheet_list:
            cells = workbook.sheet_by_name(sheets)
            for row in range(
                0,
                cells.nrows,
            ):
                by_sheets = [
                    cells.cell(
                        row,
                        col,
                    ).value
                    for col in range(cells.ncols)
                ]
                if sheets not in data:
                    data[sheets] = [by_sheets]
                else:
                    data[sheets].append(by_sheets)

        state = "succeed"
        self.logging[i].update({"state": state})
        return data

    def extract_iic_data(
        self,
        i,
        workbook,
    ):

        logging.info("Extract Data for IIC Module.")

        state = "failed"
        self.logging[i].update(
            {
                "function": "extract_iic_data",
                "state": state,
            }
        )

        sheet_list = [sheet for sheet in workbook.sheet_names() if sheet != "StyleSheet"]

        data = {}
        for sheets in sheet_list:
            cells = workbook.sheet_by_name(sheets)
            for row in range(
                0,
                cells.nrows,
            ):
                by_sheets = [
                    cells.cell(
                        row,
                        col,
                    ).value
                    for col in range(cells.ncols)
                ]
                if not all(empty == "" for empty in by_sheets):
                    if sheets not in data:
                        data[sheets] = [by_sheets]
                    else:
                        data[sheets].append(by_sheets)

        state = "succeed"
        self.logging[i].update({"state": state})
        return data

    def extract_lmt_data(
        self,
        i,
        workbook,
    ):

        logging.info("Extract Data for LMT Module.")

        state = "failed"
        self.logging[i].update(
            {
                "function": "extract_lmt_data",
                "state": state,
            }
        )

        sheet_list = [sheet for sheet in workbook.sheet_names() if sheet != "StyleSheet"]

        data = {}
        for sheets in sheet_list:
            cells = workbook.sheet_by_name(sheets)
            for row in range(
                0,
                cells.nrows,
            ):
                by_sheets = [
                    cells.cell(
                        row,
                        col,
                    ).value
                    for col in range(cells.ncols)
                ]
                if sheets not in data:
                    data[sheets] = [by_sheets]
                else:
                    data[sheets].append(by_sheets)

        state = "succeed"
        self.logging[i].update({"state": state})
        return data

    def extract_moc_data(
        self,
        i,
        workbook,
    ):

        logging.info("Extract Data for MOC Module.")

        state = "failed"
        self.logging[i].update(
            {
                "function": "extract_moc_data",
                "state": state,
            }
        )

        sheet_list = [sheet for sheet in workbook.sheet_names() if sheet != "StyleSheet"]

        data = {}
        for sheets in sheet_list:
            cells = workbook.sheet_by_name(sheets)
            for row in range(
                0,
                cells.nrows,
            ):
                by_sheets = [
                    cells.cell(
                        row,
                        col,
                    ).value
                    for col in range(cells.ncols)
                ]
                if not all(empty == "" for empty in by_sheets):
                    if sheets not in data:
                        data[sheets] = [by_sheets]
                    else:
                        data[sheets].append(by_sheets)

        state = "succeed"
        self.logging[i].update({"state": state})
        return data


class backup:
    def __init__(
        self,
    ) -> None:
        pass


class call_function(
    convert_2_files,
    collect_log,
    collect_params,
    collect_data,
):
    pass
