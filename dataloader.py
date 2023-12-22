import os
import sys

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from connector import ftpConnector  # noqa: E402
from utils import get_month, get_year  # noqa: E402


class dataLoader(object):
    def __init__(self, sns_auth_dict: dict = None) -> None:
        if sns_auth_dict is None:
            self.sns_auth_dict = {
                "username": os.environ["AUTH_USERNAME"],
                "password": os.environ["AUTH_PASSWORD"],
            }
        else:
            self.sns_auth_dict = sns_auth_dict
        self.sns_info = {
            "host": "ftpsns-fr.eu.airbus.corp",
            "port": 21,
            "root_folder": os.path.join("Apps", "HUMAN RESOURCES"),
            "local_root": "",
        }
        self.sns = ftpConnector(**self.sns_info)

    def download_nl_reco(
        self,
        years: list,
        out_path: str = None,
        source: str = "wd",
        force_actual_month: bool = False,
        overwrite_existing: bool = False,
    ) -> list:
        """
        This function is to download the NL Reconciliation for People Analytics Team.

        Args:
            - years (list): list of years for the file download
            - out_path (str): out path for the files
            - source (str): data source (could be 'wd' or 'bi')
            - force_actual_month (bool): force to download the latest file by month
            (e.g. YYYY-04 in May or YYYY-03 in April)
            - overwrite_existing (bool): force overwrite of the file
            (download will be skipped if file with exact matching name is in targe folder (out_path))

        Result:
            - files (list): list of filenames which ar available in the target folder (downloaded new or pre-available)
        """

        # create default path if not specified
        if out_path is None:
            out_path = os.getcwd()

        # convert data type if it's int or str
        if isinstance(years, str) or isinstance(years, int):
            years = [years]

        # check if the source of data is defined well
        sources = ["WD", "BI"]
        if source.upper() not in sources:
            raise NotImplementedError(
                f"source ({source}) os not implemented in available sources ({sources})."
                "please check dataLoader, download nl"
            )

        # get list of available files
        available_file_list = self.sns._list_files(
            self.sns_auth_dict,
            os.path.join(self.sns_info["root_folder"], "NL_Reconciliation", "Output"),
        )

        # generate prefix
        if source.upper() == "BI":
            prefix = "historical_nl_"
        elif source.upper() == "WD":
            prefix = "historical_nl_WD_"

        # generate prefix list
        prefix_list = [prefix + str(year) + "_updated_" for year in years]

        # create filename by actual month or use the latest available filenames
        if force_actual_month:
            # create list of file names
            files = [
                prefix
                + str(get_year(delta_month=-1))
                + "-"
                + get_month(delta_month=-1)
                + ".parquet"
                for prefix in prefix_list
            ]

            # check if there are missing files
            missing_files = list(set(files) - set(available_file_list))
            if len(missing_files) > 0:
                raise ReferenceError(f"following files not found: {missing_files}")

        else:
            # get the latest available files
            files = [
                file
                for file in available_file_list
                if any(str(prefix) in file for prefix in prefix_list)
            ]

            # ensure only one file found and used per year (maybe 2 during update of nl beginning of each month)
            if len(files) != len(years):
                files = [
                    max([file for file in files if pre in file]) for pre in prefix_list
                ]

        # create directory if not available
        if out_path != "":
            os.makedirs(out_path, exist_ok=True)

        # remove available files if not forced to overwrite
        if not overwrite_existing:
            files_to_download = [
                file for file in files if file not in os.listdir(out_path)
            ]

        # download the files if needed
        if len(files_to_download) > 0:
            self.sns.download_file_list(
                self.sns_auth_dict,
                files_to_download,
                remote_path=os.path.join("NL_Reconciliation", "Output"),
                local_path=out_path,
            )

        # return the downloaded and available files
        return files


if __name__ == "__main__":
    dl = dataLoader()
    files = dl.download_nl_reco([2017], "data")
