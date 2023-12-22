import ftplib
import paramiko
import os
from pathlib import Path


class remotefiletransfer:
    def __init__(self, host, port, root_folder, local_root):
        self._host = host
        self._port = int(port)
        self._remote_root_folder = os.path.normpath(root_folder)
        self._local_root_folder = os.path.normpath(local_root)

    def download_file(
        self,
        auth_dict: dict,
        remote_filepath: str,
        local_filepath: str,
        overwrite_existing: bool = True,
    ) -> bool:
        """
        This function will download a file from remote.

        Args:
            - auth_dict (dict): authentification dict
            - remote_filepath (str): path (including file name) of the file to download
            - local_filepath (str): target path of the file (rename possible)
            - overwrite_existing (bool): flag if file should be overwritten if local_filepath exists

        Result:
            - status (bool): True if file is downloaded and False if it is not downloaded
        """

        # get full path from inited root folder
        remote_root_filepath = Path(self._remote_root_folder).joinpath(remote_filepath)
        local_root_filepath = Path(self._local_root_folder).joinpath(local_filepath)

        # return False if file exists
        if not overwrite_existing and Path(local_root_filepath).exists():
            return False

        print(f"Connecting to {self._host}:{self._port} as {auth_dict['username']}")

        # download files
        self._get_single_file(auth_dict, remote_root_filepath, local_root_filepath)

        return True

    def download_file_list(
        self,
        auth_dict: dict,
        file_list: list,
        remote_path: str = "",
        local_path: str = "",
        overwrite_existing: bool = True,
    ) -> bool:
        """
        This function will download a filelist from remote.

        Args:
            - auth_dict (dict): authentification dict
            - file_list (list): list of files to download
            - remote_path (str): path of the files to download
            - local_path (str): target path of the files
            - overwrite_existing (bool): flag if file should be overwritten if output_filepath exists

        Result:
            - status (bool): True if file is downloaded and False if it is not downloaded
        """

        # update to system aligned path format
        remote_path = Path(remote_path)
        local_path = Path(local_path)

        print(f"Connecting to {self._host}:{self._port} as {auth_dict['username']}")

        # get full path from inited root folder
        local_root_path = Path(self._local_root_folder).joinpath(local_path)
        remote_root_path = Path(self._remote_root_folder).joinpath(remote_path)

        # shorten filelist if files available
        if not overwrite_existing:
            file_list = list(set(file_list) - set(os.listdir(local_root_path)))

        # return false if no file is available after shorten
        if len(file_list) <= 0:
            return False

        # create local and remote list combination
        file_list = [
            (
                Path(remote_root_path).joinpath(filename),
                Path(local_root_path).joinpath(filename),
            )
            for filename in file_list
        ]

        # create directory if not available
        if local_path != "":
            os.makedirs(local_path, exist_ok=True)

        # download file list
        self._get_file_list(auth_dict, file_list)

        return True

    def upload_file(
        self,
        auth_dict: dict,
        local_filepath: str,
        remote_filepath: str,
        overwrite_existing: bool = False,
    ) -> bool:
        """
        This function will upload a single file.

        Args:
            - auth_dict (dict): authentification dict
            - local_filepath (str): path (including file name) of the file to upload
            - remote_filepath (str): target path of the file (rename possible)
            - overwrite_existing (bool): flag if file should be overwritten if remote_filepath exists

        Result:
            - status (bool): True if file is uploaded and False if it is not uploaded
        """

        print(f"Connecting to {self._host}:{self._port} as {auth_dict['username']}")

        # get full path from inited root folder
        remote_root_filepath = Path(self._remote_root_folder).joinpath(remote_filepath)
        local_root_filepath = Path(self._local_root_folder).joinpath(local_filepath)

        # check if file existing
        if not overwrite_existing:
            # check if file existing and quit if existing
            if os.path.basename(remote_root_filepath) in self._list_files(
                auth_dict, os.path.dirname(remote_root_filepath)
            ):
                return False

        # upload file
        self._push_single_file(auth_dict, local_root_filepath, remote_root_filepath)

        return True

    def upload_file_list(
        self,
        auth_dict: dict,
        file_list: list,
        local_path: str = "",
        remote_path: str = "",
        overwrite_existing: bool = False,
    ) -> bool:
        """
        This function will upload a single file.

        Args:
            - auth_dict (dict): authentification dict
            - file_list (list): list of files to upload
            - local_path (str): path of the files to upload
            - remote_path (str): target path of the files
            - overwrite_existing (bool): flag if file should be overwritten if remote_filepath exists

        Result:
            - status (bool): True if file is uploaded and False if it is not uploaded
        """

        # update to system aligned path format
        remote_path = Path(remote_path)
        local_path = Path(local_path)

        print(f"Connecting to {self._host}:{self._port} as {auth_dict['username']}")

        # get full path from inited root folder
        local_root_path = Path(self._local_root_folder).joinpath(local_path)
        remote_root_path = Path(self._remote_root_folder).joinpath(remote_path)

        # check and create directory if needed
        if not str(Path(remote_root_path).name) in self._list_files(
            auth_dict, Path(remote_root_path).parent
        ):
            print(f"creating directoty: {remote_root_path}")
            self._create_dir(auth_dict, remote_root_path)

        # shorten filelist if files available
        if not overwrite_existing:
            file_list = list(
                set(file_list) - set(self._list_files(auth_dict, remote_root_path))
            )

        # return false if no file is available after shorten
        if len(file_list) <= 0:
            return False

        # create local and remote list combination
        file_list = [
            (
                Path(remote_root_path).joinpath(filename),
                Path(local_root_path).joinpath(filename),
            )
            for filename in file_list
        ]

        # upload the files
        self._push_file_list(auth_dict, file_list)

        return True

    def list_files(self, auth_dict: dict, remote_path: str = None) -> list:
        """
        This function will list all files from remote.

        Args:
            - auth_dict (dict): authentification dict
            - remote_path (str): path of remote folder to list the files

        Result:
            - output (list): list of strings with filenames in remote_path
        """

        # adding remote_path if needed
        if remote_path is not None:
            remote_root_path = (
                Path(self._remote_root_folder).joinpath(remote_path).resolve()
            )
        else:
            remote_root_path = Path(self._remote_root_folder).resolve()

        # list files
        print(f"Listing files in {remote_root_path}")
        return self._list_files(auth_dict, remote_root_path)

    def _list_files(self, auth_dict, remote_path):
        raise NotImplementedError()

    def _get_single_file(self, auth_dict, remote_filepath, local_filepath):
        raise NotImplementedError()

    def _get_file_list(self, auth_dict, file_list):
        raise NotImplementedError()

    def _push_single_file(self, auth_dict, local_filepath, remote_filepath):
        raise NotImplementedError()

    def _push_file_list(self, auth_dict, file_list):
        raise NotImplementedError()

    def _upload_folder(self, auth_dict, local_filepath, remote_filepath):
        raise NotImplementedError()

    def _create_dir(self, auth_dict, remote_dir):
        raise NotImplementedError()


class ftpConnector(remotefiletransfer):
    def __init__(self, host, port, root_folder, local_root):
        super().__init__(host, port, root_folder, local_root)

    def _list_files(self, auth_dict, remote_path):
        with ftplib.FTP(self._host) as ftp:
            ftp.login(self._add_domain(auth_dict["username"]), auth_dict["password"])
            ftp.cwd(str(remote_path))
            files_list = ftp.nlst()

        return files_list

    def _get_single_file(self, auth_dict, remote_filepath, local_filepath):
        with ftplib.FTP(self._host) as ftp:
            ftp.login(self._add_domain(auth_dict["username"]), auth_dict["password"])
            with open(local_filepath, "wb") as local_file:
                print(f"Downloading file: {remote_filepath}")
                ftp.retrbinary(f"RETR {remote_filepath}", local_file.write)

    def _get_file_list(self, auth_dict, file_list):
        with ftplib.FTP(self._host) as ftp:
            ftp.login(self._add_domain(auth_dict["username"]), auth_dict["password"])
            for remote_filepath, local_filepath in file_list:
                with open(local_filepath, "wb") as local_file:
                    print(f"Downloading file: {remote_filepath}")
                    ftp.retrbinary(f"RETR {remote_filepath}", local_file.write)

    def _push_single_file(self, auth_dict, local_filepath, remote_filepath):
        with ftplib.FTP(self._host) as ftp, open(local_filepath, "rb") as local_file:
            ftp.login(self._add_domain(auth_dict["username"]), auth_dict["password"])
            print(f"Uploading file: {local_filepath}")
            ftp.storbinary(f"STOR {remote_filepath}", local_file)

    def _push_file_list(self, auth_dict, file_list):
        with ftplib.FTP(self._host) as ftp:
            ftp.login(self._add_domain(auth_dict["username"]), auth_dict["password"])
            for remote_filepath, local_filepath in file_list:
                with open(local_filepath, "rb") as local_file:
                    print(f"Uploading file: {local_filepath}")
                    ftp.storbinary(f"STOR {remote_filepath}", local_file)

    def _create_dir(self, auth_dict, remote_dir):
        with ftplib.FTP(self._host) as ftp:
            ftp.login(self._add_domain(auth_dict["username"]), auth_dict["password"])
            ftp.mkd(str(remote_dir))
        return True

    @staticmethod
    def _add_domain(username):
        return f"eu\\{username}"


class sftpConnector(remotefiletransfer):
    def __init__(self, host, port, root_folder, local_root):
        super().__init__(host, port, root_folder, local_root)

    def _list_files(self, auth_dict, remote_path):
        remote_path = Path(remote_path).as_posix()
        transport = paramiko.Transport((self._host, self._port))
        transport.connect(**auth_dict)
        sftp = transport.open_sftp_client()

        files_list = sftp.listdir(remote_path)

        sftp.close()
        transport.close()

        return files_list

    def _get_single_file(self, auth_dict, remote_filepath, local_filepath):
        remote_filepath = Path(remote_filepath).as_posix()
        transport = paramiko.Transport((self._host, self._port))
        transport.connect(**auth_dict)
        sftp = transport.open_sftp_client()

        print(f"Downloading file: {remote_filepath}")
        sftp.get(remote_filepath, local_filepath)

        sftp.close()
        transport.close()

    def _get_file_list(self, auth_dict, file_list):
        transport = paramiko.Transport((self._host, self._port))
        transport.connect(**auth_dict)
        sftp = transport.open_sftp_client()

        for remote_filepath, local_filepath in file_list:
            remote_filepath = Path(remote_filepath).as_posix()
            print(f"Downloading file: {remote_filepath}")
            sftp.get(remote_filepath, local_filepath)

        sftp.close()
        transport.close()

    def _push_single_file(self, auth_dict, local_filepath, remote_filepath):
        remote_filepath = Path(remote_filepath).as_posix()
        transport = paramiko.Transport((self._host, self._port))
        transport.connect(**auth_dict)
        sftp = transport.open_sftp_client()
        print(f"Uploading file: {local_filepath}")
        sftp.put(local_filepath, remote_filepath)

        sftp.close()
        transport.close()


if __name__ == "__main__":
    # get authentification information from enviroment
    AUTH_DICT = {
        "username": os.environ["AUTH_USERNAME"],
        "password": os.environ["AUTH_PASSWORD"],
    }

    # example connection to sns
    SNS_INFO = {
        "host": "ftpsns-fr.eu.airbus.corp",
        "port": 21,
        "root_folder": os.path.join("Apps", "HUMAN RESOURCES"),
        "local_root": "",
    }
    sns = ftpConnector(**SNS_INFO)

    # list available files / folders
    print(sns.list_files(AUTH_DICT))

    # example connection to fts+
    FTS_INFO = {
        "host": "ftsplus.airbus.corp",
        "port": 22,
        "root_folder": os.path.join("HR-People-Analytics", "Staffing", "Movement"),
        "local_root": "",
    }
    fts = sftpConnector(**FTS_INFO)

    fts.list_files(AUTH_DICT)
    files = fts.list_files(AUTH_DICT)
    file = files[-1]
    fts.download_file(AUTH_DICT, remote_filepath=file, local_filepath=file)
    fts.download_file_list(AUTH_DICT, files)
