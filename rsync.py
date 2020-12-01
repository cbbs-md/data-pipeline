import os

import utils


def sync_data_via_rsync(
    subjects: list, 
    user: str, 
    host: str, 
    src_path: str, 
    target_path: str):
    """Gets the data from a remote server by using rsync.

    Args:
        subjects: List of subjects.
        user: The username used for logging.
        host: The hostname of the remote server.
        src_path: The path on the remote system to get data from.
        target_path: The path on the local system to store the data to.
    """

    for i in subjects:

        # TODO check correct command
        cmd = (
            "rsync --acvg --progress --info=FLIST0 {user}@{host}:{src_path} {target_path}"
            .format(
                user=user,
                host=hostname,
                path=data_path.format(acq=i[acq], target=dir_target)
            )
        )
        print("running command:\n", cmd)

        os.system(cmd)


if __name__ == "__main__":

    config = utils.get_config(filename="config.yaml")
    subjects = utils.read_subjects(filename=config["subject_file"])
    #sync_data_via_rsync(subjects=subjects, config=config)
