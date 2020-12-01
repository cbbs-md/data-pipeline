import os

import utils


def _get_path(conf, acq):
    user = conf.get("user", None)
    host = conf.get("host", None)
    path = conf["path"].format(acq)

    if user is None or host is None:
        return path
    else:
        return "{user}@{host}:{path}".format(user=user, host=host, path=path)


def sync_data_via_rsync(subjects: list, config: dict):
    """Gets the data from a remote server by using rsync.

    Args:
        subjects: List of subjects.
        config: A dictionary containing the rsync parameters. It has to containt
            the following keys:
                src:
                    user: The username with which get data.
                    host: The name of the source server to get data from.
                    path: The path on the source system to get data from.
                dest:
                    user: The username to store the data as.
                    host: The name of the target server to store the data to.
                    path: The path on the target system to store the data to.
    """

    for key in ["src", "dest"]:
        if key not in config["rsync"].keys():
            raise utils.ConfigError("Missing {} configuration.".format(key))

    for i in subjects:
        src = _get_path(config["rsync"]["src"], i["acq"])
        dest = _get_path(config["rsync"]["dest"], i["acq"])

        # rsync options used:
        # -a: archive mode; equals -rlptgoD
        #    r: recursive
        #    l: copy symlinks as symlinks
        #    p: preserve permissions
        #    t: preserve modification times
        #    g: preserve group
        #    o: preserve owner
        #    D: preserve device and special files
        # -c: skip based on checksum, not mod-time and size
        # -v: verbose
        # --progress: show progress during transfer
        # --info=FLAG: fine-grained informational verbosity
        #    FLIST: Mention file-list receiving/sending (levels 1-2)
        cmd = (
            "rsync -acv --progress --info=FLIST0 {src} {dest}"
            .format(src=src, dest=dest)
        )
        print("running command:\n", cmd)

        os.system(cmd)

        # Todo get status and return value


if __name__ == "__main__":

    config = utils.get_config(filename="config.yaml")
    subjects = utils.read_subjects(filename=config["subject_file"])
    #sync_data_via_rsync(subjects=subjects, config=config)
