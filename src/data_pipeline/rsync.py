""" Import data via rsync """

import subprocess

import data_pipeline.utils as utils


def _get_path(config, acq):
    user = config.get("user", None)
    host = config.get("host", None)
    path = config["path"].format(acq)

    if user is None or host is None:
        return path

    return "{user}@{host}:{path}".format(user=user, host=host, path=path)


def sync_data_via_rsync(subjects: list, config: dict):
    """Gets the data from a remote server by using rsync.

    Args:
        subjects: List of subjects.
        config: A dictionary containing the rsync parameters. It has to
            constraint the following keys:
                src:
                    user: The username with which get data.
                    host: The name of the source server to get data from.
                    path: The path on the source system to get data from.
                dest:
                    user: The username to store the data as.
                    host: The name of the target server to store the data to.
                    path: The path on the target system to store the data to.
    """
    log = utils.setup_logging()

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
        cmd = ["rsync", "-acv", "--progress", "--info=FLIST0", src, dest]
        log.info("running command: %s\n", " ".join(cmd))

        try:
            proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE, text=True)
            (output, error) = proc.communicate()
            proc.wait()
        except Exception:  # pylint: disable=broad-except
            log.error("Error with command execution", exec_info=True)

        if proc.returncode:
            log.debug("Output of rsync command was %s", output)
            log.error("An error occured in rsync executing:\n %s", error)
            # TODO react to this


def _main():

    config = utils.get_config(filename="config.yaml")
    subject = utils.read_subjects(filename=config["subject_file"])
    sync_data_via_rsync(subjects=subject, config=config)


if __name__ == "__main__":
    _main()
