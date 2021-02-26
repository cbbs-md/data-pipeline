""" Baisc git commands """

from pathlib import Path

import data_pipeline.utils as utils


class GitBase():
    """ basic git command """

    def __init__(self):
        self.log = utils.get_logger(__class__)  # type: ignore

    def _get_current_branch(self):
        return self._run_cmd(["git", "branch", "--show-current"])

    def checkout_branch(self, branch, rebase_branch="", do_create=False):
        """ Switch to branch dedicated for bids config

        Args:
            branch: The branch to switch to
            do_create: Optional; if the branch should be created if it does
                not exist
        """
        if self.check_if_branch_exists(branch) or not do_create:
            if rebase_branch:
                cmd = ["git", "checkout", branch]
                self._run_cmd(cmd)
                cmd = ["git", "rebase", rebase_branch]
            else:
                self.log.debug("branch '%s' exists -> switch", branch)
                cmd = ["git", "checkout", branch]
        else:
            self.log.debug("branch '%s' does not exists "
                           "-> create and switch", branch)
            cmd = ["git", "checkout", "-b", branch]

        self._run_cmd(cmd)

    @staticmethod
    def check_if_branch_exists(branch: str) -> bool:
        """ Check if a branch exists

        Args:
            branch: The name of the branch to check
        Returns:
            True if the branch exists and False if not.
        """
        return utils.check_cmd(
            ["git", "show-ref", "--verify", "--quiet",
             "refs/heads/" + branch]
        )

    def _run_cmd(self, cmd):
        return utils.run_cmd(cmd, self.log).rstrip()

    def stash(self, pop: bool = False):
        """ Interact with stash

        Either puts the changes on the stash or pops them from the it.

        Args:
            pop: retrive changes from stash or not
        """
        if pop:
            cmd = ["git", "stash", "pop"]
        else:
            cmd = ["git", "stash"]

        # do not react on exceptions
        utils.check_cmd(cmd)

    @staticmethod
    def was_changed(path: str) -> bool:
        """ Check if a file has changed,

        Args:
            path: The path to check
        Returns:
            True if path has changed, False otherwise
        """
        if not Path(path).exists():
            return False

        cmd = ["git", "diff", "--exit-code", path]
        # check_cmd returns True if no exception was thrown, but in this case
        # an exception means, that path was changed
        return not utils.check_cmd(cmd)

    def is_tracked(self, path: str) -> bool:
        """ Check if a path is tracked in git

        Args:
            path: The path to check
        Returns:
            True if path is tracked, False otherwise
        """
#        Raises:
#            Exception if path is not relative from within the repo because
#            this would falsify the result.

#        if Path(path).is_absolute():
#            msg = "path has to be relative from within the repo"
#            self.log.error(msg)
#             raise Exception(msg)

        cmd = ["git", "ls-files", "--error-unmatch", path]
        return utils.check_cmd(cmd)

    @staticmethod
    def remove_branch(branch: str):
        """ Remove a branch

        Args:
            branch: The branch to remove
        """
        cmd = ["git", "branch", "-d", branch]
        utils.check_cmd(cmd)
