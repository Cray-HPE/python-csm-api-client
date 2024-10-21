#
# MIT License
#
# (C) Copyright 2021-2024 Hewlett Packard Enterprise Development LP
#
# Permission is hereby granted, free of charge, to any person obtaining a
# copy of this software and associated documentation files (the "Software"),
# to deal in the Software without restriction, including without limitation
# the rights to use, copy, modify, merge, publish, distribute, sublicense,
# and/or sell copies of the Software, and to permit persons to whom the
# Software is furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included
# in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
# THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
"""
Client for interacting with the Version Control Service (VCS).
"""

# TODO (CRAYSAT-1214): This code was originally copied from cfs_config_util/vcs.py
# and should be refactored into a common library.
import base64
from contextlib import contextmanager
from functools import cached_property
import logging
import os
import subprocess
from tempfile import NamedTemporaryFile
from typing import (
    Dict,
    Generator,
    Optional,
    List,
)
from urllib.parse import urlparse, urlunparse

from kubernetes.client import CoreV1Api
from kubernetes.config.config_exception import ConfigException

from csm_api_client.k8s import load_kube_api


LOGGER = logging.getLogger(__name__)


class VCSError(Exception):
    """An error occurring during VCS access."""


@contextmanager
def vcs_creds_helper() -> Generator[Dict[str, str], None, None]:
    """Context manager to set up a helper script to get VCS credentials.

    Yields:
        A dictionary containing current process environment with GIT_ASKPASS
            set to the path to the script for obtaining VCS credentials.
    """
    # Get the password directly from k8s to avoid leaking it via the /proc filesystem
    try:
        with NamedTemporaryFile(delete=False) as cred_helper_script:
            cred_helper_script.write('#!/bin/bash\n'
                                     'kubectl get secret -n services vcs-user-credentials '
                                     '--template={{.data.vcs_password}} | base64 -d'.encode())

        os.chmod(cred_helper_script.name, 0o700)  # Make executable by user
        env = dict(os.environ)
        env.update(GIT_ASKPASS=cred_helper_script.name)
        yield env
    finally:
        os.remove(cred_helper_script.name)


class VCSRepo:
    """Main client object for accessing a VCS repository."""

    def __init__(self, clone_url: str) -> None:
        """Constructor for VCSRepo.

        Args:
            clone_url: The clone URL of the git repo.
        """
        parsed_url = urlparse(clone_url)
        if '@' in parsed_url.netloc:
            username, netloc = parsed_url.netloc.split('@', maxsplit=1)
            LOGGER.warning(f'Ignoring username {username} specified in clone URL {clone_url}')
            clone_url = urlunparse(parsed_url._replace(netloc=netloc))

        self.clone_url = clone_url

    @cached_property
    def repo_path(self) -> str:
        """The path component of the clone_url"""
        parsed_url = urlparse(self.clone_url)
        return parsed_url.path

    @cached_property
    def repo_name(self) -> str:
        """The name of the repo"""
        return os.path.basename(os.path.splitext(self.repo_path)[0])

    @cached_property
    def vcs_username(self) -> str:
        """The VCS username from a Kubernetes secret

        Raises:
            VCSError: if unable to read the vcs_username from the K8s secret.
        """
        try:
            k8s_api: CoreV1Api = load_kube_api()
        except ConfigException as exc:
            raise VCSError(f'Unable to load Kubernetes API: {exc}')

        username = k8s_api.read_namespaced_secret('vcs-user-credentials', 'services').data.get('vcs_username')
        if username is None:
            raise VCSError('Unable to get VCS username from Kubernetes secret.')

        return base64.b64decode(username).decode('utf-8').strip()

    @property
    def user_clone_url(self) -> str:
        """The clone url including the username"""
        parsed_url = urlparse(self.clone_url)
        user_netloc = f'{self.vcs_username}@{parsed_url.netloc}'
        return urlunparse(parsed_url._replace(netloc=user_netloc))

    @staticmethod
    def run_authenticated_git_cmd(git_cmd: List[str]) -> subprocess.CompletedProcess:
        """Run the given git command, authenticated using credentials from the K8s secret.

        Args:
            git_cmd: the git command to run

        Returns:
            The completed process

        Raises:
            subprocess.CalledProcessError: if the command fails
        """
        with vcs_creds_helper() as env:
            return subprocess.run(git_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                  env=env, check=True)

    @property
    def remote_refs(self) -> Dict[str, str]:
        """Get the remote refs for a remote repo.

        Returns:
            Mapping of remote refs to their corresponding commit hashes

        Raises:
            VCSError: if there is an error when git accesses VCS to enumerate
                remote refs
        """
        try:
            proc = self.run_authenticated_git_cmd(['git', 'ls-remote', self.user_clone_url])
        except subprocess.CalledProcessError as err:
            raise VCSError(f"Error accessing VCS: {err}") from err

        # Each line in the output from `git ls-remote` has the form
        # "<commit_hash>\t<ref_name>", and we want the returned dictionary to map
        # the other way, i.e. from ref names to commit hashes.
        remote_refs = {}
        for line in proc.stdout.decode('utf-8').split('\n'):
            if not line:
                continue
            commit_hash, ref_name = line.split('\t')
            remote_refs[ref_name] = commit_hash
        return remote_refs

    def get_commit_hash_for_branch(self, branch: str) -> Optional[str]:
        """Get the commit hash corresponding to the HEAD of some branch.

        Args:
            branch: the name of the branch

        Returns:
            A commit hash corresponding to the given branch, or None if the
            given branch is not found.
        """
        target_ref = f'refs/heads/{branch}'
        return self.remote_refs.get(target_ref)

    def clone(
        self,
        branch: Optional[str] = None,
        directory: Optional[str] = None,
        single_branch: bool = False,
        depth: Optional[int] = None
    ) -> None:
        """Clone the VCS repo to the given path.

        Args:
            branch: the branch to set as HEAD in the cloned repo.
                If omitted, defaults to the HEAD of the remote repo.
            directory: the directory to clone the repo to.
                If omitted, clone to a directory matching the name of the repo.
            single_br: whether to clone just a single branch.
            depth: create a shallow clone with history truncated
                 to this number of commits.
        """
        git_cmd = ['git', 'clone']
        if branch:
            git_cmd += ['--branch', branch]
        if single_branch:
            git_cmd += ['--single-branch']
        if depth:
            git_cmd += ['--depth', str(depth)]
        git_cmd.append(self.user_clone_url)
        if directory:
            git_cmd.append(directory)

        try:
            self.run_authenticated_git_cmd(git_cmd)
        except subprocess.CalledProcessError as err:
            raise VCSError(f"Error cloning VCS repo: {err}") from err
