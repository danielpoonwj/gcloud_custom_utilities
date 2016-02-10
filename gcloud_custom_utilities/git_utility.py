import sys
import os
import shutil
import git


class Progress(git.RemoteProgress):
    def line_dropped(self, line):
        print line

    def update(self, *args):
        print self._cur_line


def git_clone(git_url, write_dir, overwrite_warning=True):
    if os.path.isdir(write_dir):
        if overwrite_warning:
            print 'WARNING: Folder exists.\n\t%s\n\nDelete folder and continue?' % write_dir
            response = raw_input('[y/n]: ').strip().lower()
            while response not in ('y', 'n'):
                response = raw_input('Response').strip().lower()

            if response == 'n':
                sys.exit('Git Clone cancelled')

        print '\nExisting Folder Deleted\n'
        shutil.rmtree(write_dir)

    os.mkdir(write_dir)
    git.Repo.clone_from(git_url, write_dir, progress=Progress())


def git_fetch(repo_dir):
    repo = git.Repo(repo_dir)
    origin = repo.remote(name='origin')

    for fetch_info in origin.fetch(progress=Progress()):
        print("Updated %s to %s" % (fetch_info.ref, fetch_info.commit))
        print
