import os
import uuid
from collections import defaultdict
from pathlib import Path

from dplutils.pipeline import utils

DEFAULT_STAGING_PATH = Path(os.environ.get("DPL_STAGING_PATH", utils.TEMP_DIR))


class FileStager:
    """Managing temporary files for a pipeline.

    This class manages the temporary files for pipelines that do not have global
    memory, or otherwise should stage output/input via the filesystem. It tracks
    task references and removes files when they are no longer needed.

    This class just provides filenames, it does not handle writing, however it
    will handle removal of files when reference count reaches zero.

    Args:
        staging_root (str or Path, optional): The root directory for staging files.
            If not provided, a default temporary directory is used. If provided,
            it should be an absolute path or relative to the default staging
            path (environment variable `DPL_STAGING_PATH`).
    """

    def __init__(self, staging_root=None):
        if staging_root is None:
            self.staging_root = DEFAULT_STAGING_PATH / str(uuid.uuid1())
        elif not Path(staging_root).is_absolute():
            self.staging_root = DEFAULT_STAGING_PATH / staging_root
        else:
            self.staging_root = Path(staging_root)
        self.staging_root.mkdir(exist_ok=True, parents=True)
        self.usage = defaultdict(int)

    def get(self, name, num=1):
        """Get one or more paths for staging files."""
        tag = f"{name}-{uuid.uuid1()}"
        if num > 1:
            return [self.staging_root / f"{tag}-{i}.par" for i in range(num)]
        return self.staging_root / f"{tag}.par"

    def mark_usage(self, file, n=1):
        """Mark a file as being used by a task.

        When feeding to multiple tasks, use `n` to indicate how many tasks will
        consume the file.
        """
        self.usage[file] += n

    def mark_complete(self, file):
        """Mark a file as no longer needed by a task.

        Files that are no longer needed will be deleted if when their reference
        count reaches zero.
        """
        self.usage[file] -= 1
        if self.usage[file] <= 0:
            os.unlink(file)
        del self.usage[file]
