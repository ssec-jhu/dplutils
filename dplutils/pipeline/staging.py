import os
import tempfile
import uuid
from collections import defaultdict
from pathlib import Path

DEFAULT_STAGING_PATH = Path(os.environ.get("DPL_STAGING_PATH", tempfile.gettempdir()))


class FileStager:
    def __init__(self, staging_root):
        if staging_root is None:
            self.staging_root = DEFAULT_STAGING_PATH / str(uuid.uuid1())
        elif not Path(staging_root).is_absolute():
            self.staging_root = DEFAULT_STAGING_PATH / staging_root
        else:
            self.staging_root = Path(staging_root)
        self.staging_root.mkdir(exist_ok=True, parents=True)
        self.usage = defaultdict(int)

    def get(self, name, num=1):
        tag = f"{name}-{uuid.uuid1()}"
        if num > 1:
            return [self.staging_root / f"{tag}-{i}.par" for i in range(num)]
        return self.staging_root / f"{tag}.par"

    def mark_usage(self, file, n=1):
        self.usage[file] += n

    def mark_complete(self, file):
        self.usage[file] -= 1
        if self.usage[file] <= 0:
            os.unlink(file)
        del self.usage[file]
