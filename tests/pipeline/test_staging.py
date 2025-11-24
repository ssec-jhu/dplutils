from dplutils.pipeline.staging import DEFAULT_STAGING_PATH, FileStager


def test_staging(tmp_path):
    stager = FileStager(tmp_path)
    assert tmp_path.is_dir()
    fname = stager.get("test")
    assert fname.parent == tmp_path
    assert fname.name.startswith("test")

    stager = FileStager()
    assert stager.get("test").parent.parent == DEFAULT_STAGING_PATH

    stager = FileStager("test_root")
    assert stager.get("test").parent == DEFAULT_STAGING_PATH / "test_root"


def test_staging_multiple_get(tmp_path):
    stager = FileStager(tmp_path)
    fnames = stager.get("test", 3)
    assert len(fnames) == 3
    assert len(set(fnames)) == 3
