import pytest

from dplutils.pipeline.task import PipelineTask


def test_pipeline_task_value_replacements():
    t1 = PipelineTask("name", lambda x: 1, num_cpus=1)
    t2 = t1("newname", num_cpus=10)
    assert t1.num_cpus == 1
    assert t1.name == "name"
    assert t2.name == "newname"
    assert t2.num_cpus == 10


def test_pipeline_task_resolve_kwargs_returns_dict(generic_task):
    assert isinstance(generic_task.resolve_kwargs({}), dict)


def test_pipeline_task_resolve_kwargs_adds_from_context(generic_task):
    task = generic_task(context_kwargs=dict(optional="ctx_optional"))
    assert task.resolve_kwargs({"ctx_optional": "value"}) == dict(optional="value")


def test_pipeline_task_validation_passes_with_required(generic_task):
    generic_task.validate({})


def test_pipeline_task_validation_raises_with_missing_required(generic_task_with_required):
    with pytest.raises(ValueError):
        generic_task_with_required.validate({})


def test_pipeline_task_validation_raises_with_extra_args(generic_task):
    with pytest.raises(ValueError):
        generic_task(kwargs=dict(extra=1)).validate({})


def test_pipeline_task_check_kwargs_from_context(generic_task_with_required):
    generic_task_with_required(context_kwargs={"required": "ctx_required"}).validate({"ctx_required": 1})


def test_pipeline_task_check_kwargs_from_context_raises_with_missing(generic_task_with_required):
    with pytest.raises(ValueError):
        generic_task_with_required(context_kwargs={"required": "ctx_required"}).validate({})
