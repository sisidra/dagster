from dagster import job, op
from dagster._core.definitions.decorators.op_decorator import CODE_ORIGIN_TAG_NAME
from dagster._utils import file_relative_path
import inspect


def _code_origin_tag(line_no: int) -> str:
    return file_relative_path(__file__, f"test_tags.py:{line_no}")


def test_solid_tags():
    expected_line = inspect.currentframe().f_lineno + 2

    @op(tags={"foo": "bar"})
    def tags_op(_):
        pass

    assert tags_op.tags == {
        "foo": "bar",
        CODE_ORIGIN_TAG_NAME: _code_origin_tag(expected_line),
    }

    expected_line = inspect.currentframe().f_lineno + 2

    @op()
    def no_tags_op(_):
        pass

    assert no_tags_op.tags == {
        CODE_ORIGIN_TAG_NAME: _code_origin_tag(expected_line),
    }


def test_job_tags():
    @job(tags={"foo": "bar"})
    def tags_job():
        pass

    assert tags_job.tags == {"foo": "bar"}

    @job
    def no_tags_job():
        pass

    assert no_tags_job.tags == {}


def test_solid_subset_tags():
    @op
    def noop_op(_):
        pass

    @job(tags={"foo": "bar"})
    def tags_job():
        noop_op()

    assert tags_job.get_job_def_for_subset_selection(op_selection=["noop_op"]).tags == {
        "foo": "bar"
    }

    @job
    def no_tags_job():
        noop_op()

    assert no_tags_job.get_pipeline_subset_def({"noop_op"}).tags == {}
