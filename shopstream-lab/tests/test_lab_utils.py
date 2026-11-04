"""Unit test cho experiments/lab_utils.py, không cần Kafka hay Spark."""

import importlib.util
import json
from pathlib import Path

import pytest

from common import config

spec = importlib.util.spec_from_file_location(
    "lab_utils", Path(config.PROJECT_DIR) / "experiments" / "lab_utils.py"
)
lab_utils = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab_utils)


@pytest.fixture
def fake_project(tmp_path):
    lakehouse = tmp_path / "lakehouse"
    checkpoints = tmp_path / "checkpoints"
    for directory in (lakehouse / "silver", lakehouse / "exp_a" / "silver",
                      checkpoints / "silver", checkpoints / "exp_a" / "silver"):
        directory.mkdir(parents=True)
        (directory / "file.txt").write_text("x")
    return lakehouse, checkpoints


def test_safe_remove_deletes_exp_directory(fake_project):
    lakehouse, checkpoints = fake_project
    assert lab_utils.safe_remove_experiment_dir(lakehouse / "exp_a", (lakehouse, checkpoints))
    assert not (lakehouse / "exp_a").exists()
    assert lab_utils.safe_remove_experiment_dir(checkpoints / "exp_a" / "silver", (lakehouse, checkpoints))
    assert not (checkpoints / "exp_a" / "silver").exists()


@pytest.mark.parametrize("relative", ["silver", ".", "..", "exp_a/../silver"])
def test_safe_remove_refuses_paths_outside_exp(fake_project, relative):
    lakehouse, checkpoints = fake_project
    with pytest.raises(ValueError):
        lab_utils.safe_remove_experiment_dir(lakehouse / relative, (lakehouse, checkpoints))
    assert (lakehouse / "silver" / "file.txt").exists()


def test_safe_remove_refuses_data_and_results_dirs(fake_project, tmp_path):
    lakehouse, checkpoints = fake_project
    for name in ("data", "results", "exp_a"):
        (tmp_path / name).mkdir(exist_ok=True)
        with pytest.raises(ValueError):
            lab_utils.safe_remove_experiment_dir(tmp_path / name, (lakehouse, checkpoints))
        assert (tmp_path / name).exists()


def test_experiment_dirs_require_exp_prefix(tmp_path):
    lakehouse_dir, checkpoint_dir = lab_utils.experiment_dirs("exp_b", tmp_path / "l", tmp_path / "c")
    assert lakehouse_dir == tmp_path / "l" / "exp_b"
    assert checkpoint_dir == tmp_path / "c" / "exp_b"
    with pytest.raises(ValueError):
        lab_utils.experiment_dirs("silver")


def test_snapshot_state_writes_offsets_commits_and_delta_log(tmp_path):
    checkpoint = tmp_path / "checkpoints" / "silver"
    (checkpoint / "offsets").mkdir(parents=True)
    (checkpoint / "commits").mkdir(parents=True)
    (checkpoint / "offsets" / "0").write_text("v1\n{\"batchWatermarkMs\":0}\n")
    (checkpoint / "offsets" / "10").write_text("v1\n{}\n")
    (checkpoint / "offsets" / ".0.crc").write_text("bỏ qua")
    table = tmp_path / "lakehouse" / "silver"
    (table / "_delta_log").mkdir(parents=True)
    (table / "_delta_log" / "00000000000000000000.json").write_text('{"commitInfo":{}}\n')
    output = lab_utils.snapshot_state([checkpoint], [table], tmp_path / "snap.txt", "trước")
    text = output.read_text(encoding="utf-8")
    assert "## silver/offsets" in text and "## silver/commits" in text
    assert "--- 0 ---" in text and "batchWatermarkMs" in text
    assert "(rỗng)" in text  # commits rỗng
    assert ".0.crc" not in text
    assert "00000000000000000000.json" in text
    # Sắp theo số: 0 trước 10
    assert text.index("--- 0 ---") < text.index("--- 10 ---")


def test_list_names_sorted_numerically(tmp_path):
    for name in ("10", "2", "1", ".1.crc"):
        (tmp_path / name).write_text("")
    assert lab_utils.list_names(tmp_path) == ["1", "2", "10"]


def test_make_and_write_result(tmp_path):
    result = lab_utils.make_result("exp_x", expected=100, actual=112, failed_batch_id=4)
    assert result["difference"] == 12
    output = lab_utils.write_result(result, tmp_path)
    assert json.loads(output.read_text(encoding="utf-8"))["failed_batch_id"] == 4


def test_next_batch_id_from_offsets(tmp_path):
    assert lab_utils.next_batch_id(tmp_path) == 0
    (tmp_path / "offsets").mkdir()
    for name in ("0", "1", "11", ".11.crc"):
        (tmp_path / "offsets" / name).write_text("")
    assert lab_utils.next_batch_id(tmp_path) == 12


def test_delta_commits_reads_operation_and_metrics(tmp_path):
    log_dir = tmp_path / "_delta_log"
    log_dir.mkdir()
    (log_dir / "00000000000000000000.json").write_text(
        '{"commitInfo":{"operation":"WRITE","operationMetrics":{"numOutputRows":"0"}}}\n')
    (log_dir / "00000000000000000001.json").write_text(
        '{"add":{"path":"a.parquet"}}\n{"add":{"path":"b.parquet"}}\n'
        '{"commitInfo":{"operation":"MERGE","operationMetrics":{"numTargetRowsInserted":"7"}}}\n')
    (log_dir / "00000000000000000001.crc").write_text("{}")
    commits = lab_utils.delta_commits(tmp_path)
    assert [commit["version"] for commit in commits] == [0, 1]
    assert commits[1]["operation"] == "MERGE"
    assert commits[1]["metrics"]["numTargetRowsInserted"] == "7"
    assert commits[1]["added_files"] == 2
