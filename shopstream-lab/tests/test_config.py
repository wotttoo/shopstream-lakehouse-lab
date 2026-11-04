from pathlib import Path

from common import config


def test_window_and_watermark_values_match_spec():
    assert config.WINDOW_DURATION == "5 minutes"
    assert config.GOLD_WATERMARK == "10 minutes"
    assert config.SILVER_WATERMARK == "30 minutes"
    assert config.GOLD_COMPARE_WATERMARK == "20 minutes"


def test_topic_name_and_partitions():
    assert config.TOPIC_NAME == "shop_events"
    assert config.TOPIC_PARTITIONS == 3


def test_table_names_match_spec():
    assert config.BRONZE_PATH.name == "bronze"
    assert config.SILVER_PATH.name == "silver"
    assert config.GOLD_PATH.name == "gold"


def test_paths_are_relative_to_project_not_cwd(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    assert config.PROJECT_DIR == Path(config.__file__).resolve().parent.parent
    assert config.EVENTS_FILE.is_absolute()
    assert config.LAKEHOUSE_DIR.parent == config.PROJECT_DIR


def test_seed_is_42():
    assert config.SEED == 42


def test_table_path_with_suffix():
    assert config.table_path("gold", "_wm20") == config.LAKEHOUSE_DIR / "gold_wm20"
    assert config.checkpoint_path("silver") == config.CHECKPOINT_DIR / "silver"
