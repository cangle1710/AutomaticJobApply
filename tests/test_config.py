"""Unit tests for applypilot.config — paths, tiers, profile loading."""

import json
import os

import pytest
from unittest.mock import patch

from applypilot import config as cfg


# ── Tier system ───────────────────────────────────────────────────────────


class TestGetTier:
    def test_tier_1_no_llm_key(self):
        env = {"GEMINI_API_KEY": "", "OPENAI_API_KEY": "", "LLM_URL": ""}
        with patch.dict(os.environ, env, clear=False):
            assert cfg.get_tier() == 1

    def test_tier_2_with_llm_key(self):
        env = {"GEMINI_API_KEY": "test-key", "OPENAI_API_KEY": "", "LLM_URL": ""}
        with patch.dict(os.environ, env, clear=False):
            with patch("applypilot.config.shutil.which", return_value=None):
                assert cfg.get_tier() == 2

    def test_tier_2_when_chrome_missing(self):
        env = {"GEMINI_API_KEY": "test-key"}
        with patch.dict(os.environ, env, clear=False):
            with patch("applypilot.config.shutil.which", return_value="/usr/bin/claude"):
                with patch(
                    "applypilot.config.get_chrome_path",
                    side_effect=FileNotFoundError,
                ):
                    assert cfg.get_tier() == 2

    def test_tier_3_all_present(self):
        env = {"GEMINI_API_KEY": "test-key"}
        with patch.dict(os.environ, env, clear=False):
            with patch("applypilot.config.shutil.which", return_value="/usr/bin/claude"):
                with patch(
                    "applypilot.config.get_chrome_path",
                    return_value="/usr/bin/google-chrome",
                ):
                    assert cfg.get_tier() == 3


# ── Profile loading ──────────────────────────────────────────────────────


class TestLoadProfile:
    def test_loads_valid_profile(self, tmp_path):
        data = {"personal": {"full_name": "Test"}, "skills_boundary": {}}
        path = tmp_path / "profile.json"
        path.write_text(json.dumps(data), encoding="utf-8")

        with patch.object(cfg, "PROFILE_PATH", path):
            profile = cfg.load_profile()
        assert profile["personal"]["full_name"] == "Test"

    def test_raises_when_missing(self, tmp_path):
        with patch.object(cfg, "PROFILE_PATH", tmp_path / "nonexistent.json"):
            with pytest.raises(FileNotFoundError):
                cfg.load_profile()


# ── Search config loading ────────────────────────────────────────────────


class TestLoadSearchConfig:
    def test_falls_back_to_example(self, tmp_path):
        # Point SEARCH_CONFIG_PATH to nonexistent file; CONFIG_DIR to dir with example
        example_dir = tmp_path / "config"
        example_dir.mkdir()
        example = example_dir / "searches.example.yaml"
        example.write_text("queries:\n  - query: python developer\n", encoding="utf-8")

        with patch.object(cfg, "SEARCH_CONFIG_PATH", tmp_path / "missing.yaml"):
            with patch.object(cfg, "CONFIG_DIR", example_dir):
                result = cfg.load_search_config()
        assert "queries" in result

    def test_returns_empty_when_nothing_exists(self, tmp_path):
        with patch.object(cfg, "SEARCH_CONFIG_PATH", tmp_path / "missing.yaml"):
            with patch.object(cfg, "CONFIG_DIR", tmp_path / "nodir"):
                result = cfg.load_search_config()
        assert result == {}


# ── Defaults ──────────────────────────────────────────────────────────────


class TestDefaults:
    def test_min_score(self):
        assert cfg.DEFAULTS["min_score"] == 7

    def test_max_tailor_attempts(self):
        assert cfg.DEFAULTS["max_tailor_attempts"] == 5

    def test_max_apply_attempts(self):
        assert cfg.DEFAULTS["max_apply_attempts"] == 3


# ── is_manual_ats ─────────────────────────────────────────────────────────


class TestIsManualAts:
    def test_none_returns_false(self):
        assert cfg.is_manual_ats(None) is False

    def test_empty_string_returns_false(self):
        assert cfg.is_manual_ats("") is False


# ── ensure_dirs ──────────────────────────────────────────────────────────


class TestEnsureDirs:
    def test_creates_directories(self, tmp_path):
        with (
            patch.object(cfg, "APP_DIR", tmp_path / "app"),
            patch.object(cfg, "TAILORED_DIR", tmp_path / "tailored"),
            patch.object(cfg, "COVER_LETTER_DIR", tmp_path / "covers"),
            patch.object(cfg, "LOG_DIR", tmp_path / "logs"),
            patch.object(cfg, "CHROME_WORKER_DIR", tmp_path / "chrome"),
            patch.object(cfg, "APPLY_WORKER_DIR", tmp_path / "apply"),
        ):
            cfg.ensure_dirs()
            assert (tmp_path / "app").is_dir()
            assert (tmp_path / "tailored").is_dir()
            assert (tmp_path / "covers").is_dir()
            assert (tmp_path / "logs").is_dir()


# ── Tier labels ──────────────────────────────────────────────────────────


class TestTierLabels:
    def test_all_tiers_have_labels(self):
        for tier in (1, 2, 3):
            assert tier in cfg.TIER_LABELS

    def test_all_tiers_have_commands(self):
        for tier in (1, 2, 3):
            assert tier in cfg.TIER_COMMANDS
            assert isinstance(cfg.TIER_COMMANDS[tier], list)
