"""Unit tests for applypilot.scoring.scorer — response parsing and scoring."""

import pytest
from unittest.mock import patch, MagicMock

from applypilot.scoring.scorer import _parse_score_response, score_job


# ── _parse_score_response ─────────────────────────────────────────────────


class TestParseScoreResponse:
    def test_valid_response(self):
        resp = "SCORE: 8\nKEYWORDS: Python, FastAPI, REST\nREASONING: Strong match."
        result = _parse_score_response(resp)
        assert result["score"] == 8
        assert result["keywords"] == "Python, FastAPI, REST"
        assert result["reasoning"] == "Strong match."

    def test_clamps_to_min_1(self):
        result = _parse_score_response("SCORE: 0\nKEYWORDS: \nREASONING: None.")
        assert result["score"] == 1

    def test_clamps_to_max_10(self):
        result = _parse_score_response("SCORE: 15\nKEYWORDS: \nREASONING: Perfect.")
        assert result["score"] == 10

    def test_non_numeric_score_returns_zero(self):
        result = _parse_score_response("SCORE: NaN\nKEYWORDS: \nREASONING: Error.")
        assert result["score"] == 0

    def test_missing_score_line_returns_zero(self):
        result = _parse_score_response("KEYWORDS: Python\nREASONING: Good fit.")
        assert result["score"] == 0

    def test_missing_keywords_defaults_empty(self):
        result = _parse_score_response("SCORE: 7\nREASONING: Good fit.")
        assert result["keywords"] == ""

    def test_extracts_first_digit_in_score(self):
        result = _parse_score_response("SCORE: 9 (out of 10)\nKEYWORDS: \nREASONING: Great.")
        assert result["score"] == 9

    def test_multiline_reasoning_uses_first_line(self):
        resp = "SCORE: 7\nKEYWORDS: Python\nREASONING: Good fit overall."
        result = _parse_score_response(resp)
        assert "Good fit" in result["reasoning"]


# ── score_job ─────────────────────────────────────────────────────────────


def _make_job(**overrides):
    defaults = {
        "title": "Backend Engineer",
        "site": "Acme Corp",
        "location": "Remote",
        "full_description": "We need a Python developer with FastAPI experience.",
        "url": "http://example.com/job/1",
    }
    defaults.update(overrides)
    return defaults


class TestScoreJob:
    def test_success(self):
        mock_client = MagicMock()
        mock_client.chat.return_value = (
            "SCORE: 8\nKEYWORDS: Python, FastAPI\nREASONING: Strong match."
        )
        with patch("applypilot.scoring.scorer.get_client", return_value=mock_client):
            result = score_job("Python developer resume", _make_job())
        assert result["score"] == 8
        assert "Python" in result["keywords"]
        assert "_error" not in result

    def test_llm_error_returns_error_dict(self):
        mock_client = MagicMock()
        mock_client.chat.side_effect = RuntimeError("API down")
        with patch("applypilot.scoring.scorer.get_client", return_value=mock_client):
            result = score_job("resume", _make_job())
        assert result["score"] == 0
        assert result["_error"] is True

    def test_truncates_long_description(self):
        job = _make_job(full_description="x" * 10_000)
        captured = []

        mock_client = MagicMock()
        def capture_chat(messages, **kw):
            captured.append(messages)
            return "SCORE: 5\nKEYWORDS: \nREASONING: ok."
        mock_client.chat.side_effect = capture_chat

        with patch("applypilot.scoring.scorer.get_client", return_value=mock_client):
            score_job("resume", job)

        # The user message should contain the truncated description
        user_msg = captured[0][1]["content"]
        # 6000 char truncation + surrounding text < 8000 total
        assert len(user_msg) < 8000

    def test_handles_missing_location(self):
        job = _make_job()
        del job["location"]
        mock_client = MagicMock()
        mock_client.chat.return_value = "SCORE: 5\nKEYWORDS: \nREASONING: ok."
        with patch("applypilot.scoring.scorer.get_client", return_value=mock_client):
            result = score_job("resume", job)
        assert result["score"] == 5

    def test_handles_none_description(self):
        job = _make_job(full_description=None)
        mock_client = MagicMock()
        mock_client.chat.return_value = "SCORE: 3\nKEYWORDS: \nREASONING: No info."
        with patch("applypilot.scoring.scorer.get_client", return_value=mock_client):
            result = score_job("resume", job)
        assert result["score"] == 3

    def test_system_prompt_is_scoring_prompt(self):
        captured = []
        mock_client = MagicMock()
        def capture_chat(messages, **kw):
            captured.append(messages)
            return "SCORE: 7\nKEYWORDS: \nREASONING: ok."
        mock_client.chat.side_effect = capture_chat

        with patch("applypilot.scoring.scorer.get_client", return_value=mock_client):
            score_job("resume", _make_job())

        assert captured[0][0]["role"] == "system"
        assert "SCORING CRITERIA" in captured[0][0]["content"]
