"""Unit tests for applypilot.llm — provider detection, client behaviour, singleton."""

import os

import pytest
from unittest.mock import MagicMock, patch

import httpx

from applypilot.llm import (
    LLMClient,
    _detect_provider,
    get_client,
    reset_client,
    _GeminiCompatForbidden,
)


# ── Provider detection ────────────────────────────────────────────────────


class TestDetectProvider:
    def test_gemini_key(self):
        with patch.dict(os.environ, {"GEMINI_API_KEY": "gk", "LLM_URL": ""}, clear=False):
            base_url, model, key = _detect_provider()
        assert "generativelanguage" in base_url
        assert "gemini" in model
        assert key == "gk"

    def test_openai_key(self):
        env = {"GEMINI_API_KEY": "", "OPENAI_API_KEY": "sk-test", "LLM_URL": ""}
        with patch.dict(os.environ, env, clear=False):
            base_url, model, key = _detect_provider()
        assert "openai.com" in base_url
        assert key == "sk-test"

    def test_local_url_takes_priority(self):
        env = {"LLM_URL": "http://localhost:11434/v1", "GEMINI_API_KEY": "ignored"}
        with patch.dict(os.environ, env, clear=False):
            base_url, _, _ = _detect_provider()
        assert base_url == "http://localhost:11434/v1"

    def test_local_url_strips_trailing_slash(self):
        env = {"LLM_URL": "http://localhost:11434/v1/", "GEMINI_API_KEY": ""}
        with patch.dict(os.environ, env, clear=False):
            base_url, _, _ = _detect_provider()
        assert not base_url.endswith("/")

    def test_no_provider_raises(self):
        env = {"GEMINI_API_KEY": "", "OPENAI_API_KEY": "", "LLM_URL": ""}
        with patch.dict(os.environ, env, clear=False):
            with pytest.raises(RuntimeError, match="No LLM provider"):
                _detect_provider()

    def test_model_override(self):
        env = {"GEMINI_API_KEY": "gk", "LLM_MODEL": "gemini-1.5-pro", "LLM_URL": ""}
        with patch.dict(os.environ, env, clear=False):
            _, model, _ = _detect_provider()
        assert model == "gemini-1.5-pro"

    def test_gemini_priority_over_openai(self):
        env = {"GEMINI_API_KEY": "gk", "OPENAI_API_KEY": "ok", "LLM_URL": ""}
        with patch.dict(os.environ, env, clear=False):
            base_url, _, key = _detect_provider()
        assert "generativelanguage" in base_url
        assert key == "gk"


# ── LLMClient.chat ────────────────────────────────────────────────────────


class TestLLMClientChat:
    def _client(self, **overrides):
        defaults = {"base_url": "https://api.openai.com/v1", "model": "gpt-4o-mini", "api_key": "sk-test"}
        defaults.update(overrides)
        return LLMClient(**defaults)

    def _ok_response(self, content="Hello!"):
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.json.return_value = {"choices": [{"message": {"content": content}}]}
        resp.headers = {}
        resp.raise_for_status = MagicMock()
        return resp

    def test_successful_chat(self):
        client = self._client()
        with patch.object(client._client, "post", return_value=self._ok_response("Hi")):
            assert client.chat([{"role": "user", "content": "Hello"}]) == "Hi"

    def test_ask_delegates_to_chat(self):
        client = self._client()
        with patch.object(client, "chat", return_value="Answer") as mock:
            result = client.ask("Question")
        mock.assert_called_once_with([{"role": "user", "content": "Question"}])
        assert result == "Answer"

    def test_retries_on_429(self):
        client = self._client()

        rate_resp = MagicMock(spec=httpx.Response)
        rate_resp.status_code = 429
        rate_resp.headers = {}
        rate_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "429", request=MagicMock(), response=rate_resp,
        )

        ok_resp = self._ok_response("After retry")

        with patch.object(client._client, "post", side_effect=[rate_resp, ok_resp]):
            with patch("applypilot.llm.time.sleep"):
                result = client.chat([{"role": "user", "content": "Hi"}])
        assert result == "After retry"

    def test_retries_on_timeout(self):
        client = self._client()

        ok_resp = self._ok_response("After timeout retry")

        with patch.object(
            client._client, "post",
            side_effect=[httpx.TimeoutException("timed out"), ok_resp],
        ):
            with patch("applypilot.llm.time.sleep"):
                result = client.chat([{"role": "user", "content": "Hi"}])
        assert result == "After timeout retry"

    def test_raises_after_max_retries(self):
        client = self._client()

        rate_resp = MagicMock(spec=httpx.Response)
        rate_resp.status_code = 429
        rate_resp.headers = {}
        rate_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "429", request=MagicMock(), response=rate_resp,
        )

        with patch.object(client._client, "post", return_value=rate_resp):
            with patch("applypilot.llm.time.sleep"):
                with pytest.raises(httpx.HTTPStatusError):
                    client.chat([{"role": "user", "content": "Hi"}])

    def test_qwen_gets_no_think_prefix(self):
        client = self._client(model="qwen3-8b")
        captured = []

        def capture(url, json=None, **kw):
            captured.append(json)
            return self._ok_response()

        with patch.object(client._client, "post", side_effect=capture):
            client.chat([{"role": "user", "content": "Tell me something"}])

        assert captured[0]["messages"][0]["content"].startswith("/no_think")

    def test_non_qwen_no_prefix(self):
        client = self._client(model="gpt-4o-mini")
        captured = []

        def capture(url, json=None, **kw):
            captured.append(json)
            return self._ok_response()

        with patch.object(client._client, "post", side_effect=capture):
            client.chat([{"role": "user", "content": "Tell me something"}])

        assert not captured[0]["messages"][0]["content"].startswith("/no_think")

    def test_close_closes_httpx(self):
        client = self._client()
        with patch.object(client._client, "close") as mock_close:
            client.close()
        mock_close.assert_called_once()


# ── Singleton management ─────────────────────────────────────────────────


class TestSingleton:
    def setup_method(self):
        reset_client()

    def teardown_method(self):
        reset_client()

    def test_get_client_returns_singleton(self):
        with patch("applypilot.llm._detect_provider", return_value=("http://t", "m", "k")):
            c1 = get_client()
            c2 = get_client()
        assert c1 is c2

    def test_reset_clears_singleton(self):
        with patch("applypilot.llm._detect_provider", return_value=("http://t", "m", "k")):
            c1 = get_client()
            reset_client()
            c2 = get_client()
        assert c1 is not c2


# ── Gemini compat fallback ───────────────────────────────────────────────


class TestGeminiCompatForbidden:
    def test_exception_stores_response(self):
        resp = MagicMock(spec=httpx.Response)
        resp.text = "Forbidden for this model"
        exc = _GeminiCompatForbidden(resp)
        assert exc.response is resp
        assert "403" in str(exc)
