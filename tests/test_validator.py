"""Unit tests for applypilot.scoring.validator."""

import pytest

from applypilot.scoring.validator import (
    sanitize_text,
    validate_json_fields,
    validate_tailored_resume,
    validate_cover_letter,
    BANNED_WORDS,
    LLM_LEAK_PHRASES,
    FABRICATION_WATCHLIST,
)


# ── sanitize_text ─────────────────────────────────────────────────────────


class TestSanitizeText:
    def test_replaces_em_dash(self):
        assert "\u2014" not in sanitize_text("Python \u2014 5 years")

    def test_replaces_en_dash(self):
        result = sanitize_text("2020\u20132023")
        assert "\u2013" not in result
        assert "-" in result

    def test_replaces_smart_double_quotes(self):
        assert sanitize_text("\u201cHello\u201d") == '"Hello"'

    def test_replaces_smart_single_quotes(self):
        result = sanitize_text("\u2018It\u2019s\u2019")
        assert "\u2018" not in result
        assert "\u2019" not in result

    def test_strips_whitespace(self):
        assert sanitize_text("  text  ") == "text"

    def test_plain_text_unchanged(self):
        original = "Clean text with no special chars"
        assert sanitize_text(original) == original


# ── validate_json_fields ──────────────────────────────────────────────────


def _valid_json_data():
    return {
        "title": "Software Engineer",
        "summary": "Experienced engineer with Python and TypeScript skills.",
        "skills": {"languages": ["Python", "TypeScript"], "frameworks": ["FastAPI"]},
        "experience": [
            {
                "header": "Acme Corp | 2021-2024",
                "bullets": ["Built REST APIs using FastAPI", "Deployed with Docker"],
            }
        ],
        "projects": [{"header": "OpenSourceLib", "bullets": ["Contributed 50+ PRs"]}],
        "education": "B.S. Computer Science, State University, 2021",
    }


def _profile():
    return {
        "resume_facts": {
            "preserved_companies": ["Acme Corp"],
            "preserved_school": "State University",
        },
    }


class TestValidateJsonFields:
    def test_valid_data_passes(self):
        result = validate_json_fields(_valid_json_data(), _profile())
        assert result["passed"] is True
        assert result["errors"] == []

    def test_missing_required_field(self):
        data = _valid_json_data()
        del data["summary"]
        result = validate_json_fields(data, _profile())
        assert result["passed"] is False
        assert any("summary" in e for e in result["errors"])

    def test_empty_required_field(self):
        data = _valid_json_data()
        data["summary"] = ""
        result = validate_json_fields(data, _profile())
        assert result["passed"] is False

    def test_missing_company_fails(self):
        data = _valid_json_data()
        data["experience"] = [{"header": "Other Corp | 2021", "bullets": []}]
        result = validate_json_fields(data, _profile())
        assert result["passed"] is False
        assert any("Acme Corp" in e for e in result["errors"])

    def test_fabricated_skill_fails(self):
        data = _valid_json_data()
        data["skills"] = {"languages": ["Python", "golang"]}
        result = validate_json_fields(data, _profile())
        assert result["passed"] is False
        assert any("golang" in e.lower() for e in result["errors"])

    def test_llm_self_talk_fails(self):
        data = _valid_json_data()
        data["summary"] = "I apologize for the previous error."
        result = validate_json_fields(data, _profile())
        assert result["passed"] is False

    def test_banned_words_strict_fails(self):
        data = _valid_json_data()
        data["summary"] = "Passionate engineer who spearheaded initiatives."
        result = validate_json_fields(data, _profile(), mode="strict")
        assert result["passed"] is False
        assert any("Banned" in e for e in result["errors"])

    def test_banned_words_normal_warns(self):
        data = _valid_json_data()
        data["summary"] = "Passionate engineer with strong track record."
        result = validate_json_fields(data, _profile(), mode="normal")
        assert result["passed"] is True
        assert any("Banned" in w for w in result["warnings"])

    def test_banned_words_lenient_ignored(self):
        data = _valid_json_data()
        data["summary"] = "Passionate spearheaded synergy expert."
        result = validate_json_fields(data, _profile(), mode="lenient")
        assert result["passed"] is True
        assert result["warnings"] == []

    def test_missing_school_fails(self):
        data = _valid_json_data()
        data["education"] = "B.S. CS, Other University, 2021"
        result = validate_json_fields(data, _profile())
        assert result["passed"] is False

    def test_no_preserved_school_skips_check(self):
        data = _valid_json_data()
        data["education"] = "B.S. CS, Anywhere, 2021"
        profile = {"resume_facts": {"preserved_companies": ["Acme Corp"]}}
        result = validate_json_fields(data, profile)
        assert result["passed"] is True


# ── validate_tailored_resume ──────────────────────────────────────────────


_VALID_RESUME = """Jane Developer | jane@example.com | 555-123-4567

SUMMARY
Experienced software engineer with Python and TypeScript expertise.

TECHNICAL SKILLS
Languages: Python, TypeScript, JavaScript
Frameworks: React, FastAPI
Databases: PostgreSQL, Redis
Tools: Docker, Git, GitHub Actions

EXPERIENCE
Acme Corp | Software Engineer | 2021-2024
- Built REST APIs using FastAPI
- Deployed microservices with Docker

Startup Inc | Backend Engineer | 2019-2021
- Developed Python automation scripts

PROJECTS
OpenSourceLib
- Contributed 50+ pull requests

EDUCATION
B.S. Computer Science, State University, 2021"""


_RESUME_PROFILE = {
    "personal": {
        "full_name": "Jane Developer",
        "email": "jane@example.com",
        "phone": "555-123-4567",
    },
    "resume_facts": {
        "preserved_companies": ["Acme Corp", "Startup Inc"],
        "preserved_projects": ["OpenSourceLib"],
        "preserved_school": "State University",
    },
}


class TestValidateTailoredResume:
    def test_valid_resume_passes(self):
        result = validate_tailored_resume(_VALID_RESUME, _RESUME_PROFILE)
        assert result["passed"] is True
        assert result["errors"] == []

    def test_missing_summary_section(self):
        resume = _VALID_RESUME.replace("SUMMARY", "INTRO")
        result = validate_tailored_resume(resume, _RESUME_PROFILE)
        assert result["passed"] is False
        assert any("SUMMARY" in e for e in result["errors"])

    def test_missing_experience_section(self):
        # Must remove ALL occurrences of "experience" (including "Experienced")
        # since the validator uses substring matching on lowercased text.
        resume = _VALID_RESUME.replace("EXPERIENCE", "CAREER HISTORY")
        resume = resume.replace("Experienced", "Skilled")
        resume = resume.replace("experience", "background")
        result = validate_tailored_resume(resume, _RESUME_PROFILE)
        assert result["passed"] is False

    def test_missing_company_is_error(self):
        resume = _VALID_RESUME.replace("Acme Corp", "Some Other Corp")
        result = validate_tailored_resume(resume, _RESUME_PROFILE)
        assert result["passed"] is False
        assert any("Acme Corp" in e for e in result["errors"])

    def test_missing_name_is_warning(self):
        resume = _VALID_RESUME.replace("Jane Developer", "J. Dev")
        result = validate_tailored_resume(resume, _RESUME_PROFILE)
        assert any("Name" in w for w in result["warnings"])

    def test_fabricated_skill_in_skills_section(self):
        resume = _VALID_RESUME.replace(
            "Languages: Python, TypeScript, JavaScript",
            "Languages: Python, TypeScript, C#, Golang",
        )
        result = validate_tailored_resume(resume, _RESUME_PROFILE)
        assert result["passed"] is False
        assert any("FABRICATED" in e for e in result["errors"])

    def test_em_dash_fails(self):
        resume = _VALID_RESUME + "\n\u2014 extra"
        result = validate_tailored_resume(resume, _RESUME_PROFILE)
        assert result["passed"] is False

    def test_en_dash_fails(self):
        resume = _VALID_RESUME + "\n2020\u20132024"
        result = validate_tailored_resume(resume, _RESUME_PROFILE)
        assert result["passed"] is False

    def test_llm_self_talk_fails(self):
        resume = _VALID_RESUME + "\nNote: I have removed all banned words."
        result = validate_tailored_resume(resume, _RESUME_PROFILE)
        assert result["passed"] is False

    def test_missing_education_is_error(self):
        resume = _VALID_RESUME.replace("State University", "Other University")
        result = validate_tailored_resume(resume, _RESUME_PROFILE)
        assert result["passed"] is False

    def test_banned_words_are_errors(self):
        resume = _VALID_RESUME.replace(
            "Experienced software engineer",
            "Passionate and dedicated engineer",
        )
        result = validate_tailored_resume(resume, _RESUME_PROFILE)
        assert result["passed"] is False
        assert any("Banned" in e for e in result["errors"])


# ── validate_cover_letter ─────────────────────────────────────────────────


_VALID_LETTER = (
    "Dear Hiring Manager,\n\n"
    "I am writing to apply for the Software Engineer position at your company. "
    "My experience with Python and FastAPI makes me a strong candidate for this role. "
    "I have shipped production APIs serving thousands of users daily."
)


class TestValidateCoverLetter:
    def test_valid_letter_passes(self):
        result = validate_cover_letter(_VALID_LETTER)
        assert result["passed"] is True

    def test_must_start_with_dear(self):
        result = validate_cover_letter("Hello,\n\nI am writing to apply...")
        assert result["passed"] is False
        assert any("Dear" in e for e in result["errors"])

    def test_em_dash_fails(self):
        result = validate_cover_letter(_VALID_LETTER + " \u2014 extra.")
        assert result["passed"] is False

    def test_llm_self_talk_fails(self):
        result = validate_cover_letter(_VALID_LETTER + "\nNote: I have revised this.")
        assert result["passed"] is False

    def test_too_long_strict(self):
        long = "Dear Hiring Manager,\n\n" + ("word " * 260)
        result = validate_cover_letter(long, mode="strict")
        assert result["passed"] is False
        assert any("Too long" in e for e in result["errors"])

    def test_slightly_long_normal_warns(self):
        # Normal mode warns at >275 words, so use 280 filler words
        long = "Dear Hiring Manager,\n\n" + ("word " * 280)
        result = validate_cover_letter(long, mode="normal")
        assert result["passed"] is True
        assert any("Long" in w for w in result["warnings"])

    def test_no_word_count_in_lenient(self):
        very_long = "Dear Hiring Manager,\n\n" + ("word " * 500)
        result = validate_cover_letter(very_long, mode="lenient")
        assert not any("long" in e.lower() for e in result["errors"])

    def test_banned_words_strict_are_errors(self):
        letter = "Dear Hiring Manager,\n\nI am passionate and spearheaded many projects."
        result = validate_cover_letter(letter, mode="strict")
        assert result["passed"] is False

    def test_banned_words_normal_are_warnings(self):
        letter = "Dear Hiring Manager,\n\nI am passionate and spearheaded many projects."
        result = validate_cover_letter(letter, mode="normal")
        assert result["passed"] is True
        assert result["warnings"]


# ── Constants sanity ──────────────────────────────────────────────────────


class TestConstants:
    def test_banned_words_not_empty(self):
        assert len(BANNED_WORDS) > 10

    def test_llm_leak_phrases_not_empty(self):
        assert len(LLM_LEAK_PHRASES) > 10

    def test_fabrication_watchlist_not_empty(self):
        assert len(FABRICATION_WATCHLIST) > 5

    def test_banned_words_are_lowercase(self):
        for word in BANNED_WORDS:
            assert word == word.lower(), f"Banned word not lowercase: {word!r}"

    def test_llm_leak_phrases_are_lowercase(self):
        for phrase in LLM_LEAK_PHRASES:
            assert phrase == phrase.lower(), f"Leak phrase not lowercase: {phrase!r}"
