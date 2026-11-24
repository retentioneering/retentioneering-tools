"""The skills under `.agents/skills/` and `.claude/skills/` are one package
published for two hosts, so their files must match byte for byte."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
AGENTS = ROOT / ".agents" / "skills"
CLAUDE = ROOT / ".claude" / "skills"


def _files(skills_dir: Path) -> dict[str, bytes]:
    # Only skill packages: `.agents/skills/README.md` is the index and has no copy.
    return {
        str(path.relative_to(skills_dir)): path.read_bytes()
        for skill in skills_dir.iterdir()
        if skill.is_dir()
        for path in skill.rglob("*")
        if path.is_file() and "__pycache__" not in path.parts
    }


def test_skill_copies_are_identical() -> None:
    agents, claude = _files(AGENTS), _files(CLAUDE)
    assert agents, f"no skills found under {AGENTS}"
    assert sorted(agents.keys() - claude.keys()) == [], "only in .agents/skills"
    assert sorted(claude.keys() - agents.keys()) == [], "only in .claude/skills"
    differ = sorted(name for name in agents if agents[name] != claude[name])
    assert differ == [], "differ between .agents/skills and .claude/skills"
