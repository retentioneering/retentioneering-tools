.PHONY: install install-dev build build-viz build-widget export-metric-schema test watch clean release test-release rc-release

install:
	uv sync
	cd js && npm install

# Full contributor setup: base install plus the one-time git hook that runs
# ruff/gitleaks/hygiene on every commit. Only needed if you'll be committing
# to this repo -- someone building the library from source to just use it
# doesn't need the hook, so it's kept out of plain `install`.
install-dev: install
	uv run pre-commit install

build: build-viz build-widget

build-viz:
	cd js/viz-core && npm run build

# Regenerate before building so js/widget/src/generated/metric_names.generated.ts
# can't go stale between a metric_schema.py change and the next build.
build-widget: export-metric-schema
	cd js/widget && npm run build

export-metric-schema:
	uv run python scripts/export_metric_schema.py

watch:
	cd js/widget && npm run dev

test:
	uv run python -m pytest tests/ -v

clean:
	rm -rf src/retentioneering/static/widget.js src/retentioneering/static/widget-static.js
	rm -rf js/viz-core/dist
	rm -rf js/node_modules js/viz-core/node_modules js/widget/node_modules
	rm -rf .venv
	# NOTE: widget.css is a checked-in source file (a CSS custom property
	# declaration), not build output -- the JS build never regenerates it,
	# so it's deliberately not removed here.

# master is protected (PRs only, no direct push) and merging to master does
# NOT trigger a release -- only pushing a `v*` tag does (see release.yml).
# Bump the version in pyproject.toml and turn CHANGELOG.md's [Unreleased]
# section into [VERSION] - date yourself, land that via a normal PR into
# master, then once master has that commit, run this to cut the release.
#
# Usage: make release VERSION=5.1.0
release:
	@if [ -z "$(VERSION)" ]; then \
	  echo "Usage: make release VERSION=x.y.z"; exit 1; \
	fi
	@branch=$$(git branch --show-current); \
	if [ "$$branch" != "master" ]; then \
	  echo "Refusing to release: you're on '$$branch', not master. Land the version bump on master first."; exit 1; \
	fi
	git fetch origin master
	@if [ "$$(git rev-parse HEAD)" != "$$(git rev-parse origin/master)" ]; then \
	  echo "Refusing to release: local master differs from origin/master. Run 'git pull' first."; exit 1; \
	fi
	@if ! grep -q "^## \[$(VERSION)\]" CHANGELOG.md; then \
	  echo "CHANGELOG.md has no '## [$(VERSION)]' section -- as part of the version-bump PR," \
	       "rename '## [Unreleased]' to '## [$(VERSION)] - $$(date +%Y-%m-%d)' (and add a fresh" \
	       "empty '## [Unreleased]' above it), then land that on master before releasing."; exit 1; \
	fi
	git tag -a v$(VERSION) -m "Release v$(VERSION)"
	git push origin v$(VERSION)
	@echo "✓ Pushed tag v$(VERSION) -- release.yml will build and publish to PyPI."

# Dry-runs the packaging + publish pipeline against test.pypi.org, from
# whatever branch you're currently on -- doesn't touch the real PyPI project
# or require being on master. Tags a throwaway version-bump commit and
# pushes just the tag (which is what test-release.yml listens for), then
# resets the bump off your branch so nothing about this sticks around.
# TestPyPI won't accept a repeated version string, so pick a new VERSION
# each time.
#
# Usage: make test-release VERSION=5.0.0rc1
test-release:
	@if [ -z "$(VERSION)" ]; then \
	  echo "Usage: make test-release VERSION=x.y.z"; exit 1; \
	fi
	@if [ -n "$$(git status --porcelain --untracked-files=no)" ]; then \
	  echo "Refusing: you have uncommitted changes (this uses git reset --hard). Commit or stash first."; exit 1; \
	fi
	sed -i '' 's/^version = ".*"/version = "$(VERSION)"/' pyproject.toml
	git add pyproject.toml
	git commit -m "test-release: $(VERSION)"
	git tag test-v$(VERSION)
	git push origin test-v$(VERSION)
	git reset --hard HEAD~1
	@echo "✓ Pushed tag test-v$(VERSION) -- test-release.yml will build and publish to TestPyPI."
	@echo "  Your branch is unchanged; watch the run with: gh run watch"

# Publishes a release candidate straight to the REAL PyPI project from
# whatever branch you're currently on -- e.g. to let real users
# `pip install --pre` an rc before v5-migration is merged to master. Same
# temp-commit/tag/push/reset dance as test-release, but pushes a real `v*`
# tag, which release.yml picks up and publishes for real (it already marks
# any tag containing "rc" as a GitHub prerelease).
#
# Restricted to versions with an "rc" suffix so this can't be used to slip
# a final version out past the master-only gate in `release` above -- cut
# those the normal way once v5-migration lands on master.
#
# Unlike test-release, PyPI publishes here are real and permanent: the
# version string is reserved forever and the package is publicly installable
# immediately.
#
# Also renames CHANGELOG.md's "## [<base version>]" section header (e.g.
# "## [5.0.0]") to "## [<VERSION>]" (e.g. "## [5.0.0rc1]") for this temp
# commit, so release.yml's notes extraction finds a matching section. This
# is a plain rename, not a merge -- any separate "## [Unreleased]" content
# is left alone; fold it in by hand first if it should ship in these notes.
#
# Usage: make rc-release VERSION=5.0.0rc1
rc-release:
	@if [ -z "$(VERSION)" ]; then \
	  echo "Usage: make rc-release VERSION=x.y.zrcN"; exit 1; \
	fi
	@if ! echo "$(VERSION)" | grep -qE 'rc[0-9]+$$'; then \
	  echo "Refusing: '$(VERSION)' doesn't look like a release candidate (need an rcN suffix, e.g. 5.0.0rc1). Use 'make release' for final versions."; exit 1; \
	fi
	@if [ -n "$$(git status --porcelain --untracked-files=no)" ]; then \
	  echo "Refusing: you have uncommitted changes (this uses git reset --hard). Commit or stash first."; exit 1; \
	fi
	sed -i '' 's/^version = ".*"/version = "$(VERSION)"/' pyproject.toml
	@base="$$(echo $(VERSION) | sed -E 's/rc[0-9]+$$//')"; \
	sed -i '' "s/^## \[$${base}\]\$$/## [$(VERSION)]/" CHANGELOG.md
	git add pyproject.toml CHANGELOG.md
	git commit -m "rc-release: $(VERSION)"
	git tag v$(VERSION)
	git push origin v$(VERSION)
	git reset --hard HEAD~1
	@echo "✓ Pushed tag v$(VERSION) -- release.yml will build and publish to the REAL PyPI."
	@echo "  Your branch is unchanged; watch the run with: gh run watch"
