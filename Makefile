.PHONY: install build build-viz build-widget test watch clean release test-release rc-release

install:
	uv sync
	cd js && npm install

build: build-viz build-widget

build-viz:
	cd js/viz-core && npm run build

build-widget:
	cd js/widget && npm run build

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
	  echo "CHANGELOG.md has no '## [$(VERSION)]' section -- did you land the version bump on master?"; exit 1; \
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
	git add pyproject.toml
	git commit -m "rc-release: $(VERSION)"
	git tag v$(VERSION)
	git push origin v$(VERSION)
	git reset --hard HEAD~1
	@echo "✓ Pushed tag v$(VERSION) -- release.yml will build and publish to the REAL PyPI."
	@echo "  Your branch is unchanged; watch the run with: gh run watch"
