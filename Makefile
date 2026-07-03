.PHONY: install build build-viz build-widget test watch clean release

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
