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
	rm -rf src/retentioneering/static/widget.js src/retentioneering/static/widget.css
	rm -rf js/viz-core/dist
	rm -rf js/node_modules js/viz-core/node_modules js/widget/node_modules
	rm -rf .venv

# Usage: make release VERSION=0.5.0
#        make release VERSION=0.5.0 DESC="some feature, bug fix"
# Requires: CHANGELOG.md [Unreleased] section is already filled in.
# Stages all tracked modified files (git add -u), so run with a clean tree.
release:
	@if [ -z "$(VERSION)" ]; then \
	  echo "Usage: make release VERSION=x.y.z [DESC='description']"; exit 1; \
	fi
	@echo "→ Bumping version to $(VERSION) in pyproject.toml"
	@sed -i '' 's/^version = ".*"/version = "$(VERSION)"/' pyproject.toml
	@echo "→ Updating CHANGELOG.md ([Unreleased] → [$(VERSION)])"
	@TODAY=$$(date +%Y-%m-%d); \
	sed -i '' "s/^## \[Unreleased\]$$/## [Unreleased]\n\n## [$(VERSION)] - $$TODAY/" CHANGELOG.md
	@git add -u
	@if [ -n "$(DESC)" ]; then \
	  git commit -m "release: $(VERSION) — $(DESC)"; \
	else \
	  git commit -m "release: $(VERSION)"; \
	fi
	@git tag -a v$(VERSION) -m "Release v$(VERSION)"
	@echo ""
	@echo "✓ Committed and tagged v$(VERSION). Now run:"
	@echo "    git push && git push origin v$(VERSION)"
