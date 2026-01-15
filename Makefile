# Change this if your Python executable is different
PYTHON ?= python


.PHONY: build install reinstall clean distclean upload help

help:
	@echo "Common commands:"
	@echo "  make build       - Build wheel and sdist"
	@echo "  make install     - Install the built package"
	@echo "  make reinstall   - Uninstall and reinstall locally"
	@echo "  make clean       - Remove build artifacts"
	@echo "  make distclean   - Deep clean (including caches)"
	@echo "  make upload      - Upload to PyPI (requires twine)"

build:
	$(PYTHON) -m build

install:
	$(PYTHON) -m pip install --upgrade --find-links dist msfutilspkg


reinstall:
	python -m pip install --force-reinstall --find-links dist msfutilspkg

clean:
	rm -rf build/ dist/ *.egg-info

distclean: clean
	find . -type d -name "__pycache__" -exec rm -rf {} +

upload: build
	$(PYTHON) -m twine upload dist/*
