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
	$(PYTHON) -m pip install --upgrade dist/*.whl

reinstall:
	$(PYTHON) -m pip uninstall -y $$(basename $$(grep -Po '(?<=name = ").*(?=")' pyproject.toml))
	$(PYTHON) -m pip install --upgrade dist/*.whl

clean:
	rm -rf build/ dist/ *.egg-info

distclean: clean
	find . -type d -name "__pycache__" -exec rm -rf {} +

upload: build
	$(PYTHON) -m twine upload dist/*
