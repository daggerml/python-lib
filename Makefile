.PHONY: pypi
pypi:
	python -m build && twine upload dist/*
