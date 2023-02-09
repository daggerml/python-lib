.PHONY: pypi
pypi:
	python3 -m build && twine upload dist/*
