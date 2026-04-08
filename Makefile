PYTHON := .venv/bin/python
PIP := .venv/bin/pip
UVICORN := .venv/bin/uvicorn

.PHONY: install web worker

install:
	python3 -m venv .venv
	$(PIP) install -r requirements.txt

web:
	$(UVICORN) app.main:app --host 0.0.0.0 --port 8000 --reload

worker:
	$(PYTHON) -m app.worker
