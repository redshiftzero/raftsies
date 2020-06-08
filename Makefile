test:
	pytest tests/ --ignore=.venv --doctest-modules --doctest-glob='raftsies/*.py' -v --cov-report term-missing --cov=raftsies

lint:
	black tests raftsies
	flake8 --exclude .venv --max-line-length 104 --ignore=E203,W503,E231
	mypy --strict --ignore-missing-imports --package raftsies

clean:  # Removes persisted log and vote record from most recent term.
	rm log*.txt
	rm follower_status*.json

check: lint test
