server:
	@cd ./pedis && python server.py

client:
	@cd ./pedis && python client.py

clean:
	@find . -name '*.pyc' -exec rm -f {} +
	@find . -name '*.pyo' -exec rm -f {} +
	@find . -name '__pycache__' -exec rm -fr {} +
