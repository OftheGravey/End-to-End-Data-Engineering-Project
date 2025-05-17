# Mock DB Client
This client will access the locally-run databases and make random inserts and updates against the them. Used for system-tests and creating mock data to use in processing.
> Code for Mock Client was developed by ChatGPT
## How to run
Uses UV for package management. Virtual environment setup:
```sh
uv venv
uv sync
.venv/Scripts/activate
```
To run the mock client simply:
```sh
python main.py
```

## Future work
Develop into docker image and run inside docker.