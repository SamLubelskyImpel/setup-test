DOCKER_COMPOSE_FILE=docker-compose.yaml

build:
	${INFO} "Creating builder image..."
	docker compose build
	${INFO} "Build complete"

run:
	${INFO} "Running docker container..."
	docker compose up

stop:
	${INFO} "Stop development server containers"
	@ docker compose -f $(DOCKER_COMPOSE_FILE) down
	${INFO} "All containers stopped successfully"

ping:
	${INFO} "Checking health status of the app..."
	curl localhost:8090/health_check


clean-pycache:
	sudo find . -type f -name "*.py[co]" -delete
	sudo find . -type d -name "__pycache__" -exec rm -rv {} +
	sudo find . -type d -name ".pytest_cache" -exec rm -rv {} +


venv:
	${INFO} "Creating Python Virtual Environment"
	@ python3 -m venv .venv
	${SUCCESS} "Virtual Environment has be created successfully, run ' source .venv/bin/activate' to activate it"
	${INFO} "Run 'make start' command, when its done, visit http://localhost:8090 to access the app"
	${INFO} "If you encounter any issues, contact your team lead or platform-infra-devops team"
	@ echo " "


 # COLORS
GREEN  := $(shell tput -Txterm setaf 2)
YELLOW := $(shell tput -Txterm setaf 3)
WHITE  := $(shell tput -Txterm setaf 7)
NC := "\e[0m"
RESET  := $(shell tput -Txterm sgr0)

# Shell Functions
INFO := @bash -c 'printf $(YELLOW); echo "===> $$1"; printf $(NC)' SOME_VALUE
SUCCESS := @bash -c 'printf $(GREEN); echo "===> $$1"; printf $(NC)' SOME_VALUE
