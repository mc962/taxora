# Demo shortcuts. See README "Running it" for what each piece is.
COMPOSE = docker compose -f deployment/docker/docker-compose.yml --env-file $(or $(ENV_FILE),.dev.env)
export ENV_FILE ?= .dev.env

.PHONY: demo seed demo-seeded run reset down clean
demo:   ## Set up everything: env file, full stack, API key
	./scripts/demo-up.sh
seed:   ## Load the small countries (IS VA SM) so the demo starts with existing data
	./scripts/demo-seed.sh
demo-seeded: demo seed  ## demo + seed in one go
run:    ## Start ingests and watch them live (COUNTRIES="DE US" to change)
	./scripts/demo-run.sh
reset:  ## Empty the occurrences table (keeps users, keys, Temporal history)
	ENV_FILE=../../$(ENV_FILE) $(COMPOSE) exec -T -e TAXORA_ALLOW_DATA_RESET=true api python -m taxora.cli reset-data --yes
down:   ## Stop the stack, keep data
	ENV_FILE=../../$(ENV_FILE) $(COMPOSE) down
clean:  ## Stop the stack and delete all data and the saved API key
	ENV_FILE=../../$(ENV_FILE) $(COMPOSE) down -v && rm -f .demo-key
