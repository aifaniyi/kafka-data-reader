stop:
	docker compose -p kafka_data_reader down -v

init-environment: stop
	docker compose -p kafka_data_reader up -d kafka
	sudo bash hosts.sh

generate:
	go generate ./...