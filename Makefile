test: fmt worker

fmt:
	go fmt worker.go

db:
	docker inspect --type container mysql >/dev/null 2>&1 || docker run -e MYSQL_ALLOW_EMPTY_PASSWORD=true --name mysql --rm -d -p 3306:3306 mysql

schema: db
	docker cp tables.sql mysql:/tmp && docker exec -it mysql bash -c 'until mysqladmin ping; do sleep 1; done; mysql < /tmp/tables.sql'
	# docker exec -it mysql mysqldump --no-data --databases drcron
	
worker: schema
	ONEPASS=true go run worker.go

select_workers:
	docker exec -it mysql mysql drcron -e 'SELECT * FROM workers'