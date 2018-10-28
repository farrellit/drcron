test: fmt worker

fmt:
	go fmt worker.go

full: wipe test

wipe:
	docker inspect --type container mysql > /dev/null && docker kill mysql || true

db:
	docker inspect --type container mysql >/dev/null 2>&1 || docker run -e MYSQL_ALLOW_EMPTY_PASSWORD=true --name mysql --rm -d -p 3306:3306 mysql

schema: db
	docker cp tables.sql mysql:/tmp && docker exec -it mysql bash -c 'until mysql -h 127.0.0.1 mysql -e "select 1" > /dev/null; do sleep 3; done; mysql < /tmp/tables.sql'
	# docker exec -it mysql mysqldump --no-data --databases drcron
	
worker:
	ONEPASS=$${ONEPASS:-} go run worker.go

select_workers:
	docker exec -it mysql mysql drcron -e 'SELECT * FROM workers'

