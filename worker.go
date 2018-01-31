package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"time"
)

type CronWorker struct {
	Db *sql.DB
	Id int64
}

func NewCronWorker(conn string) (worker *CronWorker, err *error) {
	worker = nil
	db, e := sql.Open("mysql", conn)
	if e != nil {
		fmt.Fprintln(os.Stderr, err)
		err = &e
		return
	}
	worker = new(CronWorker)
	worker.Db = db
	fmt.Fprintln(os.Stderr, "Connected")
	err = nil
	return
}

func (worker *CronWorker) Info() string {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	return hostname
}

func (worker *CronWorker) Join() {
	stmt, err := worker.Db.Prepare("INSERT INTO workers (info) VALUES (?)")
	defer stmt.Close()
	res, err := stmt.Exec(worker.Info())
	if err != nil {
		panic(err)
	}
	wid, err := res.LastInsertId()
	if err != nil {
		panic(err)
	}
	worker.Id = wid
	fmt.Fprintln(os.Stderr, "My worker ID is ", worker.Id)
}

func (worker *CronWorker) FindWork() {
}

func (worker *CronWorker) getQueueLock() bool {
	rows, err := worker.Db.Query("SELECT GET_LOCK('queue_jobs', 0)")
	if err != nil {
		panic(err)
	}
	var locked int
	rows.Next()
	if err := rows.Scan(&locked); err != nil {
		panic(err)
	}
	if locked == 1 {
		fmt.Fprintf(os.Stderr, "Worker %d got queue_jobs lock and will now queue next runs\n", worker.Id)
		return true
	} else {
		fmt.Fprintf(os.Stderr, "Worker %d failed to get queue_jobs lock; another process is queueing\n", worker.Id)
		return false
	}
}

func (worker *CronWorker) releaseQueueLock() {
	_, err := worker.Db.Exec("SELECT RELEASE_LOCK('queue_jobs')")
	if err != nil {
		panic(err)
	} else {
    fmt.Fprintln(os.Stderr,"lock released")
  }
}

func (worker *CronWorker) ScheduleNextRun() {
	if worker.getQueueLock() == false {
		return
	}
	defer worker.releaseQueueLock()
	rows, err := worker.Db.Query(`
    select jobs.id as job_id, jobs.run_parallel, schedule, 
      SUM(if(job_executions.started is null and job_executions.id is not null,1,0)) as num_queued, 
      SUM(if(job_executions.ended is null and job_executions.started is not null,1,0)) as num_running, 
      MAX(started) last_started
      FROM jobs left join job_executions on ( jobs.id = job_executions.job_id ) 
      GROUP BY jobs.id 
      HAVING num_queued + num_running < jobs.run_parallel ;
  `)
	if err != nil {
		panic(err)
	}
	for rows.Next() {
		var job_id, run_parallel, num_queued, num_running int
		var last_started, schedule sql.NullString
		if err = rows.Scan(&job_id, &run_parallel, &schedule, &num_queued, &num_running, &last_started); err != nil {
			panic(err)
		}
		fmt.Fprintf(os.Stderr, "job_id:   \t%d\nschedule:\t%s\nrun_parallel:\t%d\nnum_queued:\t%d\nnum_running:\t%d\nlast_started:\t%s\n", job_id, schedule.String, run_parallel, num_queued, num_running, last_started.String)
	}
}

func main() {
	worker, errp := NewCronWorker("root:@tcp(127.0.0.1:3306)/drcron")
	if errp != nil {
		panic(*errp)
	}
	worker.Join()
	for {
		worker.ScheduleNextRun()
		if len(os.Getenv("ONEPASS")) > 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}
}
