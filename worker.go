package main

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"github.com/robfig/cron"
	"os"
	"time"
)

type CronWorker struct {
	Db         *sql.DB
	Id         int64
	CronParser *cron.Parser
}

func NewCronWorker(conn string) (worker *CronWorker, err *error) {
	worker = nil
	db, e := sql.Open("mysql", conn)
	if e != nil {
		fmt.Fprintln(os.Stderr, err)
		err = &e
		return
	}
	parser := cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	worker = new(CronWorker)
	worker.Db = db
	worker.CronParser = &parser
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

// have to work around the lack of proper cursor multiplexing in the mysql driver, d'oh!

type Schedulable struct {
	job_id, run_parallel, num_queued, num_running, updated_retry int
	last_started                                                 sql.NullString
	schedule                                                     string
}

type ExecInsertData struct {
	JobId   int
	NextRun time.Time
}

type SchedErrUpdate struct {
	ErrMsg string
	JobId  int
}

type SchedulePass struct {
	Schedulables     []*Schedulable
	JobUpdateQueries []*SchedErrUpdate
	InsertData       []*ExecInsertData
	InsertQuery      string
	UpdateStatement  string
}

func NewSchedulePass() *SchedulePass {
	sp := new(SchedulePass)
	sp.InsertQuery = "INSERT INTO job_executions ( job_id, scheduled_start ) VALUES "
	sp.UpdateStatement = "UPDATE jobs SET schedule_error = ?,  schedule_error_time=NOW() WHERE id = ?"
	return sp
}

func (worker *CronWorker) ScheduleNextRun() {
	sp := NewSchedulePass()
	tx, err := worker.Db.Begin()
	if err != nil {
		panic(err)
	}
	if err != nil {
		panic(fmt.Sprintf("couldn't prepare insert for new jobs: %s", err))
	}
	rows, err := tx.Query(`
    SELECT jobs.id AS job_id, jobs.run_parallel, schedule, 
      SUM(IF(job_executions.started IS NULL AND job_executions.id IS NOT NULL,1,0)) AS num_queued, 
      SUM(IF(job_executions.ended IS NULL AND job_executions.started IS NOT NULL,1,0)) AS num_running, 
      MAX(started) last_started,
      IF( jobs.schedule_error_time IS NULL, 0, 1) AS updated_retry
      FROM jobs LEFT JOIN job_executions ON ( jobs.id = job_executions.job_id ) 
      WHERE ( jobs.updated > jobs.schedule_error_time or jobs.schedule_error_time IS NULL)
      GROUP BY jobs.id 
      HAVING num_queued + num_running < jobs.run_parallel 
    FOR UPDATE
  `)
	if err != nil {
		tx.Rollback()
		panic(err)
	}
	for rows.Next() {
		s := new(Schedulable)
		if err = rows.Scan(&s.job_id, &s.run_parallel, &s.schedule, &s.num_queued, &s.num_running, &s.last_started, &s.updated_retry); err != nil {
			tx.Rollback()
			panic(err)
		}
		fmt.Fprintf(os.Stderr,
			"job_id:   \t%d\nschedule:\t%s\nrun_parallel:\t%d\nnum_queued:\t%d\nnum_running:\t%d\nlast_started:\t%s\n",
			s.job_id, s.schedule, s.run_parallel, s.num_queued, s.num_running, s.last_started.String,
		)
		// when will this next be happening?
		var last_started_time time.Time
		if s.last_started.Valid {
			panic("TODO: need to figure out how to parse this time " + s.last_started.String)
		} else {
			last_started_time = time.Unix(0, 0)
		}
		sched, err := worker.CronParser.Parse(s.schedule)
		if err != nil {
			msg := fmt.Sprintf("Failed to parse '%s': %s", s.schedule, err)
			fmt.Fprintln(os.Stderr, msg)
			// the WHERE clause above stops us from doing this too frequently ( only after update )
			sp.JobUpdateQueries = append(
				sp.JobUpdateQueries,
				&SchedErrUpdate{ErrMsg: msg, JobId: s.job_id},
			)
			continue
		}
		next_time := sched.Next(last_started_time)
		//if time.Now().After(next_time) {
		//	next_time = time.Now()
		//}
		fmt.Println("The job should run next on: ", next_time)
		sp.InsertData = append(
			sp.InsertData,
			&ExecInsertData{JobId: s.job_id, NextRun: next_time},
		)
	}
	if len(sp.JobUpdateQueries) > 0 {
		stmt, err := tx.Prepare(sp.UpdateStatement)
		if err != nil {
			tx.Rollback()
			panic(fmt.Sprintf("Error updating job queries: %s", err))
		}
		for _, data := range sp.JobUpdateQueries {
			_, err := stmt.Exec(data.ErrMsg, data.JobId)
			if err != nil {
				tx.Rollback()
				panic(err)
			}
		}
	}
	if len(sp.InsertData) > 0 {
		query := sp.InsertQuery
		for i, data := range sp.InsertData {
			if i > 0 {
				query += ", "
			}
			query += fmt.Sprintf("(%d,'%s')", data.JobId, data.NextRun.UTC().Format("2006-01-02 15:04:05.000000"))
		}
		if _, err := tx.Exec(query); err != nil {
			tx.Rollback()
			panic(err)
		}
	}
	if err := tx.Commit(); err != nil {
		panic(fmt.Sprintf("Couldn't commit transacrtion: %s", err))
	}
}

func main() {
	worker, err := NewCronWorker("root:@tcp(127.0.0.1:3306)/drcron")
	if err != nil {
		panic(*err)
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
