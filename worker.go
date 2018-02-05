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
	tx               *sql.Tx
	parser           *cron.Parser
	Schedulables     []*Schedulable
	JobUpdateQueries []*SchedErrUpdate
	InsertData       []*ExecInsertData
	InsertQuery      string
	UpdateStatement  string
}

func NewSchedulePass(tx *sql.Tx, parser *cron.Parser) *SchedulePass {
	sp := new(SchedulePass)
	sp.tx = tx
	sp.parser = parser
	sp.InsertQuery = "INSERT INTO job_executions ( job_id, scheduled_start ) VALUES "
	sp.UpdateStatement = "UPDATE jobs SET schedule_error = ?,  schedule_error_time=NOW() WHERE id = ?"
	return sp
}

func (s *Schedulable) Debug() {
	fmt.Fprintf(os.Stderr,
		"job_id:   \t%d\nschedule:\t%s\nrun_parallel:\t%d\nnum_queued:\t%d\nnum_running:\t%d\nlast_started:\t%s\n",
		s.job_id, s.schedule, s.run_parallel, s.num_queued, s.num_running, s.last_started.String,
	)
}

func (s *Schedulable) DBScan(rows *sql.Rows) (err error) {
	err = rows.Scan(&s.job_id, &s.run_parallel, &s.schedule, &s.num_queued, &s.num_running, &s.last_started, &s.updated_retry)
	s.Debug()
	return err
}

func (sp *SchedulePass) ProcessSchedulableRow(rows *sql.Rows) {
	var s Schedulable
	if err := s.DBScan(rows); err != nil {
		sp.tx.Rollback()
		panic(err)
	}
	// when will this next be happening?
	var last_started_time time.Time
	if s.last_started.Valid {
		panic("TODO: need to figure out how to parse this time " + s.last_started.String)
	} else {
		last_started_time = time.Unix(0, 0)
	}
	sched, err := sp.parser.Parse(s.schedule)
	if err != nil {
		msg := fmt.Sprintf("Failed to parse '%s': %s", s.schedule, err)
		fmt.Fprintln(os.Stderr, msg)
		// the WHERE clause above stops us from doing this too frequently ( only after update )
		sp.JobUpdateQueries = append(
			sp.JobUpdateQueries,
			&SchedErrUpdate{ErrMsg: msg, JobId: s.job_id},
		)
		return
	}
	next_time := sched.Next(last_started_time)
	fmt.Println("The job should run next on: ", next_time)
	sp.InsertData = append(
		sp.InsertData,
		&ExecInsertData{JobId: s.job_id, NextRun: next_time},
	)
}

func (*SchedulePass) SchedulingQuery() string {
	return `SELECT jobs.id AS job_id, jobs.run_parallel, schedule, 
      SUM(IF(job_executions.started IS NULL AND job_executions.id IS NOT NULL,1,0)) AS num_queued, 
      SUM(IF(job_executions.ended IS NULL AND job_executions.started IS NOT NULL,1,0)) AS num_running, 
      MAX(started) last_started,
      IF( jobs.schedule_error_time IS NULL, 0, 1) AS updated_retry
      FROM jobs LEFT JOIN job_executions ON ( jobs.id = job_executions.job_id ) 
      WHERE ( jobs.updated > jobs.schedule_error_time or jobs.schedule_error_time IS NULL)
      GROUP BY jobs.id 
      HAVING num_queued + num_running < jobs.run_parallel 
    FOR UPDATE`
}

func SQLTime(t *time.Time) string {
	return t.UTC().Format("2006-01-02 15:04:05.000000")
}

func (worker *CronWorker) ScheduleNextRun() {
	// TODO, might still make sense to do a LOCK here, even with transaction.  Otherwise it's just gonna be extra
	//        work amounting to deadlock over and over again
	tx, err := worker.Db.Begin()
	if err != nil {
		panic(err)
	}
	sp := NewSchedulePass(tx, worker.CronParser)
	if err != nil {
		panic(fmt.Sprintf("couldn't prepare insert for new jobs: %s", err))
	}
	rows, err := tx.Query(sp.SchedulingQuery())
	if err != nil {
		tx.Rollback()
		panic(err)
	}
	for rows.Next() {
		sp.ProcessSchedulableRow(rows)
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
			query += fmt.Sprintf("(%d,'%s')", data.JobId, SQLTime(&data.NextRun))
		}
		if _, err := tx.Exec(query); err != nil {
			tx.Rollback()
			panic(err)
		}
	}
	if err := tx.Commit(); err != nil {
		// TODO: catch deadlock here, as that almost certainly means a conflicting schedule pass.  This is all best effort.
		panic(fmt.Sprintf("Couldn't commit transaction: %s", err))
	}
}

func (worker *CronWorker) AssignWork() {
  // can't update limit 1 on two tables. Instead, let's select for update the job we want to do
  // and then we'll update and commit the transaction.
	query := `
    UPDATE job_executions 
    SET worker_id = ? 
    WHERE scheduled_start <= NOW() 
      AND started IS NULL
    LIMIT 1
  `
	/*query := `
    UPDATE job_executions 
    JOIN jobs ON ( job_executions.job_id = jobs.id )
    SET worker_id = ? 
    WHERE scheduled_start <= NOW() 
      AND started IS NULL
      AND jobs.enabled = TRUE 
    LIMIT 1
  `*/
	stmt, err := worker.Db.Prepare(query)
	if err != nil {
		panic(fmt.Sprintf("Couldn't prepare query\n%s\nError: %s", query, err))
	}
	if _, err := stmt.Exec(worker.Id); err != nil {
		panic(fmt.Sprintf("Couldn't schedule next job execution for worker %d: %s", worker.Id, err))
	}
}

type Execution struct {
	execution_id int
	command      string
}

func (worker *CronWorker) FindAssignedWork() (execs []*Execution) {
	execs = make([]*Execution, 0)
	query := `
    SELECT job_executions.id, command
    FROM job_executions 
    JOIN jobs ON ( jobs.id = job_executions.job_id )
    WHERE worker_id = ? 
      AND started IS NULL
    LIMIT 1
  `
	stmt, err := worker.Db.Prepare(query)
	if err != nil {
		panic(fmt.Sprintf("Couldn't prepare query\n%s\nError: %s", query, err))
	}
	rows, err := stmt.Query(worker.Id)
	if err != nil {
		panic(fmt.Sprintf("Couldn't prepare query\n%s\nError: %s", query, err))
	}
	for rows.Next() {
		ex := new(Execution)
		if err := rows.Scan(&ex.execution_id, &ex.command); err != nil {
			panic(fmt.Sprintln("Failed to scan next job execution query result: ", err))
		}
		fmt.Fprintln(os.Stderr, "FindAssignedWork query returned job exec Id %d, %s", ex.execution_id, ex.command)
		execs = append(execs, ex)
	}
	return execs
}

func main() {
	worker, err := NewCronWorker("root:@tcp(127.0.0.1:3306)/drcron")
	if err != nil {
		panic(*err)
	}
	worker.Join()
	for {
		execs := worker.FindAssignedWork()
		fmt.Println(execs)
		if len(execs) > 0 {
			for _, ex := range execs {
				fmt.Printf("should run job command '%s'", ex.command)
			}
		} else {
			worker.ScheduleNextRun()
			worker.AssignWork()
			continue
		}
		if len(os.Getenv("ONEPASS")) > 0 {
			break
		}
		time.Sleep(1 * time.Second)
	}
}
