package main

import (
	//"bytes"
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/robfig/cron"
	"io"
	"os"
	"os/exec"
	"syscall"
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
		var err error
		if last_started_time, err = time.Parse("2006-01-02 15:04:05", s.last_started.String); err != nil {
			panic(fmt.Sprintf("Couldn't parse time %s: %s", s.last_started.String, err.Error()))
		}
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

func (worker *CronWorker) ScheduleNextRun() (scheduled int) {
	// TODO, might still make sense to do a LOCK here, even with transaction.  Otherwise it's just gonna be extra
	//        work amounting to deadlock over and over again
	tx, err := worker.Db.Begin()
	if err != nil {
		panic(fmt.Sprintf("couldn't begin insert within ScheduleNextRun: %s", err))
	}
	sp := NewSchedulePass(tx, worker.CronParser)
	rows, err := tx.Query(sp.SchedulingQuery())
	defer rows.Close()
	if err != nil {
		tx.Rollback()
		panic(err)
	}
	for rows.Next() {
		sp.ProcessSchedulableRow(rows)
	}
	scheduled = len(sp.JobUpdateQueries)
	if len(sp.JobUpdateQueries) > 0 {
		stmt, err := tx.Prepare(sp.UpdateStatement)
		if err != nil {
			tx.Rollback()
			panic(fmt.Sprintf("Error updating job queries: %s", err))
		}
		defer stmt.Close()
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
	return
}

func (worker *CronWorker) AssignWork() {
	// can't update limit 1 on two tables. Instead, let's select for update the job we want to do
	// and then we'll update and commit the transaction.
	query := `
    UPDATE job_executions 
    SET worker_id = ? 
    WHERE scheduled_start <= NOW() 
      AND started IS NULL
      AND worker_id IS NULL
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
	defer stmt.Close()
	if _, err := stmt.Exec(worker.Id); err != nil {
		if mysqlerr, ok := err.(*mysql.MySQLError); ok {
			if mysqlerr.Number == 1213 {
				fmt.Fprintln(os.Stderr, "Deadlock assigning work, retrying")
				time.Sleep(10 * time.Microsecond) // add some swerve
				worker.AssignWork()               // try again on deadlock
				return
			}
		}
		panic(fmt.Sprintf("Couldn't schedule next job execution for worker %d: %T: %s", worker.Id, err, err))
	}
}

type Execution struct {
	execution_id int
	command      string
}

func (worker *CronWorker) recordExecStart(ex *Execution) {
	query := "UPDATE job_executions SET started = NOW() WHERE id = ?"
	stmt, err := worker.Db.Prepare(query)
	if err != nil {
		panic(fmt.Sprintf("Couldn't prepare query\n%s\nError: %s", query, err))
	}
	defer stmt.Close()
	if _, err := stmt.Exec(ex.execution_id); err != nil {
		panic(fmt.Sprintf("Couldn't record job execution start: %s", worker.Id, err))
	}
}

func (worker *CronWorker) recordExecEnd(ex *Execution, err error) {
	// todo: process state could be returned to collect runtime information
	var args []interface{}
	var query string
	if err == nil {
		query = "UPDATE job_executions SET ended = NOW(), exit_code = 0 WHERE id = ?"
		args = []interface{}{ex.execution_id}
	} else {
		query = "UPDATE job_executions SET ended = NOW(), error = ? WHERE id = ?"
		args = []interface{}{err.Error(), ex.execution_id}
		if execerr, ok := err.(*exec.ExitError); ok {
			if exit_status, ok := execerr.Sys().(syscall.WaitStatus); ok {
				query = "UPDATE job_executions SET ended = NOW(), exit_code = ?, error = ? WHERE id = ?"
				args = []interface{}{exit_status, execerr.Error(), ex.execution_id}
			}
		}
	}
	stmt, err := worker.Db.Prepare(query)
	if err != nil {
		panic(fmt.Sprintf("Couldn't prepare query\n%s\nError: %s", query, err))
	}
	defer stmt.Close()
	if _, err := stmt.Exec(args...); err != nil {
		panic(fmt.Sprintf("Couldn't record job execution end: %s", worker.Id, err))
	}
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
	defer stmt.Close()
	rows, err := stmt.Query(worker.Id)
	if err != nil {
		panic(fmt.Sprintf("Couldn't prepare query\n%s\nError: %s", query, err))
	}
	defer rows.Close()
	for rows.Next() {
		ex := new(Execution)
		if err := rows.Scan(&ex.execution_id, &ex.command); err != nil {
			panic(fmt.Sprintln("Failed to scan next job execution query result: ", err))
		}
    if false {
		  fmt.Fprintln(os.Stderr, "FindAssignedWork query returned job exec Id %d, %s", ex.execution_id, ex.command)
    }
		execs = append(execs, ex)
	}
	return execs
}

func (w *CronWorker) MustPrepareStatement(query string) (stmt *sql.Stmt) {
	stmt, err := w.Db.Prepare(query)
	if err != nil {
		panic(fmt.Sprintf("Couldn't prepare query\n%s\nError: %s", query, err))
	}
	return
}

func (w *CronWorker) TimeTillNextJob() (microseconds int) {
	microseconds = 3000000 // default
	query := `
    SELECT TIMESTAMPDIFF(MICROSECOND, NOW(6),scheduled_start) AS microseconds_to_wait 
      FROM job_executions 
     WHERE started IS NULL 
       AND worker_id IS NULL
     ORDER BY scheduled_start ASC LIMIT 1;
    `
	stmt := w.MustPrepareStatement(query)
	defer stmt.Close()
	rows, err := stmt.Query()
	defer rows.Close()
	if err != nil {
		panic(fmt.Sprintf("Error querying, query '%s', error %T %s\n", query, err, err.Error()))
	}
	if rows.Next() {
		if err := rows.Scan(&microseconds); err != nil {
			panic(fmt.Sprintln("Failed to scan query '%s' result row: %T %s", query, err, err.Error()))
    }
	}
  // if it's this close, don't sleep.  
  if microseconds < 0 {
    microseconds = 0
  }
	return
}

func (w *CronWorker) GetLogWriter(ex *Execution, source string) *LogWriter {
	return &LogWriter{
		exid:   ex.execution_id,
		db:     w.Db,
		source: source,
	}
}

type LogWriter struct {
	exid   int
	source string //'stdout' or 'stderr'
	db     *sql.DB
}

func (lw *LogWriter) Write(p []byte) (int, error) {
	go func(exid int, source string, data string) {
		query := `
      INSERT INTO job_execution_logs SET execution_id = ?, source = ?, output = ?
    `
		stmt, err := lw.db.Prepare(query)
		if err != nil {
			panic(fmt.Sprintf("Lost log: couldn't prepare query\n%s\nError: %s", query, err))
		}
		defer stmt.Close()
		res, err := stmt.Exec(exid, source, data)
		lid, err := res.LastInsertId()
		if err != nil {
			panic(fmt.Sprintf("Lost Log: Couldn't execute query\n%s\nError: %s", query, err))
		}
		if false { // todo: log levels
			fmt.Fprintf(os.Stderr, "Logged exid %d output source %s to log entry %d\n", lw.exid, lw.source, lid)
		}
	}(lw.exid, lw.source, string(p))
	//fmt.Fprintf(os.Stderr, "Spawned goroutine to log output source %s '%s'\n", lw.source, string(p))
	return len(p), nil
}

func main() {
	worker, err := NewCronWorker("root:@tcp(127.0.0.1:3306)/drcron")
	if err != nil {
		panic(*err)
	}
	worker.Join()
  var already_said bool = false // notify once for polling loop
	for {
		worker.AssignWork()
		execs := worker.FindAssignedWork()
		if len(execs) > 0 {
      already_said = false; // if we get into a polling loop for new work, we should notify once.
			for _, ex := range execs { //TODO: parallelize
				cmd := exec.Command("bash", "-c", ex.command)
				//var stdoutBuf, stderrBuf bytes.Buffer
				stdoutIn, _ := cmd.StdoutPipe()
				stderrIn, _ := cmd.StderrPipe()
				stdout := io.MultiWriter(os.Stdout /*&stdoutBuf,*/, worker.GetLogWriter(ex, "stdout"))
				stderr := io.MultiWriter(os.Stderr /*&stderrBuf,*/, worker.GetLogWriter(ex, "stderr"))
				var errStdout, errStderr error
				go func() {
					_, errStdout = io.Copy(stdout, stdoutIn)
				}()
				go func() {
					_, errStderr = io.Copy(stderr, stderrIn)
				}()
				starterr := cmd.Start()
				if starterr != nil {
					// TODO: better error handling
					panic(fmt.Sprintf("error starting command: %s", starterr.Error()))
				}
				fmt.Printf("Exec job command '%s'", ex.command)
				worker.recordExecStart(ex)
				// TODO: a goroutine could update the database for us for the pings, could send and clear stderr buffers too ?
				waiterr := cmd.Wait()
				worker.recordExecEnd(ex, waiterr)
				if waiterr != nil {
					panic(waiterr)
				}
				if errStdout != nil {
					fmt.Fprintf(os.Stderr, "Warning, failed to capture stdout - %T - %s\n", errStdout, errStdout.Error())
				}
				if errStderr != nil {
					fmt.Fprintf(os.Stderr, "Warning, failed to capture stderr - %T - %s\n", errStderr, errStderr.Error())
				}
				// TODO: ping while command runs and update
				fmt.Printf("Job finished.\n")
				//TODO: save results to database
			}
		} else if worker.ScheduleNextRun() > 0 {
			// we scheduled something
			continue
		} else {
      usec := worker.TimeTillNextJob()
      if usec > 3000000 {
        // 3 seconds ?  We bettr check again for new jobs eventually
			  if ! already_said {
          fmt.Fprintf(os.Stderr, "No work to do for at least 3 seconds; I'll poll every 3 sec for new jobs.\n")
          already_said = true
        }
			  time.Sleep(time.Duration(3) * time.Second )
      } else if usec > 2000 {
        already_said = false // if we miss our job, and get into a 3 second loop, we should say so agian.
			  fmt.Fprintf(os.Stderr, "No work to do at the moment and nothing to schedule; sleeping %d us for next job\n",usec)
			  time.Sleep(time.Duration(usec) * time.Microsecond )
      } // under 2ms? let's just try again , it'll take 2ms to have gotten this response and rerun the query.
			continue
		}
		if len(os.Getenv("ONEPASS")) > 0 {
			fmt.Fprintln(os.Stderr, "ONEPASS is configured, so I'll exit now.")
			break
		}
	}
}
