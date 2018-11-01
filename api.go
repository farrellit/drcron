
package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"net/http"
	//"time"
)

func jsonNull() []byte {
  return []byte("null")
}

type NullString struct {
  sql.NullString
}

type NullTime struct {
  mysql.NullTime
}

type NullInt64 struct {
  sql.NullInt64
}

func (nt *NullTime)MarshalJSON() ([]byte, error){
  if nt.Valid {
    return json.Marshal(fmt.Sprint(nt.Time))
  }
  return jsonNull(), nil
}

func (ns *NullString)MarshalJSON() ([]byte, error){
  if ns.Valid {
    return json.Marshal(ns.String)
  }
  return jsonNull(), nil
}

func (ni *NullInt64)MarshalJSON() ([]byte, error){
  if ni.Valid {
    return json.Marshal(ni.Int64)
  }
  return jsonNull(), nil
}

type WorkerRow struct {
  Id int64
  Info NullString
  Added NullTime
  Heartbeat NullTime
  Exited NullTime
}

type JobRow struct {
  Id int64
  Created NullTime
  Updated NullTime
  Name NullString
  Schedule NullString
  Command NullString
  Enabled bool
  Run_parallel uint
  Schedule_error NullString
  Schedule_error_time NullTime
}

type JobExecutionRow struct {
  Id int64
  JobId int64
  WorkerId int64
  Queued NullTime
  Scheduled_start NullTime
  Started NullTime
  Pid NullInt64
  Ended NullTime
  Exit_code NullInt64
}

type RouteHandler interface {
  HandleFunc(*sql.DB)(func(http.ResponseWriter, *http.Request))
  UrlPath() string
}

type QueryResult struct {
  dbconn *sql.DB
  query string
  urlpath string
  doScan func(*sql.Rows)(interface{},error)
}

func (qr *QueryResult)HandleFunc(dbconn *sql.DB) func(http.ResponseWriter, *http.Request) {
  return func(w http.ResponseWriter, r *http.Request){
    res, err := dbconn.Query(qr.query)
    if( err != nil  ){
      w.WriteHeader(http.StatusInternalServerError)
      fmt.Fprintf(w,"Error with query %s: %s", qr.query, err.Error())
      return
    }
    rows := make([]interface{},0)
    for res.Next() {
      if data, err := qr.doScan(res); err != nil {
        panic(err) //TODO
      } else {
        rows = append(rows, data)
      }
    }
    j, err := json.Marshal(rows)
    if err != nil { panic(err) }
    w.Header().Set("Content-Type", "application/json")
    fmt.Fprint(w,string(j))
  }
}

func (qr *QueryResult)UrlPath() string{
  return qr.urlpath
}

func main() {
  dbconn,err := sql.Open("mysql","root@tcp(127.0.0.1:3306)/drcron")
  if err != nil{
    panic(err)
  }
  routes := []RouteHandler{
    &QueryResult{ //Workers
      query: "SELECT id, info, added, heartbeat, exited FROM workers",
      urlpath: "/workers",
      doScan: func(row *sql.Rows)(interface{},error){
        worker := new(WorkerRow) // TODO: passing this as a value works, but the MarshalJSON on main.NullTime no longer invokes.  Why?
        err := row.Scan(&worker.Id, &worker.Info, &worker.Added, &worker.Heartbeat, &worker.Exited)
        return worker, err
      },
    },
    &QueryResult{ // Jobs
      query: "SELECT id, created, updated, name, schedule, command, enabled, run_parallel, schedule_error, schedule_error_time FROM jobs",
      urlpath: "/jobs",
      doScan: func(row *sql.Rows)(interface{},error){
        job:= new(JobRow)
        err := row.Scan(&job.Id, &job.Created, &job.Updated, &job.Name, &job.Schedule, &job.Command, &job.Enabled, &job.Run_parallel, &job.Schedule_error, &job.Schedule_error_time)
        return job, err
      },
    },
    &QueryResult{ //JobExecutions
      query: "SELECT id, job_id, worker_id, queued, scheduled_start, started, pid, ended, exit_code FROM job_executions ",
      urlpath: "/executions",
      doScan: func(row *sql.Rows)(interface{},error){
        je := new(JobExecutionRow)
        err := row.Scan(
          &je.Id, &je.JobId, &je.WorkerId,
          &je.Queued, &je.Scheduled_start, &je.Started,
          &je.Pid, &je.Ended, &je.Exit_code,
        )
        return je, err
      },
    },
  }
  for _, route := range routes {
    http.HandleFunc(route.UrlPath(), route.HandleFunc(dbconn))
  }
	panic(http.ListenAndServe(":8000", nil))
}
