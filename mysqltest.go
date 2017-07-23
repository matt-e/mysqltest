// Package mysqltest provides standalone test instances of mysql sutable for
// use in tests.
package mysqltest

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"text/template"
	"time"

	// We're optionally provide a DB instance backed by this driver.
	_ "github.com/go-sql-driver/mysql"

	"github.com/facebookgo/freeport"
	"github.com/facebookgo/waitout"
)

var mysqlReadyForConnections = []byte("mysqld: ready for connections")

var configTemplate, configTemplateErr = template.New("config").Parse(`
[mysqld]
bind-address                    = 127.0.0.1
datadir                         = {{.DataDir}}
explicit_defaults_for_timestamp = 1
innodb-buffer-pool-size         = 5M
innodb-log-file-size            = 4M
innodb-read-io-threads          = 2
key_buffer_size                 = 16K
max-binlog-size                 = 256K
max-delayed-threads             = 5
max_allowed_packet              = 256K
net_buffer_length               = 2K
port                            = {{.Port}}
socket                          = {{.Socket}}
sort_buffer_size                = 32K
sql_mode                        = ''
thread_cache_size               = 2
thread_stack                    = 128K
user                            = root
`)

var mysqlBaseDir string

func init() {
	if configTemplateErr != nil {
		panic(configTemplateErr)
	}

	out, err := exec.Command("mysqld", "--help", "--verbose").CombinedOutput()
	if err != nil {
		panic(err)
	}

	// The spaces are important.
	hit := regexp.MustCompile(`basedir        .*`).Find(out)
	mysqlBaseDir = string(bytes.TrimSpace(hit[8:]))
}

// Fatalf is satisfied by testing.T or testing.B.
type Fatalf interface {
	Fatalf(format string, args ...interface{})
}

// Server is a unique instance of a mysqld.
type Server struct {
	Port    int
	DataDir string
	Socket  string
	T       Fatalf
	cmd     *exec.Cmd
}

// Start the server, this will return once the server has been started.
func (s *Server) Start() {
	port, err := freeport.Get()
	if err != nil {
		s.T.Fatalf(err.Error())
	}
	s.Port = port

	dir, err := ioutil.TempDir("", "mysql-datadir-")
	if err != nil {
		s.T.Fatalf(err.Error())
	}
	s.DataDir = filepath.Join(dir, "data")
	s.Socket = filepath.Join(dir, "socket")

	cf, err := os.Create(filepath.Join(dir, "my.cnf"))
	if err != nil {
		s.T.Fatalf(err.Error())
	}
	if err := configTemplate.Execute(cf, s); err != nil {
		s.T.Fatalf(err.Error())
	}
	if err := cf.Close(); err != nil {
		s.T.Fatalf(err.Error())
	}

	defaultsFile := fmt.Sprintf("--defaults-file=%s", cf.Name())
	baseDir := fmt.Sprintf("--basedir=%s", mysqlBaseDir)
	s.cmd = exec.Command("mysqld", defaultsFile, "--initialize-insecure", baseDir)
	if os.Getenv("MYSQLTEST_VERBOSE") == "1" {
		s.cmd.Stdout = os.Stdout
		s.cmd.Stderr = os.Stderr
	}
	if err := s.cmd.Run(); err != nil {
		s.T.Fatalf(err.Error())
	}

	waiter := waitout.New(mysqlReadyForConnections)
	s.cmd = exec.Command("mysqld", defaultsFile, "--basedir", mysqlBaseDir)
	if os.Getenv("MYSQLTEST_VERBOSE") == "1" {
		s.cmd.Stdout = os.Stdout
		s.cmd.Stderr = io.MultiWriter(os.Stderr, waiter)
	} else {
		s.cmd.Stderr = waiter
	}
	if err := s.cmd.Start(); err != nil {
		s.T.Fatalf(err.Error())
	}
	waiter.Wait()
}

// Stop the server, this will also remove all data.
func (s *Server) Stop() {
	s.cmd.Process.Kill()
	os.RemoveAll(s.DataDir)
}

// DSN for the mysql server, suitable for use with sql.Open. The suffix is in
// the form "dbname?param=value".
func (s *Server) DSN(suffix string) string {
	return fmt.Sprintf("root@tcp(127.0.0.1:%d)/%s", s.Port, suffix)
}

// DB for the server. The suffix is in the form "dbname?param=value".
func (s *Server) DB(suffix string) *sql.DB {
	db, err := sql.Open("mysql", s.DSN(suffix))
	if err != nil {
		s.T.Fatalf(err.Error())
	}
	return db
}

// Load takes sql statements from reader r and applies them to database db.
// Statements are delimited by a semicolon.
func Load(db *sql.DB, r io.Reader) error {

	scanner := bufio.NewScanner(r)
	scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
		if atEOF && len(data) == 0 {
			return 0, nil, nil
		}
		if i := bytes.IndexByte(data, ';'); i >= 0 {
			return i + 1, data[0:i], nil
		}

		if atEOF {
			return len(data), data, nil
		}
		return 0, nil, nil
	})

	for scanner.Scan() {
		stmt := scanner.Text()
		stmt = strings.Trim(stmt, " \n\r")
		if stmt == "" {
			continue
		}
		_, err := db.Exec(stmt)
		if err != nil {
			return fmt.Errorf("\"%s\" failed: %v", stmt, err)
		}
	}

	return scanner.Err()
}

// NewStartedServer creates a new server starts it.
func NewStartedServer(t Fatalf) *Server {
	for {
		s := &Server{T: t}
		start := make(chan struct{})
		go func() {
			defer close(start)
			s.Start()
		}()
		select {
		case <-start:
			return s
		case <-time.After(30 * time.Second):
		}
	}
}

// NewServerDB creates a new server, starts it, creates the named DB, and
// returns both.
func NewServerDB(t Fatalf, db string) (*Server, *sql.DB) {
	s := NewStartedServer(t)
	if _, err := s.DB("").Exec("create database " + db); err != nil {
		t.Fatalf(err.Error())
	}
	return s, s.DB(db)
}
