package orient // import "gopkg.in/istreamdata/orientgo.v2"

import (
	"encoding/json"
	"fmt"
	"strings"
)

const concurrentRetriesDefault = 5

var (
	concurrentRetries = concurrentRetriesDefault
)

// SetRetryCountConcurrent sets a retry count when ErrConcurrentModification occurs.
//
// n == 0 - use default value
//
// n < 0 - no limit for retries
//
// n > 0 - maximum of n retries
func SetRetryCountConcurrent(n int) {
	if n == 0 {
		n = concurrentRetriesDefault
	} else if n < 0 {
		n = -1
	}
	concurrentRetries = n
}

const poolLimit = 6

// FetchPlan is an additional parameter to queries, that instructs DB how to handle linked documents.
//
// The format is:
//
//		(field:depth)*
//
// Field is the name of the field to specify the depth-level. Wildcard '*' means any fields.
//
// Depth is the depth level to fetch. -1 means infinite, 0 means no fetch at all and 1-N the depth level value.
//
// WARN: currently fetch plan have no effect on returned results, as records cache is not implemented yet.
type FetchPlan string

const (
	// DefaultFetchPlan is an empty fetch plan. Let the database decide. Usually means "do not follow any links".
	DefaultFetchPlan = FetchPlan("")
	// NoFollow is a fetch plan that does not follow any links
	NoFollow = FetchPlan("*:0")
	// FollowAll is a fetch plan that follows all links
	FollowAll = FetchPlan("*:-1")
)

// Dial opens a new connection to OrientDB server.
//
// For now, user must import protocol implementation, which will be used for connection:
//
//		import _  "gopkg.in/istreamdata/orientgo.v2/obinary"
//
// Address must be in host:port format. Connection to OrientDB cluster is not supported yet.
//
// Returned Client uses connection pool under the hood, so it can be shared between goroutines.
func Dial(addr string) (*Client, error) {
	dial := protos[ProtoBinary]
	if dial == nil {
		return nil, fmt.Errorf("orientgo: no protocols are active; forgot to import obinary package?")
	}
	cli := &Client{
		dial: func() (DBConnection, error) {
			return dial(addr)
		},
	}
	conn, err := cli.dial()
	if err != nil {
		return nil, err
	}
	cli.mconn = conn
	return cli, nil
}

// Client represents connection to OrientDB server. It is safe for concurrent use.
type Client struct {
	mconn DBConnection
	dial  func() (DBConnection, error)
}

// Auth initiates a new administration session with OrientDB server, allowing to manage databases.
func (c *Client) Auth(user, pass string) (*Admin, error) {
	if c.mconn == nil {
		conn, err := c.dial()
		if err != nil {
			return nil, err
		}
		c.mconn = conn
	}
	m, err := c.mconn.Auth(user, pass)
	if err != nil {
		return nil, err
	}
	return &Admin{c, m}, nil
}

// Open initiates a new database session, allowing to make queries to selected database.
//
// For database management use Auth instead.
func (c *Client) Open(name string, dbType DatabaseType, user, pass string) (*Database, error) {
	db := &Database{newConnPool(poolLimit, func() (DBSession, error) {
		conn, err := c.dial()
		if err != nil {
			return nil, err
		}
		ds, err := conn.Open(name, dbType, user, pass)
		if err != nil {
			conn.Close()
			return nil, err
		}
		return ds, nil
	}), c}
	conn, err := db.pool.getConn()
	if err != nil {
		return nil, err
	}
	db.pool.putConn(conn)
	return db, nil
}

// Close must be called to close all active DB connections.
func (c *Client) Close() error {
	if c.mconn != nil {
		return c.mconn.Close()
	}
	return nil
}

// Admin wraps a database management session.
type Admin struct {
	cli *Client
	db  DBAdmin
}

// DatabaseExists checks if database with given name and storage type exists.
func (a *Admin) DatabaseExists(name string, storageType StorageType) (bool, error) {
	return a.db.DatabaseExists(name, storageType)
}

// CreateDatabase creates a new database with given database type (Document or Graph) and storage type (Persistent or Volatile).
func (a *Admin) CreateDatabase(name string, dbType DatabaseType, storageType StorageType) error {
	return a.db.CreateDatabase(name, dbType, storageType)
}

// DropDatabase removes database from the server.
func (a *Admin) DropDatabase(name string, storageType StorageType) error {
	return a.db.DropDatabase(name, storageType)
}

// ListDatabases returns a list of databases in a form:
//
// 		dbname: dbpath
//
func (a *Admin) ListDatabases() (map[string]string, error) {
	return a.db.ListDatabases()
}

// Close closes DB management session.
func (a *Admin) Close() error {
	return a.db.Close()
}

// Database wraps a database session. It is safe for concurrent use.
type Database struct {
	pool *connPool
	cli  *Client
}

// Size return the size of current database (in bytes).
func (db *Database) Size() (int64, error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return 0, err
	}
	defer db.pool.putConn(conn)
	return conn.Size()
}

// Close closes database session.
func (db *Database) Close() error {
	if db != nil && db.pool != nil {
		return db.pool.close()
	}
	return nil
}

// ReloadSchema reloads documents schema from database.
func (db *Database) ReloadSchema() error {
	conn, err := db.pool.getConn()
	if err != nil {
		return err
	}
	defer db.pool.putConn(conn)
	return conn.ReloadSchema()
}

// GetCurDB returns database metadata
func (db *Database) GetCurDB() *ODatabase {
	conn, err := db.pool.getConn()
	if err != nil {
		return nil
	}
	defer db.pool.putConn(conn)
	return conn.GetCurDB()
}

// AddCluster creates new cluster with given name and returns its ID.
func (db *Database) AddCluster(name string) (int16, error) {
	return db.AddClusterWithID(name, -1) // -1 means generate new cluster id
}

// AddClusterWithID creates new cluster with given cluster position and name
func (db *Database) AddClusterWithID(name string, clusterID int16) (int16, error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return 0, err
	}
	defer db.pool.putConn(conn)
	return conn.AddClusterWithID(name, clusterID)
}

// DropCluster deletes cluster from database
func (db *Database) DropCluster(name string) error {
	conn, err := db.pool.getConn()
	if err != nil {
		return err
	}
	defer db.pool.putConn(conn)
	return conn.DropCluster(name)
}

// GetClusterDataRange returns the begin and end positions of data in the requested cluster.
func (db *Database) GetClusterDataRange(clusterName string) (begin, end int64, err error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return 0, 0, err
	}
	defer db.pool.putConn(conn)
	return conn.GetClusterDataRange(clusterName)
}

// ClustersCount returns total count of records in given clusters
func (db *Database) ClustersCount(withDeleted bool, clusterNames ...string) (int64, error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return 0, err
	}
	defer db.pool.putConn(conn)
	return conn.ClustersCount(withDeleted, clusterNames...)
}

// CreateRecord saves a record to the database. Record RID and version will be changed.
func (db *Database) CreateRecord(rec ORecord) error {
	conn, err := db.pool.getConn()
	if err != nil {
		return err
	}
	defer db.pool.putConn(conn)
	return conn.CreateRecord(rec)
}

// DeleteRecordByRID removes a record from database
func (db *Database) DeleteRecordByRID(rid RID, recVersion int) error {
	conn, err := db.pool.getConn()
	if err != nil {
		return err
	}
	defer db.pool.putConn(conn)
	return conn.DeleteRecordByRID(rid, recVersion)
}

// GetRecordByRID returns a record using specified fetch plan. If ignoreCache is set to true implementations will
// not use local records cache and will fetch record from database.
func (db *Database) GetRecordByRID(rid RID, fetchPlan FetchPlan, ignoreCache bool) (ORecord, error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return nil, err
	}
	defer db.pool.putConn(conn)
	return conn.GetRecordByRID(rid, fetchPlan, ignoreCache)
}

// UpdateRecord updates given record in a database. Record version will be changed after the call.
func (db *Database) UpdateRecord(rec ORecord) error {
	conn, err := db.pool.getConn()
	if err != nil {
		return err
	}
	defer db.pool.putConn(conn)
	return conn.UpdateRecord(rec)
}

// CountRecords returns total records count.
func (db *Database) CountRecords() (int64, error) {
	conn, err := db.pool.getConn()
	if err != nil {
		return 0, err
	}
	defer db.pool.putConn(conn)
	return conn.CountRecords()
}

// Command executes command against current database. Example:
//
//		result := db.Command(NewSQLQuery("SELECT FROM V WHERE id = ?", id).Limit(10))
//
func (db *Database) Command(cmd OCommandRequestText) Results {
	conn, err := db.pool.getConn()
	if err != nil {
		return errorResult{err: err}
	}
	defer db.pool.putConn(conn)
	var result interface{}
	for i := 0; concurrentRetries < 0 || i < concurrentRetries; i++ {
		result, err = conn.Command(cmd)
		err = convertError(err)
		switch err.(type) {
		case ErrConcurrentModification:
			continue
		}
		break
	}
	if err != nil {
		return errorResult{err: convertError(err)}
	}
	return newResults(result)
}

func sqlEscape(s string) string { // TODO: get rid of it
	s = strings.Replace(s, `\`, `\\`, -1)
	s = strings.Replace(s, `"`, `\"`, -1)
	return `"` + s + `"`
}

// CreateScriptFunc is a helper for saving server-side functions to database.
func (db *Database) CreateScriptFunc(fnc Function) error {
	sql := `CREATE FUNCTION ` + fnc.Name + ` ` + sqlEscape(fnc.Code) // TODO: pass as parameter
	if len(fnc.Params) > 0 {
		sql += ` PARAMETERS [` + strings.Join(fnc.Params, ", ") + `]`
	}
	sql += ` IDEMPOTENT ` + fmt.Sprint(fnc.Idemp)
	if fnc.Lang != "" {
		sql += ` LANGUAGE ` + string(fnc.Lang)
	}
	return db.Command(NewSQLCommand(sql)).Err()
}

// DeleteScriptFunc deletes server-side function with a given name from current database.
func (db *Database) DeleteScriptFunc(name string) error {
	return db.Command(NewSQLCommand(`DELETE FROM OFunction WHERE name = ?`, name)).Err()
}

// UpdateScriptFunc updates code of server-side function
func (db *Database) UpdateScriptFunc(name string, script string) error {
	return db.Command(NewSQLCommand(`UPDATE OFunction SET code = ? WHERE name = ?`, script, name)).Err()
}

// CallScriptFunc is a helper for calling server-side functions (especially JS). Ideally should be a shorthand for
//
//		db.Command(NewFunctionCommand(name, params...))
//
// but it uses some workarounds to allow to return JS objects from that functions.
func (db *Database) CallScriptFunc(name string, params ...interface{}) Results {
	//		conn, err := db.pool.getConn()
	//		if err != nil {
	//			return nil, err
	//		}
	//		defer db.pool.putConn(conn)
	//		recs, err := conn.CallScriptFunc(name, params...)
	//		if err != nil {
	//			return recs, err
	//		}
	//		if result != nil {
	//			err = recs.DeserializeAll(result)
	//		}
	//		return recs, err
	sparams := make([]string, 0, len(params))
	for _, p := range params {
		data, _ := json.Marshal(p)

		sparams = append(sparams, string(data))
	}
	cmd := fmt.Sprintf(`var out = %s(%s); (typeof(out) == "object" && out.toString() == "[object Object]" ? (new com.orientechnologies.orient.core.record.impl.ODocument()).fromJSON(JSON.stringify(out)) : out)`,
		name, strings.Join(sparams, ","))
	return db.Command(NewScriptCommand(LangJS, cmd))
}

// InitScriptFunc is a helper for updating all server-side functions to specified state.
func (db *Database) InitScriptFunc(fncs ...Function) (err error) {
	for _, fnc := range fncs {
		if fnc.Lang == "" {
			err = fmt.Errorf("no language provided for function '%s'", fnc.Name)
			return
		}
		db.DeleteScriptFunc(fnc.Name)
		err = db.CreateScriptFunc(fnc)
		if err != nil && !strings.Contains(err.Error(), "found duplicated key") {
			return
		}
	}
	return nil
}

// MarshalContent is a helper for constructing SQL commands with CONTENT keyword.
// Shorthand for json.Marshal. Will panic on errors.
func MarshalContent(o interface{}) string {
	data, err := json.Marshal(o)
	if err != nil {
		panic(err)
	}
	return string(data)
}
