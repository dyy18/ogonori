package obinary

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"gopkg.in/istreamdata/orientgo.v2"
	"gopkg.in/istreamdata/orientgo.v2/obinary/rw"
)

func init() {
	orient.RegisterProto(orient.ProtoBinary, func(addr string) (orient.DBConnection, error) {
		return Dial(addr)
	})
}

func validateAddr(addr string) (string, error) {
	var host, port string
	if addr != "" {
		var err error
		host, port, err = net.SplitHostPort(addr)
		if err != nil {
			return "", err
		}
	}
	if host == "" {
		host = "localhost"
	}
	if port == "" {
		port = "2424" // binary port range is: 2424-2430
	}
	return net.JoinHostPort(host, port), nil
}

// Dial creates a new binary connection to OrientDB server.
// The Client returned is ready to make calls to the OrientDB but has not
// yet established a database session or a session with the OrientDB server.
// After this, the user needs to call either OpenDatabase or CreateServerSession.
func Dial(addr string) (*Client, error) {
	addr, err := validateAddr(addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout("tcp", addr, time.Minute)
	if err != nil {
		return nil, err
	}
	c := &Client{
		addr: addr, conn: conn, done: make(chan struct{}),
		br: bufio.NewReader(conn), bw: bufio.NewWriter(conn),
	}
	c.pr = rw.NewReader(c.br)
	c.pw = rw.NewWriter(c.bw)
	if err := c.handshakeVersion(); err != nil {
		conn.Close()
		return nil, err
	}
	c.sess = make(map[int32]*session)
	c.root = c.newSess(noSessionId)
	go c.run()
	return c, nil
}

// Client encapsulates the active TCP connection to an OrientDB server
// to be used with the Network Binary Protocol.
// It also may be connected to up to one database at a time.
// Do not create a Client struct directly.  You should use NewClient,
// followed immediately by ConnectToServer, to connect to the OrientDB server,
// or OpenDatabase, to connect to a database on the server.
type Client struct {
	addr string

	done chan struct{}

	conn net.Conn
	br   *bufio.Reader
	pr   *rw.Reader
	bw   *bufio.Writer
	pw   *rw.Writer
	cmuw sync.Mutex

	root *session

	sessmu sync.RWMutex
	sess   map[int32]*session

	currmu sync.RWMutex
	currdb *Database // only one db session open at a time

	srvProtoVers int
	curProtoVers int

	recordFormat orient.RecordSerializer
}

func (c *Client) handshakeVersion() error {
	c.conn.SetReadDeadline(time.Now().Add(time.Second * 5))
	defer c.conn.SetReadDeadline(time.Time{})

	c.srvProtoVers = int(c.pr.ReadShort())
	if err := c.pr.Err(); err != nil {
		return err
	}
	if c.srvProtoVers < MinProtocolVersion { // || c.srvProtoVers > MaxProtocolVersion {
		return ErrUnsupportedVersion(c.srvProtoVers)
	} else if c.srvProtoVers < minBinarySerializerVersion { // may switch to CSV serialization, but we don't care for now
		return ErrUnsupportedVersion(c.srvProtoVers)
	} else if c.srvProtoVers > MaxProtocolVersion {
		log.Printf("OrientDB version is unsupported by driver: %d vs %d. Will fallback to protocol %d.",
			MaxProtocolVersion, c.srvProtoVers, CurrentProtoVersion)
	}
	c.recordFormat = orient.GetDefaultRecordSerializer()
	c.curProtoVers = CurrentProtoVersion
	if c.curProtoVers > c.srvProtoVers {
		c.curProtoVers = c.srvProtoVers
	}
	return nil
}

func (c *Client) writeCmd(op byte, sid int32, wr func(*rw.Writer) error) error {
	c.cmuw.Lock()
	defer c.cmuw.Unlock()
	c.pw.WriteByte(op)
	c.pw.WriteInt(sid)
	if wr != nil {
		if err := wr(c.pw); err != nil {
			return err
		}
	}
	c.bw.Flush()
	return c.pw.Err()
}

func (c *Client) newSess(id int32) *session {
	c.sessmu.Lock()
	s := c.sess[id]
	if s == nil {
		s = &session{id: id, cli: c, in: make(chan resp)}
		c.sess[id] = s
	}
	c.sessmu.Unlock()
	return s
}

func (c *Client) closeSess(id int32, ref *Database) {
	c.sessmu.Lock()
	delete(c.sess, id)
	c.sessmu.Unlock()
	c.currmu.Lock()
	if c.currdb == ref {
		c.currdb = nil
	}
	c.currmu.Unlock()
}

func newReadChanCloser(r io.Reader, ch chan struct{}) *readChanCloser {
	return &readChanCloser{
		r: r, done: ch,
	}
}

type readChanCloser struct {
	r    io.Reader
	done chan struct{}
}

func (r *readChanCloser) Read(p []byte) (int, error) {
	select {
	case <-r.done:
		return 0, ErrClosedConnection
	default:
		return r.r.Read(p)
	}
}
func (r *readChanCloser) Close() error {
	select {
	case <-r.done:
	default:
		close(r.done)
	}
	return nil
}

func (c *Client) pushResp(id int32, r io.Reader, e error) {
	var to <-chan time.Time
	c.sessmu.Lock()
	s := c.sess[id]
	c.sessmu.Unlock()
	if s == nil {
		to = time.After(time.Second)
		s = c.newSess(id)
	}
	if r == nil { // no reader, error returned
		select {
		case <-c.done:
		case <-to:
		case s.in <- resp{err: e}:
		}
		return
	}
	done := make(chan struct{})
	select {
	case <-c.done:
	case <-to: // connection expects that response will be read, so stream is broken
		panic(ErrBrokenProtocol{fmt.Errorf("no session %d found", id)})
	case s.in <- resp{ReadCloser: newReadChanCloser(r, done)}:
		<-done
	}
}

// ReadErrorResponse reads an "Exception" message from the OrientDB server.
// The OrientDB server can return multiple exceptions, all of which are
// incorporated into a single OServerException Error struct.
// If error (the second return arg) is not nil, then there was a
// problem reading the server exception on the wire.
func readErrorResponse(r *rw.Reader, protoVers int) (serverException error) {
	var (
		exClass, exMsg string
	)
	exc := make([]orient.Exception, 0, 1) // usually only one ?
	for {
		// before class/message combo there is a 1 (continue) or 0 (no more)
		marker := r.ReadByte()
		if marker == byte(0) {
			break
		}
		exClass = r.ReadString()
		exMsg = r.ReadString()
		exc = append(exc, orient.UnknownException{Class: exClass, Message: exMsg})
	}

	// Next there is a serialized exception of bytes, but it is only
	// useful to Java clients, so read and ignore if present.
	if protoVers >= ProtoVersion19 {
		_ = r.ReadBytes()
	}

	for _, e := range exc {
		switch e.ExcClass() {
		case "com.orientechnologies.orient.core.storage.ORecordDuplicatedException":
			return ODuplicatedRecordException{OServerException: orient.OServerException{Exceptions: exc}}
		}
	}
	return orient.OServerException{Exceptions: exc}
}

func (c *Client) run() error {
	defer close(c.done)
	var (
		status byte
		sessId int32
	)
	for { // TODO: close safely
		status = c.pr.ReadByte()
		sessId = c.pr.ReadInt()
		if err := c.pr.Err(); err != nil {
			return err
		}
		switch status {
		case responseStatusOk:
			c.pushResp(sessId, c.br, nil)
		case responseStatusError:
			e := readErrorResponse(c.pr, c.curProtoVers)
			c.pushResp(sessId, nil, e)
		case responseStatusPush:
			return ErrBrokenProtocol{fmt.Errorf("server push is not supported yet")}
		default:
			return ErrBrokenProtocol{fmt.Errorf("unknown resp status: %d", status)}
		}
	}
}

type resp struct {
	io.ReadCloser
	err error
}

type session struct {
	mu  sync.Mutex
	id  int32
	in  chan resp
	cli *Client
}

func (s *session) catch(err *error) {
	if r := recover(); r != nil {
		switch rr := r.(type) {
		case error:
			*err = rr
		default:
			*err = fmt.Errorf("%v", r)
		}
		go s.cli.Close() // panic means that stream is likely to be broken
	}
}

func (s *session) sendCmd(op byte, wr func(*rw.Writer) error, rd func(*rw.Reader) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.cli.writeCmd(op, s.id, wr); err != nil {
		return err
	}
	if op == requestDbClose {
		return nil
	}
	select {
	case <-s.cli.done:
		return fmt.Errorf("server gone")
	case resp, ok := <-s.in:
		if !ok {
			return ErrClosedConnection
		} else if resp.err != nil {
			return resp.err
		}
		defer resp.Close()
		if rd != nil {
			br := rw.NewReader(resp.ReadCloser.(io.Reader))
			if err := rd(br); err != nil {
				return err
			} else if err = br.Err(); err != nil {
				return err
			}
		}
		return nil
	}
}

func (c *Client) getCurrDB() *Database {
	c.currmu.RLock()
	db := c.currdb
	c.currmu.RUnlock()
	return db
}

// GetCurDB returns database metadata
func (db *Database) GetCurDB() *orient.ODatabase {
	if db == nil || db.db == nil {
		return nil
	}
	return &orient.ODatabase{
		Name:    db.db.Name,
		Type:    db.db.Type,
		Classes: db.db.Classes,
	}
}

// Close closes database connection
func (c *Client) Close() error {
	if c == nil {
		return nil
	}
	if db := c.getCurrDB(); db != nil {
		// ignoring any error here, since closing the conx also terminates the session
		if err := db.Close(); err != nil {
			return err
		}
	}
	return c.conn.Close()
}

func (db *Database) readIdentifiable(r *rw.Reader) (orient.OIdentifiable, error) {
	classId := r.ReadShort()
	if err := r.Err(); err != nil {
		return nil, err
	}
	switch classId {
	case RecordNull:
		return nil, nil
	case RecordRID:
		var rid orient.RID
		if err := rid.FromStream(r); err != nil {
			return nil, err
		}
		return rid, nil
	default:
		tp := orient.RecordType(r.ReadByte())
		if err := r.Err(); err != nil {
			return nil, err
		}
		record := orient.NewRecordOfType(tp)
		switch rec := record.(type) {
		case *orient.Document:
			rec.SetSerializer(db.sess.cli.recordFormat)
		}

		var rid orient.RID
		if err := rid.FromStream(r); err != nil {
			return nil, err
		}
		version := int(r.ReadInt())
		content := r.ReadBytes()

		if err := record.Fill(rid, version, content); err != nil {
			return nil, fmt.Errorf("cannot create record %T from content: %s", record, err)
		}
		return record, nil
	}
}
