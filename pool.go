package orient

import (
	"errors"
	"sync"

	"github.com/Sirupsen/logrus"
)

// -
var (
	ErrPoolClosed    = errors.New("Connection pool is closed")
	ErrDialFactory   = errors.New("Dial factory not set")
	ErrNilConnection = errors.New("Rejecting nil connection")
)

type connPool struct {
	factory func() (DBSession, error)

	mutex    sync.Mutex
	sessions chan DBSession
}

func newConnPool(size int, factory func() (DBSession, error)) *connPool {
	if size <= 0 {
		size = poolLimit
	}
	return &connPool{
		factory:  factory,
		sessions: make(chan DBSession, size),
	}
}

func (p *connPool) getConns() chan DBSession {
	p.mutex.Lock()
	conns := p.sessions
	p.mutex.Unlock()
	return conns
}

func (p *connPool) getConn() (DBSession, error) {
	conns := p.getConns()
	if conns == nil {
		return nil, ErrPoolClosed
	}

	select {
	case conn := <-p.sessions:
		if conn == nil {
			return nil, ErrPoolClosed
		}
		logrus.Debugf("Conn get pool %v", conn)
		return conn, nil
	default:
		if p.factory == nil {
			return nil, ErrDialFactory
		}
		conn, err := p.factory()
		if err != nil {
			return nil, err
		}
		logrus.Debugf("Conn get factory %v", conn)
		return conn, nil
	}
}

func (p *connPool) putConn(conn DBSession) error {
	if conn == nil {
		return ErrNilConnection
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	if p.sessions == nil {
		if err := conn.Close(); err != nil {
			return err
		}
	}

	select {
	case p.sessions <- conn:
		logrus.Debugf("Conn put %v", conn)
		return nil
	default:
		return conn.Close()
	}
}

func (p *connPool) close() error {
	logrus.Debugf("Closing connection pool, %d", p.len())
	p.mutex.Lock()
	conns := p.sessions
	p.sessions = nil
	p.factory = nil
	p.mutex.Unlock()

	if conns == nil {
		return nil
	}

	close(conns)
	for conn := range conns {
		if err := conn.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (p *connPool) len() int {
	return len(p.getConns())
}
