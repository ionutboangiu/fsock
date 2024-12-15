/*
fsock.go is released under the MIT License <http://www.opensource.org/licenses/mit-license.php
Copyright (C) ITsysCOM. All Rights Reserved.

Provides FreeSWITCH socket communication.

*/

package fsock

import (
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

var (
	ErrConnectionPoolTimeout = errors.New("ConnectionPool timeout")
)

// NewFSock connects to FS and starts buffering input.
func NewFSock(addr, passwd string, reconnects int,
	maxReconnectInterval, replyTimeout time.Duration,
	delayFunc func(time.Duration, time.Duration) func() time.Duration,
	eventHandlers map[string][]func(string, int),
	eventFilters map[string][]string,
	logger logger, connIdx int, bgapi bool, stopError chan error,
) (fsock *FSock, err error) {
	if logger == nil ||
		(reflect.ValueOf(logger).Kind() == reflect.Ptr && reflect.ValueOf(logger).IsNil()) {
		logger = nopLogger{}
	}
	fsock = &FSock{
		mu:                   new(sync.RWMutex),
		connIdx:              connIdx,
		addr:                 addr,
		passwd:               passwd,
		eventFilters:         eventFilters,
		eventHandlers:        eventHandlers,
		reconnects:           reconnects,
		maxReconnectInterval: maxReconnectInterval,
		replyTimeout:         replyTimeout,
		delayFunc:            delayFunc,
		logger:               logger,
		bgapi:                bgapi,
		stopError:            stopError,
	}
	if err = fsock.Connect(); err != nil {
		return nil, err
	}
	return
}

// FSock reperesents the connection to FreeSWITCH Socket
type FSock struct {
	mu      *sync.RWMutex
	connIdx int // identifier for the component using this instance of FSock, optional
	addr    string
	passwd  string
	fsConn  *FSConn

	reconnects           int
	maxReconnectInterval time.Duration
	replyTimeout         time.Duration
	delayFunc            func(time.Duration, time.Duration) func() time.Duration // used to create/reset the delay function

	eventFilters  map[string][]string
	eventHandlers map[string][]func(string, int) // eventStr, connId

	logger    logger
	bgapi     bool
	stopError chan error // will communicate on final disconnect
}

// Connect adds locking to connect method.
func (fs *FSock) Connect() (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.connect()
}

// connect establishes a connection to FreeSWITCH using the provided configuration details.
// This method is not thread-safe and should be called with external synchronization if used
// from multiple goroutines. Upon encountering read errors, it automatically attempts to
// restart the connection unless the error is intentionally triggered for stopping.
func (fs *FSock) connect() (err error) {

	// Create an error channel to listen for connection errors.
	connErr := make(chan error)

	// Initialize a new FSConn connection instance. Pass configuration and the error channel.
	fs.fsConn, err = NewFSConn(fs.addr, fs.passwd, fs.connIdx, fs.replyTimeout, connErr,
		fs.logger, fs.eventFilters, fs.eventHandlers, fs.bgapi)
	if err != nil {
		return err
	}

	// Start a goroutine to handle automatic reconnects in case the connection drops.
	go fs.handleConnectionError(connErr)

	return
}

// handleConnectionError listens for connection errors and decides whether to attempt a
// reconnection. It logs errors and manages the stopError channel signaling based on the
// encountered error.
func (fs *FSock) handleConnectionError(connErr chan error) {
	err := <-connErr // wait for an error signal from readEvents

	// All errors will be logged regardless.
	fs.logger.Err(fmt.Sprintf("<FSock> readEvents error (connection index: %d): %v", fs.connIdx, err))

	// NOTE: we could revise handleConnectionError to allow receiving more
	// than error. This way, if it's an error that does require
	// reconnecting but the connection is still alive, we can just continue
	// right after logging the error. This way we do not reach the
	// fs.disconnect() Call and we can keep using the current connection.
	/*
		for err := range connErr{
			...
		}
	*/

	fs.mu.RLock()
	defer fs.mu.RUnlock()

	// NOTE: fs.disconnect() was moved higher because now we do not Close the connection on
	// readHeaders/readBody side anymore to prevent redundant double closes
	// (sometimes on already closed connections).

	// Always making sure the connection is closed to prevent stale connections.
	if err := fs.disconnect(); err != nil {
		fs.logger.Warning(fmt.Sprintf(
			"<FSock> Failed to disconnect from FreeSWITCH (connection index: %d): %v",
			fs.connIdx, err))
	}

	// NOTE: Even though we are receiving a net.ErrClosed error, we cannot
	// use errors.Is to check for it because it is not wrapped properly.
	if strings.Contains(err.Error(), "use of closed network connection") {
		fs.signalError(nil) // intended shutdown
		return
	}

	if !shouldReconnect(err) {
		fs.signalError(err)
		return
	}

	if err := fs.reconnectIfNeeded(); err != nil {
		fs.signalError(err)
	}
}

// shouldReconnect determines if an fsock reconnect is supposed to happen based on err.
// Assumes err is non-nil.
func shouldReconnect(err error) bool {
	var opErr *net.OpError
	var numErr *strconv.NumError
	switch {
	// NOTE: special handling required for errors of type net.OpError. We
	// currently want to reconnect only for ECONNRESET and ETIMEDOUT.
	// Won't be needed anymore after false will be returned on the default case.
	// See below how the shouldReconnect function should look in the end
	// once we have everything figured out.
	case errors.As(err, &opErr):
		// NOTE: opErr.Timeout() check accounts for errors like
		// syscall.ECONNRESET, syscall.ETIMEOUT, syscall.EAGAIN,
		// syscall.EWOULDBLOCK, os.ErrDeadlineExceeded. There could be
		// other which is why I would use the Timeout() method instead.
		if errors.Is(opErr.Err, syscall.ECONNRESET) || opErr.Timeout() {
			return true
		}
		return false
	case
		// NOTE: add the conditions like this in case we don't want to
		// use the Timeout() function. But it might miss some timeout
		// errors.
		//
		// errors.Is(err, syscall.ECONNRESET),
		// errors.Is(err, syscall.ETIMEDOUT),
		// errors.Is(err, syscall.EAGAIN),
		// errors.Is(err, syscall.EWOULDBLOCK),
		// errors.Is(err, os.ErrDeadlineExceeded),

		errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF):
		return true
	case errors.As(err, &numErr):
		// NOTE: previously ignored (would not be logged and would trigger reconnect)

		// for content length parsing errors
		return false
	default:
		// TODO: currently returning true to maintain backwards
		// compatibility. Might want to specifically handle all the
		// errors that we want to reconnect on and change the default
		// case to return false.
		return true
	}
}

/*
This is how shouldReconnect should look like after figuring out exactly on
what errors we want to reconnect on.

func shouldReconnect(err error) bool {
	var opErr *net.OpError
	switch {
	case errors.As(err, &opErr) && opErr.Timeout():
		// Always reconnect on timeout errors.
		return true
	case
		// Add here all the other errors we should reconnect on.
		errors.Is(err, syscall.ECONNRESET), // connection reset by peer
		errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF):
		return true
	default:
		return false
	}
}
*/

// signalError handles logging or sending the error to the stopError channel.
func (fs *FSock) signalError(err error) {
	if fs.stopError == nil {
		// No stopError channel designated. Log the error if not nil.
		if err != nil {
			fs.logger.Err(fmt.Sprintf(
				"<FSock> Error encountered while reading events (connection index: %d): %v",
				fs.connIdx, err))
		}
		return
	}
	// Otherwise, signal on the stopError channel.
	fs.stopError <- err
}

// Connected adds up locking on top of normal connected method.
func (fs *FSock) Connected() (ok bool) {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	return fs.connected()
}

// connected checks if socket connected. Not thread safe.
func (fs *FSock) connected() (ok bool) {
	return fs.fsConn != nil
}

// Disconnect adds up locking for disconnect
func (fs *FSock) Disconnect() (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.disconnect()
}

// Disconnect disconnects from socket
func (fs *FSock) disconnect() (err error) {
	if fs.fsConn != nil {
		fs.logger.Info("<FSock> Disconnecting from FreeSWITCH!")
		err = fs.fsConn.Disconnect()
		fs.fsConn = nil
	}
	return
}

// ReconnectIfNeeded adds up locking to reconnectIfNeeded
func (fs *FSock) ReconnectIfNeeded() (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.reconnectIfNeeded()
}

// reconnectIfNeeded if not connected, attempt reconnect if allowed
func (fs *FSock) reconnectIfNeeded() (err error) {
	if fs.connected() { // No need to reconnect
		return
	}
	delay := fs.delayFunc(time.Second, fs.maxReconnectInterval)
	for i := 0; fs.reconnects == -1 || i < fs.reconnects; i++ { // Maximum reconnects reached, -1 for infinite reconnects
		if err = fs.connect(); err == nil && fs.connected() {
			break // No error or unrelated to connection
		}
		time.Sleep(delay())
	}
	if err == nil && !fs.connected() {
		return errors.New("not connected to FreeSWITCH")
	}
	return // nil or last error in the loop
}

// Generic proxy for commands
func (fs *FSock) SendCmd(cmdStr string) (rply string, err error) {
	fs.mu.Lock() // make sure the fsConn does not get nil-ed after the reconnect
	defer fs.mu.Unlock()
	if err = fs.reconnectIfNeeded(); err != nil {
		return
	}
	return fs.fsConn.Send(cmdStr + "\n") // ToDo: check if we have to send a secondary new line
}

func (fs *FSock) SendCmdWithArgs(cmd string, args map[string]string, body string) (string, error) {
	for k, v := range args {
		cmd += k + ": " + v + "\n"
	}
	if len(body) != 0 {
		cmd += "\n" + body + "\n"
	}
	return fs.SendCmd(cmd)
}

// Send API command
func (fs *FSock) SendApiCmd(cmdStr string) (string, error) {
	return fs.SendCmd("api " + cmdStr + "\n")
}

// SendMsgCmdWithBody command
func (fs *FSock) SendMsgCmdWithBody(uuid string, cmdargs map[string]string, body string) (err error) {
	if len(cmdargs) == 0 {
		return errors.New("need command arguments")
	}
	_, err = fs.SendCmdWithArgs("sendmsg "+uuid+"\n", cmdargs, body)
	return
}

// SendMsgCmd command
func (fs *FSock) SendMsgCmd(uuid string, cmdargs map[string]string) error {
	return fs.SendMsgCmdWithBody(uuid, cmdargs, "")
}

// SendEventWithBody command
func (fs *FSock) SendEventWithBody(eventSubclass string, eventParams map[string]string, body string) (string, error) {
	// Event-Name is overrided to CUSTOM by FreeSWITCH,
	// so we use Event-Subclass instead
	eventParams["Event-Subclass"] = eventSubclass
	return fs.SendCmdWithArgs("sendevent "+eventSubclass+"\n", eventParams, body)
}

// SendEvent command
func (fs *FSock) SendEvent(eventSubclass string, eventParams map[string]string) (string, error) {
	return fs.SendEventWithBody(eventSubclass, eventParams, "")
}

// Send BGAPI command
func (fs *FSock) SendBgapiCmd(cmdStr string) (out chan string, err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	if err := fs.reconnectIfNeeded(); err != nil {
		return out, err
	}
	return fs.fsConn.SendBgapiCmd(cmdStr)
}

func (fs *FSock) LocalAddr() net.Addr {
	fs.mu.RLock()
	defer fs.mu.RUnlock()
	if !fs.connected() {
		return nil
	}
	return fs.fsConn.LocalAddr()
}
