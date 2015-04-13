package obinary

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"strconv"
	"strings"

	"github.com/quux00/ogonori/constants"
	"github.com/quux00/ogonori/obinary/rw"
	"github.com/quux00/ogonori/oerror"
	"github.com/quux00/ogonori/ogl"
	"github.com/quux00/ogonori/oschema"
)

//
// OpenDatabase sends the REQUEST_DB_OPEN command to the OrientDb server to
// open the db in read/write mode.  The database name and type are required, plus
// username and password.  Database type should be one of the obinary constants:
// DocumentDbType or GraphDbType.
//
func OpenDatabase(dbc *DBClient, dbname string, dbtype constants.DatabaseType, username, passw string) error {
	buf := dbc.buf
	buf.Reset()

	// first byte specifies request type
	err := rw.WriteByte(buf, REQUEST_DB_OPEN)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// session-id - send a negative number to create a new server-side conx
	err = rw.WriteInt(buf, RequestNewSession)
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = rw.WriteStrings(buf, DriverName, DriverVersion)
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = rw.WriteShort(buf, dbc.binaryProtocolVersion)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// dbclient id - send as null, but cannot be null if clustered config
	// TODO: change to use dbc.clusteredConfig once that is added
	err = rw.WriteNull(buf)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// serialization-impl
	err = rw.WriteString(buf, dbc.serializationType)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// token-session  // TODO: hardcoded as false for now -> change later based on ClientOptions settings
	err = rw.WriteBool(buf, false)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// dbname, dbtype, username, password
	err = rw.WriteStrings(buf, dbname, string(dbtype), username, passw)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// now send to the OrientDB server
	_, err = dbc.conx.Write(buf.Bytes())
	if err != nil {
		return oerror.NewTrace(err)
	}

	/* ---[ read back response ]--- */

	// first byte indicates success/error
	status, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}

	dbc.currDb = NewDatabase(dbname, dbtype)

	// the first int returned is the session id sent - which was the `RequestNewSession` sentinel
	sessionValSent, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}
	if sessionValSent != RequestNewSession {
		return errors.New("Unexpected Error: Server did not return expected session-request-val that was sent")
	}

	// if status returned was ERROR, then the rest of server data is the exception info
	if status != RESPONSE_STATUS_OK {
		exceptions, err := rw.ReadErrorResponse(dbc.conx)
		if err != nil {
			return oerror.NewTrace(err)
		}
		return fmt.Errorf("Server Error(s): %v", exceptions)
	}

	// for the REQUEST_DB_OPEN case, another int is returned which is the new sessionId
	sessionId, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}
	dbc.sessionId = sessionId

	// next is the token, which may be null
	tokenBytes, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}
	dbc.token = tokenBytes

	// array of cluster info in this db // TODO: do we need to retain all this in memory?
	numClusters, err := rw.ReadShort(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}

	clusters := make([]OCluster, 0, numClusters)

	for i := 0; i < int(numClusters); i++ {
		clusterName, err := rw.ReadString(dbc.conx)
		if err != nil {
			return oerror.NewTrace(err)
		}
		clusterId, err := rw.ReadShort(dbc.conx)
		if err != nil {
			return oerror.NewTrace(err)
		}
		clusters = append(clusters, OCluster{Name: clusterName, Id: clusterId})
	}
	dbc.currDb.Clusters = clusters

	// cluster-config - bytes - null unless running server in clustered config
	// TODO: treating this as an opaque blob for now
	clusterCfg, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}
	dbc.currDb.ClustCfg = clusterCfg

	// orientdb server release - throwing away for now // TODO: need this?
	_, err = rw.ReadString(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// load #0:0
	schemaRID, err := loadConfigRecord(dbc)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// load schemaRecord (usually #0:1)
	err = loadSchema(dbc, schemaRID)
	if err != nil {
		return oerror.NewTrace(err)
	}

	return nil
}

//
// loadConfigRecord loads record #0:0 for the current database, caching
// some of the information returned into OStorageConfiguration
//
func loadConfigRecord(dbc *DBClient) (schemaRID string, err error) {
	// The config record comes back as type 'b' (binary?), which should just be converted
	// to a string then tokenized by the pipe char

	dbc.buf.Reset()
	var (
		clusterId  int16
		clusterPos int64
	)
	err = writeCommandAndSessionId(dbc, REQUEST_RECORD_LOAD)
	if err != nil {
		return schemaRID, err
	}

	clusterId = 0
	err = rw.WriteShort(dbc.buf, clusterId)
	if err != nil {
		return schemaRID, err
	}

	clusterPos = 0
	err = rw.WriteLong(dbc.buf, clusterPos)
	if err != nil {
		return schemaRID, err
	}

	fetchPlan := "*:-1 index:0"
	err = rw.WriteString(dbc.buf, fetchPlan)
	if err != nil {
		return schemaRID, err
	}

	ignoreCache := true
	err = rw.WriteBool(dbc.buf, ignoreCache)
	if err != nil {
		return schemaRID, err
	}

	loadTombstones := true // based on Java client code
	err = rw.WriteBool(dbc.buf, loadTombstones)
	if err != nil {
		return schemaRID, err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return schemaRID, err
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return schemaRID, err
	}

	payloadStatus, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return schemaRID, err
	}

	if payloadStatus == byte(0) {
		return schemaRID, errors.New("Payload status for #0:0 load was 0. No config data returned.")
	}

	rectype, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return schemaRID, err
	}

	// this is the record version - don't see a reason to check or cache it right now
	_, err = rw.ReadInt(dbc.conx)
	if err != nil {
		return schemaRID, err
	}

	databytes, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return schemaRID, err
	}

	if rectype != 'b' {
		if err != nil {
			return schemaRID, fmt.Errorf("Expected rectype %d, but was: %d", 'b', rectype)
		}
	}

	payloadStatus, err = rw.ReadByte(dbc.conx)
	if err != nil {
		return schemaRID, err
	}

	if payloadStatus != byte(0) {
		return schemaRID,
			errors.New("Second Payload status for #0:0 load was not 0. More than one record returned unexpectedly")
	}

	err = parseConfigRecord(dbc.currDb, string(databytes))
	if err != nil {
		return schemaRID, err
	}

	schemaRID = dbc.currDb.StorageCfg.schemaRID
	return schemaRID, err
}

//
// parseConfigRecord takes the pipe-separate values that comes back
// from reading record #0:0 and turns it into an OStorageConfiguration
// object, which it adds to the db database object.
// TODO: move this function to be a method of OStorageConfiguration?
//
func parseConfigRecord(db *ODatabase, psvData string) error {
	sc := OStorageConfiguration{}

	toks := strings.Split(psvData, "|")

	version, err := strconv.ParseInt(toks[0], 10, 8)
	if err != nil {
		return err
	}

	sc.version = byte(version)
	sc.name = strings.TrimSpace(toks[1])
	sc.schemaRID = strings.TrimSpace(toks[2])
	sc.dictionaryRID = strings.TrimSpace(toks[3])
	sc.idxMgrRID = strings.TrimSpace(toks[4])
	sc.localeLang = strings.TrimSpace(toks[5])
	sc.localeCountry = strings.TrimSpace(toks[6])
	sc.dateFmt = strings.TrimSpace(toks[7])
	sc.dateTimeFmt = strings.TrimSpace(toks[8])
	sc.timezone = strings.TrimSpace(toks[9])

	db.StorageCfg = sc

	return nil
}

//
// loadSchema loads record #0:1 for the current database, caching the
// SchemaVersion, GlobalProperties and Classes info in the current ODatabase
// object (dbc.currDb).
//
func loadSchema(dbc *DBClient, schemaRID string) error {
	docs, err := GetRecordByRID(dbc, schemaRID, "*:-1 index:0") // fetchPlan used by the Java client
	if err != nil {
		return err
	}
	// TODO: this idea of returning multiple docs has to be wrong
	if len(docs) != 1 {
		return fmt.Errorf("Load Record %s should only return one record. Returned: %d", schemaRID, len(docs))
	}

	/* ---[ schemaVersion ]--- */
	dbc.currDb.SchemaVersion = docs[0].GetField("schemaVersion").Value.(int32)

	/* ---[ globalProperties ]--- */
	globalPropsFld := docs[0].GetField("globalProperties")

	var globalProperty oschema.OGlobalProperty
	for _, pfield := range globalPropsFld.Value.([]interface{}) {
		pdoc := pfield.(*oschema.ODocument)
		globalProperty = oschema.NewGlobalPropertyFromDocument(pdoc)
		dbc.currDb.GlobalProperties[int(globalProperty.Id)] = globalProperty
	}

	ogl.Debugln("=======================================")
	ogl.Debugln("=======================================")
	ogl.Debugf("dbc.currDb.SchemaVersion: %v\n", dbc.currDb.SchemaVersion)
	ogl.Debugf("len(dbc.currDb.GlobalProperties): %v\n", len(dbc.currDb.GlobalProperties))
	ogl.Debugf("dbc.currDb.GlobalProperties[19].Name: %v\n", dbc.currDb.GlobalProperties[19].Name)
	ogl.Debugf("dbc.currDb.GlobalProperties[2].Type: %v\n", dbc.currDb.GlobalProperties[2].Type)
	ogl.Debugf("dbc.currDb.GlobalProperties[13].Name: %v\n", dbc.currDb.GlobalProperties[13].Name)
	ogl.Debugf("dbc.currDb.GlobalProperties: %v\n", dbc.currDb.GlobalProperties)
	ogl.Debugln("=======================================")
	ogl.Debugln("=======================================")

	/* ---[ classes ]--- */
	var oclass *oschema.OClass
	classesFld := docs[0].GetField("classes")
	for _, cfield := range classesFld.Value.([]interface{}) {
		cdoc := cfield.(*oschema.ODocument)
		oclass = oschema.NewOClassFromDocument(cdoc)
		dbc.currDb.Classes[oclass.Name] = oclass
	}

	return nil
}

//
// CloseDatabase closes down a session with a specific database that
// has already been opened (via OpenDatabase). This should be called
// when exiting an app or before starting a connection to a different
// OrientDB database.
//
func CloseDatabase(dbc *DBClient) error {
	dbc.buf.Reset()

	err := writeCommandAndSessionId(dbc, REQUEST_DB_CLOSE)
	if err != nil {
		return err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return err
	}

	// the server has no response to a DB_CLOSE

	// remove session, token and currDb info
	dbc.sessionId = NoSessionId
	dbc.token = nil
	dbc.currDb = nil // TODO: anything in currDb that needs to be closed?

	return nil
}

//
// GetDatabaseSize retrieves the size of the current database in bytes.
// It is a database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
//
func GetDatabaseSize(dbc *DBClient) (int64, error) {
	return getLongFromDb(dbc, byte(REQUEST_DB_SIZE))
}

//
// GetNumRecordsInDatabase retrieves the number of records of the current
// database. It is a database-level operation, so OpenDatabase must have
// already been called first in order to start a session with the database.
//
func GetNumRecordsInDatabase(dbc *DBClient) (int64, error) {
	return getLongFromDb(dbc, byte(REQUEST_DB_COUNTRECORDS))
}

func DeleteRecordByRIDAsync(dbc *DBClient, rid string, recVersion int32) error {
	return deleteByRID(dbc, rid, recVersion, true)
}

//
// DeleteRecordByRID deletes a record specified by its RID and its version.
// This is the synchronous version where the server confirms whether the
// delete was successful and the client reports that back to the caller.
// See DeleteRecordByRIDAsync for the async version.
//
// If nil is returned, delete succeeded.
// If error is returned, delete request was either never issued, or there was
// a problem on the server end or the record did not exist in the database.
//
func DeleteRecordByRID(dbc *DBClient, rid string, recVersion int32) error {
	return deleteByRID(dbc, rid, recVersion, false)
}

func deleteByRID(dbc *DBClient, rid string, recVersion int32, async bool) error {
	dbc.buf.Reset()
	var (
		clusterId  int16
		clusterPos int64
		err        error
	)
	rid = strings.TrimPrefix(rid, "#")
	clusterId, clusterPos, err = parseRid(rid)

	err = writeCommandAndSessionId(dbc, REQUEST_RECORD_DELETE)
	if err != nil {
		return err
	}

	err = rw.WriteShort(dbc.buf, clusterId)
	if err != nil {
		return err
	}

	err = rw.WriteLong(dbc.buf, clusterPos)
	if err != nil {
		return err
	}

	err = rw.WriteInt(dbc.buf, recVersion)
	if err != nil {
		return err
	}

	// sync mode ; 0 = synchronous; 1 = asynchronous
	var syncMode byte
	if async {
		syncMode = byte(1)
	}
	err = rw.WriteByte(dbc.buf, syncMode)
	if err != nil {
		return err
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return err
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return err
	}

	payloadStatus, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return err
	}

	// status 1 means record was deleted;
	// status 0 means record was not deleted (either failed or didn't exist)
	if payloadStatus == byte(0) {
		return fmt.Errorf("Server reports record %s was not deleted. Either failed or did not exist.",
			rid)
	}

	return nil
}

//
// GetRecordById takes an RID of the form N:M or #N:M and reads that record from
// the database.
// NOTE: for now I'm assuming all records are Documents (they can also be "raw bytes" or "flat data")
// and for some reason I don't understand, multiple records can be returned, so I'm returning
// a slice of ODocument
//
// TODO: may also want to expose options: ignoreCache, loadTombstones bool
func GetRecordByRID(dbc *DBClient, rid string, fetchPlan string) ([]*oschema.ODocument, error) {
	dbc.buf.Reset()
	var (
		clusterId  int16
		clusterPos int64
		err        error
	)
	rid = strings.TrimPrefix(rid, "#")
	clusterId, clusterPos, err = parseRid(rid)

	err = writeCommandAndSessionId(dbc, REQUEST_RECORD_LOAD)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	err = rw.WriteShort(dbc.buf, clusterId)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	err = rw.WriteLong(dbc.buf, clusterPos)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	err = rw.WriteString(dbc.buf, fetchPlan)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	ignoreCache := true // hardcoding for now
	err = rw.WriteBool(dbc.buf, ignoreCache)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	loadTombstones := false // hardcoding for now
	err = rw.WriteBool(dbc.buf, loadTombstones)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// this query can return multiple records (though I don't understand why)
	// so must do this in a loop
	docs := make([]*oschema.ODocument, 0, 1)
	for {
		payloadStatus, err := rw.ReadByte(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		if payloadStatus == byte(0) {
			break
		}

		rectype, err := rw.ReadByte(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		recversion, err := rw.ReadInt(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		databytes, err := rw.ReadBytes(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		ogl.Debugf("rectype:%v, recversion:%v, len(databytes):%v\n", rectype, recversion, len(databytes))

		if rectype == 'd' {
			// we don't know the classname so set empty value
			doc := oschema.NewDocument("")
			doc.Rid = rid
			doc.Version = recversion

			// the first byte specifies record serialization version
			// use it to look up serializer
			serde := dbc.currDb.RecordSerDes[int(databytes[0])]
			// then strip off the version byte and send the data to the serde
			err = serde.Deserialize(doc, bytes.NewBuffer(databytes[1:]))
			if err != nil {
				return nil, fmt.Errorf("ERROR in Deserialize for rid %v: %v\n", rid, err)
			}
			docs = append(docs, doc)

		} else {
			return nil,
				fmt.Errorf("Only `document` records are currently supported by the client. Record returned was type: %v", rectype)
		}
	}

	return docs, nil
}

//
// parseRid splits an OrientDB RID into its components parts - clusterId
// and clusterPos, returning the integer value of each. Note that the rid
// passed in must NOT have a leading '#'.
//
func parseRid(rid string) (clusterId int16, clusterPos int64, err error) {
	parts := strings.Split(rid, ":")
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("RID %s is not of form x:y", rid)
	}
	id64, err := strconv.ParseInt(parts[0], 10, 16)
	if err != nil {
		return 0, 0, oerror.NewTrace(err)
	}
	clusterId = int16(id64)

	clusterPos, err = strconv.ParseInt(parts[1], 10, 64)
	return clusterId, clusterPos, err
}

//
// SQLCommand executes SQL commands that are not queries. Any SQL statement
// that does not being with "SELECT" should be sent here.  All SELECT
// statements should go to the SQLQuery function.
//
// Commands can be optionally paramterized using ?, such as:
//
//     INSERT INTO Foo VALUES(a, b, c) (?, ?, ?)
//
// The values for the placeholders (currently) must be provided as strings.
//
// Constraints (for now):
// 1. cmds with only simple positional parameters allowed
// 2. cmds with lists of parameters ("complex") NOT allowed
// 3. parameter types allowed: string only for now
//
// SQL commands in OrientDB tend to return one of two types - a string return value or
// one or more documents. The meaning are specific to the type of query.
//
// ----------------
// For example:
// ----------------
//  for a DELETE statement:
//    retval = number of rows deleted (as a string)
//    docs = empty list
//
//  for an INSERT statement:
//    n = ?
//    docs = ?
//
//  for an CREATE CLASS statement:
//    retval = cluster id of the class (TODO: or it might be number of classes in cluster)
//    docs = empty list
//
//  for an DROP CLASS statement:
//    retval = "true" if successful, "" if class didn't exist (technically it returns null)
//    docs = empty list
//
func SQLCommand(dbc *DBClient, sql string, params ...string) (retval string, docs []*oschema.ODocument, err error) {
	dbc.buf.Reset()

	err = writeCommandAndSessionId(dbc, REQUEST_COMMAND)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	mode := byte('s') // synchronous only supported for now
	err = rw.WriteByte(dbc.buf, mode)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	// need a separate buffer to write the command-payload to, so
	// we can calculate its length before writing it to main dbc.buf
	commandBuf := new(bytes.Buffer)

	// "classname" (command-type, really) and the sql command
	err = rw.WriteStrings(commandBuf, "c", sql) // c for command(non-idempotent)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	// SQLCommand
	//  (text:string)
	//  (has-simple-parameters:boolean)
	//  (simple-paremeters:bytes[])  -> serialized Map (EMBEDDEDMAP??)
	//  (has-complex-parameters:boolean)
	//  (complex-parameters:bytes[])  -> serialized Map (EMBEDDEDMAP??)

	serializedParams, err := serializeSimpleSQLParams(dbc, params)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	// has-simple-parameters
	err = rw.WriteBool(commandBuf, serializedParams != nil)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	if serializedParams != nil {
		rw.WriteBytes(commandBuf, serializedParams)
	}

	// FIXME: no complex parameters yet since I don't understand what they are
	// has-complex-paramters => HARDCODING FALSE FOR NOW
	err = rw.WriteBool(commandBuf, false)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	serializedCmd := commandBuf.Bytes()

	// command-payload-length and command-payload
	err = rw.WriteBytes(dbc.buf, serializedCmd)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return "", nil, oerror.NewTrace(err)
	}

	// for synchronous commands the remaining content is an array of form:
	// [(synch-result-type:byte)[(synch-result-content:?)]]+
	// so the final value will by byte(0) to indicate the end of the array
	// and we must use a loop here

	for {
		resType, err := rw.ReadByte(dbc.conx)
		if err != nil {
			return "", nil, oerror.NewTrace(err)
		}
		if resType == byte(0) {
			break
		}

		resultType := rune(resType)
		ogl.Printf("resultType for SQLCommand: %v (%s)\n", resultType, string(rune(resultType)))

		if resultType == 'n' { // null result
			// do nothing - anything need to be done here?

		} else if resultType == 'r' { // single record
			resultType, err := rw.ReadShort(dbc.conx)
			if err != nil {
				return "", nil, oerror.NewTrace(err)
			}

			if resultType == int16(-2) { // null record
				return "", nil, nil
			}
			if resultType == int16(-3) {
				rid, err := readRID(dbc)
				if err != nil {
					return "", nil, oerror.NewTrace(err)
				}
				ogl.Warn(fmt.Sprintf("Code path not seen before!!: SQLCommand resulted in RID: %v\n", rid))
				// TODO: would now load that record from the DB if the user (Go SQL API) wants it
			}
			if resultType != int16(0) {
				_, file, line, _ := runtime.Caller(0)
				return "", nil, fmt.Errorf("Unexpected resultType in SQLCommand (file: %s; line %d): %d",
					file, line+1, resultType)
			}

			doc, err := readSingleRecord(dbc)
			if err != nil {
				return "", nil, oerror.NewTrace(err)
			}

			ogl.Debugf("r>doc = %v\n", doc) // DEBUG
			if docs == nil {
				docs = make([]*oschema.ODocument, 1)
				docs[0] = doc
			}

		} else if resultType == 'l' { // collection of records
			collectionDocs, err := readResultSet(dbc)
			if err != nil {
				return "", nil, oerror.NewTrace(err)
			}

			if docs == nil {
				docs = collectionDocs
			} else {
				docs = append(docs, collectionDocs...)
			}
			break // TODO: not sure this is right !!

		} else if resultType == 'a' { // serialized type
			// TODO: for now I'm going to assume that this always just returns a number as a string (number of rows affected)
			serializedRec, err := rw.ReadBytes(dbc.conx)
			if err != nil {
				return "", nil, oerror.NewTrace(err)
			}
			// ogl.Printf("serializedRec from 'a' return type: %v\n", serializedRec)
			retval = string(serializedRec)
			if err != nil {
				return "", nil, oerror.NewTrace(err)
			}

		} else {
			// TODO: I've not yet tested this route of code -> how do so?
			ogl.Warn(fmt.Sprintf(">> Got back resultType %v (%v): Not yet supported: line:%d; file:%s\n",
				resultType, string(rune(resultType))))
			_, file, line, _ := runtime.Caller(0)
			// TODO: returning here is NOT the correct long-term behavior
			return "", nil, fmt.Errorf("Got back resultType %v (%v): Not yet supported: line:%d; file:%s\n",
				resultType, string(rune(resultType)), line, file)
		}
	}

	return retval, docs, err
}

// TODO: what datatypes can the params be? => right now allowing only string
func serializeSimpleSQLParams(dbc *DBClient, params []string) ([]byte, error) {
	// Java client uses Map<Object, Object>
	// Entry: {0=Honda, 1=Accord}, so positional params start with 0
	// OSQLQuery#serializeQueryParameters(Map<O,O> params)
	//   creates an ODocument
	//   params.put("params", convertToRIDsIfPossible(params))
	//   the convertToRIDsIfPossible is the one that handles Set vs. Map vs. ... vs. else -> primitive which is what simple strings are
	//  then the serialization is done via ODocument#toStream -> ORecordSerializer#toStream
	//    serializeClass(document)  => returns null
	//    only field name in the document is "params"
	//    when the embedded map comes in {0=Honda, 1=Accord}, it calls writeSingleValue

	if len(params) == 0 {
		return nil, nil
	}

	doc := oschema.NewDocument("")

	// the params must be serialized as an embedded map of form:
	// {params => {0=>paramVal1, 1=>paramVal2}}
	// which in ogonori is a Field with:
	//   Field.Name = params
	//   Field.Value = {0=>paramVal1, 1=>paramVal2}} (map[string]interface{})

	paramsMap := oschema.NewEmbeddedMapWithCapacity(2)
	for i, pval := range params {
		paramsMap.Put(strconv.Itoa(i), pval, oschema.STRING)
	}
	doc.FieldWithType("params", paramsMap, oschema.EMBEDDEDMAP)

	ogl.Debugf("DOC XX: %v\n", doc)
	///////

	buf := new(bytes.Buffer)
	err := buf.WriteByte(dbc.serializationVersion)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	serde := dbc.RecordSerDes[int(dbc.serializationVersion)]
	err = serde.Serialize(doc, buf)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	ogl.Debugf("serialized params: %v\n", buf.Bytes())

	return buf.Bytes(), nil

	// ------------------------
	// final byte type = network.readByte();
	//  switch (type) {
	//  case 'n':
	//    result = null;
	//    break;

	//  case 'r':
	//    result = OChannelBinaryProtocol.readIdentifiable(network);
	//    if (result instanceof ORecord)
	//      database.getLocalCache().updateRecord((ORecord) result);
	//    break;

	//  case 'l':
	//    final int tot = network.readInt();
	//    final Collection<OIdentifiable> list = new ArrayList<OIdentifiable>(tot);
	//    for (int i = 0; i < tot; ++i) {
	//      final OIdentifiable resultItem = OChannelBinaryProtocol.readIdentifiable(network);
	//      if (resultItem instanceof ORecord)
	//        database.getLocalCache().updateRecord((ORecord) resultItem);
	//      list.add(resultItem);
	//    }
	//    result = list;
	//    break;

	//  case 'a':  // 'a' means "serialized result"
	//    final String value = new String(network.readBytes());
	//    result = ORecordSerializerStringAbstract.fieldTypeFromStream(null, ORecordSerializerStringAbstract.getType(value),
	//        value);
	//    break;

	//  default:
	//    OLogManager.instance().warn(this, "Received unexpected result from query: %d", type);
	//  }

	return nil, nil
}

//
// SQLQuery
//
// TODO: right now I return the entire resultSet as an array, thus all loaded into memory
//       it would be better to have obinary.dbCommands provide an iterator based model
//       that only needs to read a "row" (ODocument) at a time
// Perhaps SQLQuery() -> iterator/cursor
//         SQLQueryGetAll() -> []*ODocument ??
//
func SQLQuery(dbc *DBClient, sql string, fetchPlan string, params ...string) ([]*oschema.ODocument, error) {
	dbc.buf.Reset()

	err := writeCommandAndSessionId(dbc, REQUEST_COMMAND)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	mode := byte('s') // synchronous only supported for now
	err = rw.WriteByte(dbc.buf, mode)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// need a separate buffer to write the command-payload to, so
	// we can calculate its length before writing it to main dbc.buf
	commandBuf := new(bytes.Buffer)

	err = rw.WriteStrings(commandBuf, "q", sql) // q for query
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// non-text-limit (-1 = use limit from query text)
	err = rw.WriteInt(commandBuf, -1)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// fetch plan
	err = rw.WriteString(commandBuf, fetchPlan)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	serializedParams, err := serializeSimpleSQLParams(dbc, params)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	if serializedParams != nil {
		rw.WriteBytes(commandBuf, serializedParams)
	}

	serializedCmd := commandBuf.Bytes()

	// command-payload-length and command-payload
	err = rw.WriteBytes(dbc.buf, serializedCmd)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	// send to the OrientDB server
	finalBytes := dbc.buf.Bytes()

	_, err = dbc.conx.Write(finalBytes)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	resType, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	resultType := int32(resType)

	var docs []*oschema.ODocument

	if resultType == 'n' {
		panic("Result type in SQLQuery is 'n' -> what to do? nothing ???")

	} else if resultType == 'r' {
		record, err := rw.ReadBytes(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		ogl.Debugf("record = %v\n", record) // DEBUG
		// TODO: I've not yet tested this route of code -> how do so?
		ogl.Fatal("NOTE NOTE NOTE: testing the resultType == 'r' route of code -- remove this note and test it!!!")

	} else if resultType == 'l' {
		docs, err = readResultSet(dbc)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		return docs, err

	} else {
		// TODO: I've not yet tested this route of code -> how do so?
		ogl.Warn(">> Not yet supported")
		ogl.Fatal(fmt.Sprintf("NOTE NOTE NOTE: testing the resultType == '%v' (else) route of code -- "+
			"remove this note and test it!!", string(resultType)))
	}

	return docs, nil
}

//
// GetClusterDataRange returns the range of record ids for a cluster
//
func GetClusterDataRange(dbc *DBClient, clusterName string) (begin, end int64, err error) {
	dbc.buf.Reset()

	clusterId := findClusterWithName(dbc.currDb.Clusters, strings.ToLower(clusterName))
	if clusterId < 0 {
		// TODO: This is problematic - someone else may add the cluster not through this
		//       driver session and then this would fail - so options:
		//       1) do a lookup of all clusters on the DB
		//       2) provide a GetClusterRangeById(dbc, clusterId)
		return begin, end,
			fmt.Errorf("No cluster with name %s is known in database %s\n", clusterName, dbc.currDb.Name)
	}

	err = writeCommandAndSessionId(dbc, REQUEST_DATACLUSTER_DATARANGE)
	if err != nil {
		return begin, end, oerror.NewTrace(err)
	}

	err = rw.WriteShort(dbc.buf, clusterId)
	if err != nil {
		return begin, end, oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return begin, end, oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return begin, end, oerror.NewTrace(err)
	}

	begin, err = rw.ReadLong(dbc.conx)
	if err != nil {
		return begin, end, oerror.NewTrace(err)
	}

	end, err = rw.ReadLong(dbc.conx)
	return begin, end, err
}

//
// AddCluster adds a cluster to the current database. It is a
// database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
// The clusterId is returned if the command is successful.
//
func AddCluster(dbc *DBClient, clusterName string) (clusterId int16, err error) {
	dbc.buf.Reset()

	err = writeCommandAndSessionId(dbc, REQUEST_DATACLUSTER_ADD)
	if err != nil {
		return int16(0), oerror.NewTrace(err)
	}

	cname := strings.ToLower(clusterName)

	err = rw.WriteString(dbc.buf, cname)
	if err != nil {
		return int16(0), oerror.NewTrace(err)
	}

	err = rw.WriteShort(dbc.buf, -1) // -1 means generate new cluster id
	if err != nil {
		return int16(0), oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return int16(0), oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return int16(0), oerror.NewTrace(err)
	}

	clusterId, err = rw.ReadShort(dbc.conx)
	if err != nil {
		return clusterId, oerror.NewTrace(err)
	}

	dbc.currDb.Clusters = append(dbc.currDb.Clusters, OCluster{cname, clusterId})
	return clusterId, err
}

//
// DropCluster drops a cluster to the current database. It is a
// database-level operation, so OpenDatabase must have already
// been called first in order to start a session with the database.
// If nil is returned, then the action succeeded.
//
func DropCluster(dbc *DBClient, clusterName string) error {
	dbc.buf.Reset()

	clusterId := findClusterWithName(dbc.currDb.Clusters, strings.ToLower(clusterName))
	if clusterId < 0 {
		// TODO: This is problematic - someone else may add the cluster not through this
		//       driver session and then this would fail - so options:
		//       1) do a lookup of all clusters on the DB
		//       2) provide a DropClusterById(dbc, clusterId)
		return fmt.Errorf("No cluster with name %s is known in database %s\n", clusterName, dbc.currDb.Name)
	}

	err := writeCommandAndSessionId(dbc, REQUEST_DATACLUSTER_DROP)
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = rw.WriteShort(dbc.buf, clusterId)
	if err != nil {
		return oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return oerror.NewTrace(err)
	}

	delStatus, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}
	if delStatus != byte(1) {
		return fmt.Errorf("Drop cluster action failed. Return code from server was not '1', but %d",
			delStatus)
	}

	return nil
}

//
// GetClusterCountIncludingDeleted gets the number of records in all
// the clusters specified *including* deleted records (applicable for
// autosharded storage only)
//
func GetClusterCountIncludingDeleted(dbc *DBClient, clusterNames ...string) (count int64, err error) {
	return getClusterCount(dbc, true, clusterNames)
}

//
// GetClusterCountIncludingDeleted gets the number of records in all the
// clusters specified. The count does NOT include deleted records in
// autosharded storage. Use GetClusterCountIncludingDeleted if you want
// the count including deleted records
//
func GetClusterCount(dbc *DBClient, clusterNames ...string) (count int64, err error) {
	return getClusterCount(dbc, false, clusterNames)
}

func getClusterCount(dbc *DBClient, countTombstones bool, clusterNames []string) (count int64, err error) {
	dbc.buf.Reset()

	clusterIds := make([]int16, len(clusterNames))
	for i, name := range clusterNames {
		clusterId := findClusterWithName(dbc.currDb.Clusters, strings.ToLower(name))
		if clusterId < 0 {
			// TODO: This is problematic - someone else may add the cluster not through this
			//       driver session and then this would fail - so options:
			//       1) do a lookup of all clusters on the DB
			//       2) provide a GetClusterCountById(dbc, clusterId)
			return int64(0),
				fmt.Errorf("No cluster with name %s is known in database %s\n",
					name, dbc.currDb.Name)
		}
		clusterIds[i] = clusterId
	}

	err = writeCommandAndSessionId(dbc, REQUEST_DATACLUSTER_COUNT)
	if err != nil {
		return int64(0), oerror.NewTrace(err)
	}

	// specify number of clusterIds being sent and then write the clusterIds
	err = rw.WriteShort(dbc.buf, int16(len(clusterIds)))
	if err != nil {
		return int64(0), oerror.NewTrace(err)
	}

	for _, cid := range clusterIds {
		err = rw.WriteShort(dbc.buf, cid)
		if err != nil {
			return int64(0), oerror.NewTrace(err)
		}
	}

	// count-tombstones
	var ct byte
	if countTombstones {
		ct = byte(1)
	}
	err = rw.WriteByte(dbc.buf, ct) // presuming that 0 means "false"
	if err != nil {
		return int64(0), oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return int64(0), oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return int64(0), oerror.NewTrace(err)
	}

	nrecs, err := rw.ReadLong(dbc.conx)
	if err != nil {
		return int64(0), oerror.NewTrace(err)
	}

	return nrecs, err
}

func writeCommandAndSessionId(dbc *DBClient, cmd byte) error {
	if dbc.sessionId == NoSessionId {
		return oerror.SessionNotInitialized{}
	}

	err := rw.WriteByte(dbc.buf, cmd)
	if err != nil {
		return oerror.NewTrace(err)
	}

	err = rw.WriteInt(dbc.buf, dbc.sessionId)
	if err != nil {
		return oerror.NewTrace(err)
	}

	return nil
}

func getLongFromDb(dbc *DBClient, cmd byte) (int64, error) {
	dbc.buf.Reset()

	err := writeCommandAndSessionId(dbc, cmd)
	if err != nil {
		return int64(-1), oerror.NewTrace(err)
	}

	// send to the OrientDB server
	_, err = dbc.conx.Write(dbc.buf.Bytes())
	if err != nil {
		return int64(-1), oerror.NewTrace(err)
	}

	/* ---[ Read Response ]--- */

	err = readStatusCodeAndSessionId(dbc)
	if err != nil {
		return int64(-1), oerror.NewTrace(err)
	}

	// the answer to the query
	longFromDb, err := rw.ReadLong(dbc.conx)
	if err != nil {
		return int64(-1), oerror.NewTrace(err)
	}

	return longFromDb, nil
}

//
// Returns negative number if no cluster with `clusterName` is found
// in the clusters slice.
//
func findClusterWithName(clusters []OCluster, clusterName string) int16 {
	for _, cluster := range clusters {
		if cluster.Name == clusterName {
			return cluster.Id
		}
	}
	return int16(-1)
}

func readStatusCodeAndSessionId(dbc *DBClient) error {
	status, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}

	sessionId, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return oerror.NewTrace(err)
	}
	if sessionId != dbc.sessionId {
		return fmt.Errorf("sessionId from server (%v) does not match client sessionId (%v)",
			sessionId, dbc.sessionId)
	}

	if status == RESPONSE_STATUS_ERROR {
		serverExceptions, err := rw.ReadErrorResponse(dbc.conx)
		if err != nil {
			return oerror.NewTrace(err)
		}
		return fmt.Errorf("Server Error(s): %v", serverExceptions)
	}

	return nil
}

//
// readSingleRecord should be called when a single record (as opposed to a collection of
// records) is returned from a db query/command (REQUEST_COMMAND only ???).
// That is when the server sends back:
//     1) Writing byte (1 byte): 0 [OChannelBinaryServer]   -> SUCCESS
//     2) Writing int (4 bytes): 192 [OChannelBinaryServer] -> session-id
//     3) Writing byte (1 byte): 114 [OChannelBinaryServer] -> 'r'  (single record)
//     4) Writing short (2 bytes): 0 [OChannelBinaryServer] -> full record (not null, not RID only)
// Line 3 can be 'l' or possibly other things. For 'l' call readResultSet.
// Line 4 can be 0=full-record, -2=null, -3=RID only.  For -3, call readRID.  For 0, call this fn.
//
// TODO: it is not a given that this method should always return an ODocument
// The rest of the server response (following after above) is:
//     5) Writing byte (1 byte): 100 [OChannelBinaryServer] -> 'd' (ODocument record)
//     6) Writing short (2 bytes): 12 [OChannelBinaryServer] -> cluster-id
//     7) Writing long (8 bytes): 7 [OChannelBinaryServer]  -> cluster-position
//     8) Writing int (4 bytes): 3 [OChannelBinaryServer]   -> record-version
// Line 5 can be:
//     record-type is
//     'b': raw bytes
//     'f': flat data
//     'd': document
// So it might make sense for readSingleRecord to take value interface{} param of type
// []byte, ??? (for flat data - not sure what that is), or *oschema.ODocument.  Or maybe
// ODocument with OField can handle all those.
//
func readSingleRecord(dbc *DBClient) (*oschema.ODocument, error) {
	// this picks up reading the dbc.conx at:
	// 4) Writing short (2 bytes): 0 [OChannelBinaryServer] -> full record (not null, not RID only)
	// which could be -2=null, -3=RID or 0=full-record

	// recordType can be 'b'=raw bytes; 'd': document; 'f': flat data
	recordType, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	if recordType == byte('b') {
		err = fmt.Errorf("raw bytes ('b') record type -> haven't seen that before. Send to Deserializer?")
		fatal(err)
		return nil, err

	} else if recordType == byte('f') {
		err = fmt.Errorf("flat record ('f') record type -> haven't seen that before. What is it?")
		fatal(err)
		return nil, err
	}

	// if get here, recordType == 'd'
	clusterId, err := rw.ReadShort(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	clusterPos, err := rw.ReadLong(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	recVersion, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	recBytes, err := rw.ReadBytes(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	rid := fmt.Sprintf("%d:%d", clusterId, clusterPos)
	doc, err := createDocument(rid, recVersion, recBytes, dbc)
	ogl.Debugf("::single record doc:::::: %v\n", doc)
	return doc, err
}

//
// readRID should be called when a single record (as opposed to a collection of
// records) is returned from a db query/command (REQUEST_COMMAND only ???).
// That is when the server sends back:
//     1) Writing byte (1 byte): 0 [OChannelBinaryServer]   -> SUCCESS
//     2) Writing int (4 bytes): 192 [OChannelBinaryServer] -> session-id
//     3) Writing byte (1 byte): 114 [OChannelBinaryServer] -> 'r'  (single record)
//     4) Writing short (2 bytes): 0 [OChannelBinaryServer] -> full record (not null, not RID only)
// Line 3 can be 'l' or possibly other things. For 'l' call readResultSet.
// Line 4 can be 0=full-record, -2=null, -3=RID only.  For -3, call readRID.  For 0, call this readSingleRecord.
//
// TODO: this is likely the wrong return val
func readRID(dbc *DBClient) (string, error) {
	// svr response: (-3:short)(cluster-id:short)(cluster-position:long)
	// TODO: impl me -> in the future this may need to call loadRecord for the RID and return the ODocument
	clusterId, err := rw.ReadShort(dbc.conx)
	if err != nil {
		return "", oerror.NewTrace(err)
	}
	clusterPos, err := rw.ReadLong(dbc.conx)
	if err != nil {
		return "", oerror.NewTrace(err)
	}

	return fmt.Sprintf("%d:%d", clusterId, clusterPos), nil
}

//
// should only be called for collections -> TODO: what should be called for single records?
//
func readResultSet(dbc *DBClient) ([]*oschema.ODocument, error) {
	// for Collection
	// next val is: (collection-size:int)
	// and then each record is serialized according to format:
	// (0:short)(record-type:byte)(cluster-id:short)(cluster-position:long)(record-version:int)(record-content:bytes)

	resultSetSize, err := rw.ReadInt(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}

	rsize := int(resultSetSize)
	docs := make([]*oschema.ODocument, rsize)

	for i := 0; i < rsize; i++ {
		// TODO: move code below to readRecordInResultSet
		// this apparently should always be zero for serialized records -> not sure it's meaning
		zero, err := rw.ReadShort(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		if zero != int16(0) {
			return nil, fmt.Errorf("ERROR: readResultSet: expected short value of 0 but is %d", zero)
		}

		recType, err := rw.ReadByte(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		// TODO: may need to check recType here => not sure that clusterId, clusterPos and version follow next if
		//       type is 'b' (raw bytes) or 'f' (flat record)
		//       see the readSingleRecord method (and probably call that one instead?)
		clusterId, err := rw.ReadShort(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		clusterPos, err := rw.ReadLong(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}

		recVersion, err := rw.ReadInt(dbc.conx)
		if err != nil {
			return nil, oerror.NewTrace(err)
		}
		if recType == byte('d') { // Document
			var doc *oschema.ODocument
			rid := fmt.Sprintf("%d:%d", clusterId, clusterPos)
			recBytes, err := rw.ReadBytes(dbc.conx)
			if err != nil {
				return nil, oerror.NewTrace(err)
			}
			doc, err = createDocument(rid, recVersion, recBytes, dbc)
			if err != nil {
				return nil, oerror.NewTrace(err)
			}
			docs[i] = doc

		} else {
			_, file, line, _ := runtime.Caller(0)
			return nil, fmt.Errorf("%v: %v: Record type %v is not yet supported", file, line+1, recType)
		}
	} // end for loop

	end, err := rw.ReadByte(dbc.conx)
	if err != nil {
		return nil, oerror.NewTrace(err)
	}
	if end != byte(0) {
		return nil, fmt.Errorf("Final Byte read from collection result set was not 0, but was: %v", end)
	}
	return docs, nil
}

// TODO: decide if this is needed
// func refreshGlobalProperties(dbc *DBClient) error {
// 	docs, err := GetRecordByRID(dbc, "#0:1", "")
// 	if err != nil {
// 		return err
// 	}
// 	fmt.Println("=======================================\n=======================================\n=======================================")
// 	fmt.Printf("len(docs):: %v\n", len(docs))
// 	doc0 := docs[0]
// 	fmt.Printf("len(doc0.Fields):: %v\n", len(doc0.Fields))
// 	fmt.Println("Field names:")
// 	for k, _ := range doc0.Fields {
// 		fmt.Printf("  %v\n", k)
// 	}
// 	schemaVersion := doc0.Fields["schemaVersion"]
// 	fmt.Printf("%v\n", schemaVersion)
// 	fmt.Printf("%v\n", doc0.Fields["globalProperties"])
// 	return nil
// }
