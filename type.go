package orient

import (
	"log"
	"reflect"
	"time"
	"unsafe"
)

// OType is an enum for the various data types supported by OrientDB.
type OType byte

// in alignment with: http://orientdb.com/docs/last/Types.html
const (
	BOOLEAN      OType = 0
	INTEGER      OType = 1
	SHORT        OType = 2
	LONG         OType = 3
	FLOAT        OType = 4
	DOUBLE       OType = 5
	DATETIME     OType = 6
	STRING       OType = 7
	BINARY       OType = 8 // means []byte
	EMBEDDED     OType = 9
	EMBEDDEDLIST OType = 10
	EMBEDDEDSET  OType = 11
	EMBEDDEDMAP  OType = 12
	LINK         OType = 13
	LINKLIST     OType = 14
	LINKSET      OType = 15
	LINKMAP      OType = 16
	BYTE         OType = 17
	TRANSIENT    OType = 18
	DATE         OType = 19
	CUSTOM       OType = 20
	DECIMAL      OType = 21
	LINKBAG      OType = 22
	ANY          OType = 23
	UNKNOWN OType = 255 // driver addition
)

// detect the int size (4 or 8 bytes)
const (
	intSize  = unsafe.Sizeof(int(0))
	uintSize = unsafe.Sizeof(uint(1))
)

func (t OType) String() string { // do not change - it may be used as field type for SQL queries
	switch t {
	case BOOLEAN:
		return "BOOLEAN"
	case INTEGER:
		return "INTEGER"
	case LONG:
		return "LONG"
	case FLOAT:
		return "FLOAT"
	case DOUBLE:
		return "DOUBLE"
	case DATETIME:
		return "DATETIME"
	case STRING:
		return "STRING"
	case BINARY:
		return "BINARY"
	case EMBEDDED:
		return "EMBEDDED"
	case EMBEDDEDLIST:
		return "EMBEDDEDLIST"
	case EMBEDDEDSET:
		return "EMBEDDEDSET"
	case EMBEDDEDMAP:
		return "EMBEDDEDMAP"
	case LINK:
		return "LINK"
	case LINKLIST:
		return "LINKLIST"
	case LINKSET:
		return "LINKSET"
	case LINKMAP:
		return "LINKMAP"
	case BYTE:
		return "BYTE"
	case TRANSIENT:
		return "TRANSIENT"
	case DATE:
		return "DATE"
	case CUSTOM:
		return "CUSTOM"
	case DECIMAL:
		return "DECIMAL"
	case LINKBAG:
		return "LINKBAG"
	case ANY:
		return "ANY"
	default:
		return "UNKNOWN"
	}
}

func (t OType) ReflectKind() reflect.Kind {
	switch t {
	case BOOLEAN:
		return reflect.Bool
	case BYTE:
		return reflect.Uint8
	case SHORT:
		return reflect.Int16
	case INTEGER:
		return reflect.Int32
	case LONG:
		return reflect.Int64
	case FLOAT:
		return reflect.Float32
	case DOUBLE:
		return reflect.Float64
	case STRING:
		return reflect.String
	case EMBEDDEDLIST, EMBEDDEDSET:
		return reflect.Slice
	case EMBEDDEDMAP:
		return reflect.Map
	case LINKLIST, LINKSET:
		return reflect.Slice
	case LINKMAP:
		return reflect.Map
	default:
		return reflect.Invalid
	}
}

func (t OType) ReflectType() reflect.Type {
	switch t {
	case BOOLEAN:
		return reflect.TypeOf(bool(false))
	case INTEGER:
		return reflect.TypeOf(int32(0))
	case LONG:
		return reflect.TypeOf(int64(0))
	case FLOAT:
		return reflect.TypeOf(float32(0))
	case DOUBLE:
		return reflect.TypeOf(float64(0))
	case DATETIME, DATE:
		return reflect.TypeOf(time.Time{})
	case STRING:
		return reflect.TypeOf(string(""))
	case BINARY:
		return reflect.TypeOf([]byte{})
	case BYTE:
		return reflect.TypeOf(byte(0))
		//	case EMBEDDED:
		//		return "EMBEDDED"
		//	case EMBEDDEDLIST:
		//		return "EMBEDDEDLIST"
		//	case EMBEDDEDSET:
		//		return "EMBEDDEDSET"
		//	case EMBEDDEDMAP:
		//		return "EMBEDDEDMAP"
		//	case LINK:
		//		return "LINK"
		//	case LINKLIST:
		//		return "LINKLIST"
		//	case LINKSET:
		//		return "LINKSET"
		//	case LINKMAP:
		//		return "LINKMAP"
		//	case CUSTOM:
		//		return "CUSTOM"
		//	case DECIMAL:
		//		return "DECIMAL"
		//	case LINKBAG:
		//		return "LINKBAG"
	default: // and ANY, TRANSIENT
		return reflect.TypeOf((*interface{})(nil)).Elem()
	}
}

func OTypeForValue(val interface{}) (ftype OType) {
	ftype = UNKNOWN
	// TODO: need to add more types: LINKSET, LINKLIST, etc. ...
	switch val.(type) {
	case string:
		ftype = STRING
	case bool:
		ftype = BOOLEAN
	case int32:
		ftype = INTEGER
	case int64:
		ftype = LONG
	case int16:
		ftype = SHORT
	case int:
		if intSize == 4 {
			ftype = INTEGER
		} else {
			ftype = LONG
		}
	case byte, int8:
		ftype = BYTE
	case *Document, DocumentSerializable:
		ftype = EMBEDDED
	case float32:
		ftype = FLOAT
	case float64:
		ftype = DOUBLE
	case []byte:
		ftype = BINARY
	case OIdentifiable:
		ftype = LINK
	case []OIdentifiable, []RID:
		ftype = LINKLIST
	case *RidBag:
		ftype = LINKBAG
	case time.Time:
		ftype = DATETIME
	// TODO: more types need to be added
	default:
		if isDecimal(val) {
			ftype = DECIMAL
			return
		}
		rt := reflect.TypeOf(val)
		if rt.Kind() == reflect.Ptr {
			rt = rt.Elem()
		}
		switch rt.Kind() {
		case reflect.Map:
			ftype = EMBEDDEDMAP
		case reflect.Slice, reflect.Array:
			if reflect.TypeOf(val).Elem() == reflect.TypeOf(byte(0)) {
				ftype = BINARY
			} else {
				ftype = EMBEDDEDLIST
			}
		case reflect.Bool:
			ftype = BOOLEAN
		case reflect.Uint8:
			ftype = BYTE
		case reflect.Int16:
			ftype = SHORT
		case reflect.Int32:
			ftype = INTEGER
		case reflect.Int64:
			ftype = LONG
		case reflect.Int:
			if intSize == 4 {
				ftype = INTEGER
			} else {
				ftype = LONG
			}
		case reflect.Uint:
			if uintSize == 4 {
				ftype = INTEGER
			} else {
				ftype = LONG
			}
		case reflect.Uint64:
			ftype = LONG
		case reflect.String:
			ftype = STRING
		case reflect.Struct:
			ftype = EMBEDDED
		default:
			log.Printf("unknown type in serialization: %T, kind: %v", val, reflect.TypeOf(val).Kind())
		}
	}
	return
}

func OTypeFromString(typ string) OType {
	switch typ {
	case "BOOLEAN":
		return BOOLEAN
	case "INTEGER":
		return INTEGER
	case "SHORT":
		return SHORT
	case "LONG":
		return LONG
	case "FLOAT":
		return FLOAT
	case "DOUBLE":
		return DOUBLE
	case "DATETIME":
		return DATETIME
	case "STRING":
		return STRING
	case "BINARY":
		return BINARY
	case "EMBEDDED":
		return EMBEDDED
	case "EMBEDDEDLIST":
		return EMBEDDEDLIST
	case "EMBEDDEDSET":
		return EMBEDDEDSET
	case "EMBEDDEDMAP":
		return EMBEDDEDMAP
	case "LINK":
		return LINK
	case "LINKLIST":
		return LINKLIST
	case "LINKSET":
		return LINKSET
	case "LINKMAP":
		return LINKMAP
	case "BYTE":
		return BYTE
	case "TRANSIENT":
		return TRANSIENT
	case "DATE":
		return DATE
	case "CUSTOM":
		return CUSTOM
	case "DECIMAL":
		return DECIMAL
	case "LINKBAG":
		return LINKBAG
	case "ANY":
		return ANY
	default:
		panic("Unkwown type: " + typ)
	}
}
