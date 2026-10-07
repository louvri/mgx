// Package mgx is a thin, type-safe wrapper over the official MongoDB Go
// driver (go.mongodb.org/mongo-driver/v2), shaped like github.com/louvri/dsx
// and restricted to what Firestore with MongoDB compatibility supports.
//
// It adds four things to the driver and nothing else:
//
//   - generics, so results come back as T instead of being decoded by hand;
//   - an operator enum and a fluent builder instead of hand-written bson.D;
//   - keyset ("cursor") pagination, which the driver does not provide;
//   - Firestore's connection rules, applied so callers cannot forget them.
//
// Everything else is the driver's: [DB.Collection] returns the underlying
// *mongo.Collection for anything mgx does not cover.
//
// # Differences from dsx
//
//   - A Datastore kind becomes a collection and a key becomes the document's
//     _id. An _id may be any type Firestore accepts (string, ObjectID, int32,
//     int64, double, binary, document), so ids are typed any or generic.
//   - There are no namespaces and no ancestor queries; MongoDB has neither.
//   - Cursors are keyset tokens built by mgx, not server cursors. See
//     [QueryBuilder.SelectWithCursor] for what that requires of the data.
//     Datastore leaves out an entity that lacks a sort property; mgx fails
//     the page instead, since MongoDB sorts such a document rather than
//     dropping it.
//   - OpNotEqual and OpNotIn also match documents that lack the field.
//     Datastore skips those; MongoDB does not.
//   - A transaction is carried by the context, so mgx calls made with the
//     callback's context join it. See [RunInTransaction].
package mgx

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math"
	"reflect"
	"slices"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// Sentinel errors reported by mgx. Compare them with errors.Is; the returned
// errors are wrapped with the operation and collection that produced them.
var (
	// ErrNotFound reports that a lookup matched no document. It is returned by
	// [QueryBuilder.Get] and [GetByID] instead of a nil document and nil error.
	ErrNotFound = errors.New("mgx: document not found")

	// ErrPaginationConflict reports that a query mixes offset-based and
	// cursor-based pagination.
	ErrPaginationConflict = errors.New("mgx: offset and cursor pagination are mutually exclusive")

	// ErrInvalidCursor reports a cursor that cannot be decoded, or one that
	// was issued for a different ordering than the query it is used with.
	ErrInvalidCursor = errors.New("mgx: invalid cursor")
)

// Batch sizes. The driver already splits a batch to the limits a server
// advertises; these keep each request small and make a failure easy to place.
// Firestore documents no per-request write count, so they are conservative
// values carried over from dsx, not Firestore limits.
const (
	maxWriteBatch  = 500
	maxLookupBatch = 1000
)

// FieldID is the name of the document id field, for use with WithFilter and
// the ordering methods.
const FieldID = "_id"

// FilterOperator is a comparison operator for [QueryBuilder.WithFilter].
// Every one of these is supported by Firestore with MongoDB compatibility.
type FilterOperator string

// Filter operators, each sent as the MongoDB query operator it is named after.
const (
	OpEqual        FilterOperator = "$eq"
	OpNotEqual     FilterOperator = "$ne"
	OpGreater      FilterOperator = "$gt"
	OpGreaterEqual FilterOperator = "$gte"
	OpLess         FilterOperator = "$lt"
	OpLessEqual    FilterOperator = "$lte"
	// OpIn and OpNotIn take a non-empty slice of any element type.
	OpIn    FilterOperator = "$in"
	OpNotIn FilterOperator = "$nin"
)

func (o FilterOperator) valid() bool {
	switch o {
	case OpEqual, OpNotEqual, OpGreater, OpGreaterEqual, OpLess, OpLessEqual, OpIn, OpNotIn:
		return true
	}
	return false
}

// DB is a connection to one database. It is safe for concurrent use and is
// meant to be created once and shared.
//
// Firestore allows a single database per connection, so a DB is bound to one.
type DB struct {
	client *mongo.Client
	db     *mongo.Database

	// The client's encoding settings, for the documents and ids mgx encodes
	// itself; see [DB.marshal].
	registry *bson.Registry
	encode   []func(*bson.Encoder)
}

// Option configures [Connect].
type Option func(*connectOptions)

type connectOptions struct {
	clientOptions []*options.ClientOptions
}

// WithClientOptions passes extra options to the driver. They are applied
// after the URI, so they override it, except for retryable writes, which
// Connect always disables.
func WithClientOptions(opts ...*options.ClientOptions) Option {
	return func(c *connectOptions) { c.clientOptions = append(c.clientOptions, opts...) }
}

// Connect creates a client for the given database. It does not open a
// connection; like the driver, that happens on first use. Use [DB.Client] to
// ping if you want to fail fast.
//
// Retryable writes are switched off whatever the URI says, because Firestore
// does not support them.
//
// A Firestore URI has this shape, with the auth part depending on where the
// code runs:
//
//	// GKE, Cloud Run, Compute Engine (workload identity)
//	mongodb://UID.LOCATION.firestore.goog:443/DATABASE?loadBalanced=true&tls=true&retryWrites=false&authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE
//
//	// Username and password
//	mongodb://USER:PASSWORD@UID.LOCATION.firestore.goog:443/DATABASE?loadBalanced=true&tls=true&retryWrites=false&authMechanism=SCRAM-SHA-256
func Connect(uri, database string, opts ...Option) (*DB, error) {
	if database == "" {
		return nil, errors.New("mgx: connect: database name is empty")
	}
	var cfg connectOptions
	for _, opt := range opts {
		opt(&cfg)
	}
	clientOpts := make([]*options.ClientOptions, 0, len(cfg.clientOptions)+2)
	clientOpts = append(clientOpts, options.Client().ApplyURI(uri))
	clientOpts = append(clientOpts, cfg.clientOptions...)
	clientOpts = append(clientOpts, options.Client().SetRetryWrites(false))

	// Merged here, as the driver would merge them, so the effective encoding
	// settings are known.
	merged := options.MergeClientOptions(clientOpts...)
	client, err := mongo.Connect(merged)
	if err != nil {
		return nil, fmt.Errorf("mgx: connect: %w", err)
	}
	return &DB{
		client:   client,
		db:       client.Database(database),
		registry: merged.Registry,
		encode:   encoderSetters(merged.BSONOptions),
	}, nil
}

// encoderSetters returns the Encoder methods that o switches on, mirroring
// mongo.getEncoder. Decode-only options are not listed.
func encoderSetters(o *options.BSONOptions) []func(*bson.Encoder) {
	if o == nil {
		return nil
	}
	var on []func(*bson.Encoder)
	for _, e := range []struct {
		set bool
		fn  func(*bson.Encoder)
	}{
		{o.ErrorOnInlineDuplicates, (*bson.Encoder).ErrorOnInlineDuplicates},
		{o.IntMinSize, (*bson.Encoder).IntMinSize},
		{o.NilByteSliceAsEmpty, (*bson.Encoder).NilByteSliceAsEmpty},
		{o.NilMapAsEmpty, (*bson.Encoder).NilMapAsEmpty},
		{o.NilSliceAsEmpty, (*bson.Encoder).NilSliceAsEmpty},
		{o.OmitZeroStruct, (*bson.Encoder).OmitZeroStruct},
		{o.OmitEmpty, (*bson.Encoder).OmitEmpty},
		{o.StringifyMapKeysWithFmt, (*bson.Encoder).StringifyMapKeysWithFmt},
		{o.UseJSONStructTags, (*bson.Encoder).UseJSONStructTags},
	} {
		if e.set {
			on = append(on, e.fn)
		}
	}
	return on
}

// marshal encodes v as the driver encodes documents for this client,
// honouring a registry or BSON options passed to Connect. Without this, a
// document mgx encodes itself could be stored with other field names or
// types than the same document written by the driver. The option mapping
// mirrors the driver's own (mongo.getEncoder).
func (db *DB) marshal(v any) (bson.Raw, error) {
	var buf bytes.Buffer
	enc := bson.NewEncoder(bson.NewDocumentWriter(&buf))
	for _, set := range db.encode {
		set(enc)
	}
	if db.registry != nil {
		enc.SetRegistry(db.registry)
	}
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// Client returns the underlying driver client.
func (db *DB) Client() *mongo.Client { return db.client }

// Database returns the underlying driver database.
func (db *DB) Database() *mongo.Database { return db.db }

// Collection returns the underlying driver collection, for anything mgx does
// not wrap: aggregation pipelines, update operators, index management.
func (db *DB) Collection(name string) *mongo.Collection { return db.db.Collection(name) }

// Close disconnects the client.
func (db *DB) Close(ctx context.Context) error { return db.client.Disconnect(ctx) }

type sortKey struct {
	field string
	desc  bool
}

// QueryBuilder builds and runs a query against one collection.
//
// Builder methods never return an error. The first problem found while
// building is recorded and returned by the terminal call; [QueryBuilder.Err]
// exposes it earlier.
//
// A QueryBuilder is mutable and is not safe for concurrent use. Build one per
// operation; they are cheap.
//
// Filters, ordering, projection and pagination describe a query, and Select
// and SelectWithCursor honour all of them. The other terminals follow one
// rule: a setting that cannot change their result is ignored, and one that
// would change what they act on is rejected rather than ignored.
//
//   - Get ignores a limit; it returns one document either way.
//   - [SelectIDs] replaces the projection with _id alone.
//   - Count, Delete and [Distinct] ignore projection, and ordering apart from
//     Count checking a cursor against it.
//   - Delete and Distinct reject a limit, offset or cursor, which the server
//     cannot apply to them.
//   - The write terminals (Upsert, UpsertMulti, Insert, InsertMulti) address
//     documents by id, so they reject every one of these settings.
type QueryBuilder[T any] struct {
	db         *DB
	coll       string
	filters    bson.A
	order      []sortKey
	projection []string
	limit      int64
	offset     int64
	cursor     string
	err        error
}

// Query starts a query on a collection, decoding documents into T.
func Query[T any](db *DB, collection string) *QueryBuilder[T] {
	qb := &QueryBuilder[T]{db: db, coll: collection}
	if collection == "" {
		qb.err = errors.New("mgx: collection name is empty")
	}
	return qb
}

// Err returns the first error recorded while building the query.
func (qb *QueryBuilder[T]) Err() error { return qb.err }

func (qb *QueryBuilder[T]) fail(err error) *QueryBuilder[T] {
	if qb.err == nil {
		qb.err = err
	}
	return qb
}

func (qb *QueryBuilder[T]) c() *mongo.Collection { return qb.db.db.Collection(qb.coll) }

func (qb *QueryBuilder[T]) wrap(op string, err error) error {
	return fmt.Errorf("mgx: %s %s: %w", op, qb.coll, err)
}

// WithFilter adds a condition. Conditions are combined with AND, and the same
// field may appear more than once, as a range needs.
//
// The value is always sent as an operand, never as a query document, so a
// value taken from a request cannot smuggle in an operator. A field name
// starting with $ is rejected for the same reason: as a top-level key it
// would be an operator, such as $expr, which evaluates the value.
func (qb *QueryBuilder[T]) WithFilter(field string, operator FilterOperator, value any) *QueryBuilder[T] {
	if err := checkField("filter", field); err != nil {
		return qb.fail(err)
	}
	if !operator.valid() {
		return qb.fail(fmt.Errorf("mgx: unknown filter operator %q", string(operator)))
	}
	if operator == OpIn || operator == OpNotIn {
		rv := reflect.ValueOf(value)
		isList := rv.IsValid() && (rv.Kind() == reflect.Slice || rv.Kind() == reflect.Array) &&
			rv.Type().Elem().Kind() != reflect.Uint8 // []byte is one binary value, not a list
		if !isList {
			return qb.fail(fmt.Errorf("mgx: %s on %q needs a slice, got %T", operator, field, value))
		}
		if rv.Len() == 0 {
			return qb.fail(fmt.Errorf("mgx: %s on %q was given an empty slice", operator, field))
		}
	}
	qb.filters = append(qb.filters, bson.D{{Key: field, Value: bson.D{{Key: string(operator), Value: value}}}})
	return qb
}

// WithRawFilter adds a filter written as a driver document, ANDed with the
// rest. It is the escape hatch for operators mgx has no constant for, such as
// $exists, $regex, $or, $all and $elemMatch. Firestore rejects $where.
//
// Unlike WithFilter, the document is sent as written, so do not build it from
// untrusted input.
func (qb *QueryBuilder[T]) WithRawFilter(filter bson.D) *QueryBuilder[T] {
	if len(filter) == 0 {
		return qb.fail(errors.New("mgx: raw filter is empty"))
	}
	qb.filters = append(qb.filters, filter)
	return qb
}

// checkField rejects a field name that is empty or would be read as an
// operator.
func checkField(role, field string) error {
	if field == "" {
		return fmt.Errorf("mgx: %s field is empty", role)
	}
	if strings.HasPrefix(field, "$") {
		return fmt.Errorf("mgx: %s field %q starts with $", role, field)
	}
	return nil
}

func (qb *QueryBuilder[T]) addOrder(field string, desc bool) *QueryBuilder[T] {
	if err := checkField("order", field); err != nil {
		return qb.fail(err)
	}
	for _, k := range qb.order {
		if k.field == field {
			return qb.fail(fmt.Errorf("mgx: field %q is ordered twice", field))
		}
	}
	qb.order = append(qb.order, sortKey{field: field, desc: desc})
	return qb
}

// WithOrder sorts ascending by field. Calls add up, first call first.
func (qb *QueryBuilder[T]) WithOrder(field string) *QueryBuilder[T] { return qb.addOrder(field, false) }

// WithOrderDesc sorts descending by field.
func (qb *QueryBuilder[T]) WithOrderDesc(field string) *QueryBuilder[T] {
	return qb.addOrder(field, true)
}

// WithLimit caps the number of documents. Zero means no limit.
func (qb *QueryBuilder[T]) WithLimit(limit int) *QueryBuilder[T] {
	if limit < 0 {
		return qb.fail(fmt.Errorf("mgx: negative limit %d", limit))
	}
	qb.limit = int64(limit)
	return qb
}

// WithOffset skips documents. The server still walks what it skips, so use
// cursors beyond the first few pages.
func (qb *QueryBuilder[T]) WithOffset(offset int) *QueryBuilder[T] {
	if offset < 0 {
		return qb.fail(fmt.Errorf("mgx: negative offset %d", offset))
	}
	if offset > 0 && qb.cursor != "" {
		return qb.fail(ErrPaginationConflict)
	}
	qb.offset = int64(offset)
	return qb
}

// WithCursor resumes after the document a previous
// [QueryBuilder.SelectWithCursor] ended on. An empty cursor means the first
// page.
func (qb *QueryBuilder[T]) WithCursor(cursor string) *QueryBuilder[T] {
	if cursor != "" && qb.offset > 0 {
		return qb.fail(ErrPaginationConflict)
	}
	qb.cursor = cursor
	return qb
}

// WithProject returns only the named fields. The _id is always returned too.
func (qb *QueryBuilder[T]) WithProject(fields ...string) *QueryBuilder[T] {
	for _, f := range fields {
		if err := checkField("projected", f); err != nil {
			return qb.fail(err)
		}
	}
	qb.projection = append(qb.projection, fields...)
	return qb
}

// keysetOrder is the ordering used for cursor pagination: the caller's order
// with _id appended as a tie-breaker, so that every document has a distinct
// position.
func (qb *QueryBuilder[T]) keysetOrder() []sortKey {
	for _, k := range qb.order {
		if k.field == FieldID {
			return qb.order
		}
	}
	return append(append([]sortKey(nil), qb.order...), sortKey{field: FieldID})
}

func sortDoc(keys []sortKey) bson.D {
	d := make(bson.D, 0, len(keys))
	for _, k := range keys {
		dir := 1
		if k.desc {
			dir = -1
		}
		d = append(d, bson.E{Key: k.field, Value: dir})
	}
	return d
}

// projectionDoc projects the requested fields plus extra, the sort fields a
// cursor is built from. A path under another listed path is left out: the
// server rejects the pair as a path collision, and the parent returns it.
func (qb *QueryBuilder[T]) projectionDoc(extra []sortKey) bson.D {
	if len(qb.projection) == 0 {
		return nil
	}
	fields := slices.Clone(qb.projection)
	for _, k := range extra {
		fields = append(fields, k.field)
	}
	d := make(bson.D, 0, len(fields))
	for i, f := range fields {
		covered := slices.ContainsFunc(fields, func(g string) bool { return strings.HasPrefix(f, g+".") })
		if !covered && !slices.Contains(fields[:i], f) {
			d = append(d, bson.E{Key: f, Value: 1})
		}
	}
	return d
}

// plan is a query resolved to driver terms.
type plan struct {
	filter bson.D
	sort   bson.D
	keys   []sortKey       // non-nil only for cursor pagination
	after  []bson.RawValue // the cursor's sort values, when resuming
}

// plan resolves the query for the terminal op. Its errors are wrapped.
func (qb *QueryBuilder[T]) plan(op string, keyset bool) (plan, error) {
	p, err := qb.resolve(keyset)
	if err != nil {
		return plan{}, qb.wrap(op, err)
	}
	return p, nil
}

func (qb *QueryBuilder[T]) resolve(keyset bool) (plan, error) {
	if qb.err != nil {
		return plan{}, qb.err
	}
	clauses := append(bson.A(nil), qb.filters...)
	p := plan{sort: sortDoc(qb.order)}
	if keyset || qb.cursor != "" {
		if qb.offset > 0 {
			return plan{}, ErrPaginationConflict
		}
		p.keys = qb.keysetOrder()
		p.sort = sortDoc(p.keys)
		if qb.cursor != "" {
			after, vals, err := afterFilter(p.keys, qb.cursor, keyset)
			if err != nil {
				return plan{}, err
			}
			clauses = append(clauses, after)
			p.after = vals
		}
	}
	switch len(clauses) {
	case 0:
		p.filter = bson.D{}
	case 1:
		p.filter = clauses[0].(bson.D)
	default:
		p.filter = bson.D{{Key: "$and", Value: clauses}}
	}
	return p, nil
}

// cursorToken is what a cursor string decodes to: the ordering it was issued
// for and the last document's value for each sort field.
type cursorToken struct {
	Keys   []string        `bson:"k"`
	Values []bson.RawValue `bson:"v"`
}

func keyNames(keys []sortKey) []string {
	names := make([]string, len(keys))
	for i, k := range keys {
		names[i] = k.field + ":1"
		if k.desc {
			names[i] = k.field + ":-1"
		}
	}
	return names
}

// pager checks each result of a cursor query and keeps the last one's sort
// values, from which the next cursor is built.
type pager struct {
	keys  []sortKey
	paths [][]string
	vals  []bson.RawValue // the result being checked; aliases it
	last  []bson.RawValue // the previous result's, or the cursor's; owned
}

// newPager starts a page, resuming from the cursor's values when after is
// set.
func newPager(keys []sortKey, after []bson.RawValue) *pager {
	pg := &pager{
		keys:  keys,
		paths: make([][]string, len(keys)),
		vals:  make([]bson.RawValue, len(keys)),
		last:  make([]bson.RawValue, len(keys)),
	}
	for i, k := range keys {
		pg.paths[i] = strings.Split(k.field, ".")
	}
	for i, v := range after {
		pg.last[i] = bson.RawValue{Type: v.Type, Value: slices.Clone(v.Value)}
	}
	return pg
}

// add checks one result. Every result is checked, not only the last, so one
// that cannot be paged past fails wherever the page boundary falls. Errors
// are left for the caller to wrap.
func (pg *pager) add(doc bson.Raw) error {
	tied := true // the keys before i equal the previous result's
	for i, k := range pg.keys {
		v, err := pg.lookup(doc, i)
		if err != nil {
			return err
		}
		// Key i is compared only where the keys before it tie, so only there
		// must it hold the type it is compared with. An appended _id of mixed
		// types is fine under a unique sort field.
		prev := pg.last[i]
		if tied && prev.Type != 0 && typeClass(v.Type) != typeClass(prev.Type) {
			return fmt.Errorf("cannot page: sort field %q holds both %s and %s values", k.field, prev.Type, v.Type)
		}
		tied = tied && prev.Type != 0 && mayTie(v, prev)
		pg.vals[i] = v
	}
	for i, v := range pg.vals {
		pg.last[i] = bson.RawValue{Type: v.Type, Value: append(pg.last[i].Value[:0], v.Value...)}
	}
	return nil
}

func (pg *pager) lookup(doc bson.Raw, i int) (bson.RawValue, error) {
	field, path := pg.keys[i].field, pg.paths[i]
	v, err := doc.LookupErr(path...)
	if err != nil {
		// A dotted path through an array of documents is not found by
		// LookupErr, but the field is there, and it is an array.
		for j := 1; j < len(path); j++ {
			if p, err := doc.LookupErr(path[:j]...); err == nil && p.Type == bson.TypeArray {
				return v, fmt.Errorf("cannot page: sort field %q runs through an array in a result", field)
			}
		}
		return v, fmt.Errorf("cannot page: sort field %q is missing in a result", field)
	}
	if why := unpageable(v); why != "" {
		return v, fmt.Errorf("cannot page: sort field %q is %s in a result", field, why)
	}
	return v, nil
}

// unpageable says why no cursor can resume at v, or returns "" if one can.
// It guards both the results a cursor is built from and the cursors a
// caller hands back.
func unpageable(v bson.RawValue) string {
	switch v.Type {
	case 0, bson.TypeNull, bson.TypeUndefined:
		return "null"
	case bson.TypeArray:
		// An array sorts by one of its elements, but compares as a whole or
		// element-wise in a filter, so no condition resumes after it.
		return "an array"
	case bson.TypeDouble, bson.TypeDecimal128:
		// NaN equals nothing and is not ordered against numbers in a filter,
		// so no condition resumes after it either.
		f, isDouble := v.DoubleOK()
		d, isDecimal := v.Decimal128OK()
		if (isDouble && math.IsNaN(f)) || (isDecimal && d.IsNaN()) {
			return "NaN"
		}
	}
	return ""
}

// mayTie reports whether two sort values can sort as equal. idKey tells
// int32, int64 and whole doubles apart exactly; other numbers of different
// types are taken to tie, so the next key is checked rather than skipped.
func mayTie(a, b bson.RawValue) bool {
	if idKey(a) == idKey(b) {
		return true
	}
	return a.Type != b.Type && typeClass(a.Type) == bson.TypeDouble && typeClass(b.Type) == bson.TypeDouble
}

// cursor encodes the last result's position. Call it only after an add.
func (pg *pager) cursor() (string, error) {
	b, err := bson.Marshal(cursorToken{Keys: keyNames(pg.keys), Values: pg.last})
	if err != nil {
		return "", fmt.Errorf("cannot build a cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

// typeClass groups the BSON types that compare with each other by value.
func typeClass(t bson.Type) bson.Type {
	switch t {
	case bson.TypeInt32, bson.TypeInt64, bson.TypeDecimal128:
		return bson.TypeDouble
	case bson.TypeSymbol:
		return bson.TypeString
	}
	return t
}

// afterFilter turns a cursor into the condition "sorts after that document",
// and returns the cursor's values. For keys (a, b, c) and values (x, y, z)
// that is
//
//	a > x  OR  (a = x AND b > y)  OR  (a = x AND b = y AND c > z)
//
// with < in place of > for a descending key.
//
// When strict, each > is sent as "not <=" and each < as "not >=". For values
// of the cursor's type that is the same condition, but it also matches what
// a comparison never does: a missing or null field, an array, and a value of
// another type. Those sort among the rest, so a plain > or < would skip them
// without a trace. Matched, they surface in the next page, where the pager
// rejects them. Only SelectWithCursor runs the pager, so only it is strict;
// elsewhere they would be returned, some from before the cursor.
func afterFilter(keys []sortKey, cursor string, strict bool) (bson.D, []bson.RawValue, error) {
	b, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrInvalidCursor, err)
	}
	var tok cursorToken
	if err := bson.Unmarshal(b, &tok); err != nil {
		return nil, nil, fmt.Errorf("%w: %w", ErrInvalidCursor, err)
	}
	if want := keyNames(keys); len(tok.Values) != len(want) || !slices.Equal(tok.Keys, want) {
		return nil, nil, fmt.Errorf("%w: issued for order %v, used with %v", ErrInvalidCursor, tok.Keys, want)
	}
	// mgx never issues these, so the cursor is corrupt or forged. A NaN, say,
	// would otherwise pass as a number and silently restart from page one.
	for i, v := range tok.Values {
		if why := unpageable(v); why != "" {
			return nil, nil, fmt.Errorf("%w: value for %q is %s", ErrInvalidCursor, keys[i].field, why)
		}
	}
	or := make(bson.A, 0, len(keys))
	for i, k := range keys {
		clause := make(bson.D, 0, i+1)
		for j, prev := range keys[:i] {
			clause = append(clause, bson.E{Key: prev.field, Value: bson.D{{Key: "$eq", Value: tok.Values[j]}}})
		}
		op, notOp := "$gt", "$lte"
		if k.desc {
			op, notOp = "$lt", "$gte"
		}
		cond := bson.D{{Key: op, Value: tok.Values[i]}}
		if strict {
			cond = bson.D{{Key: "$not", Value: bson.D{{Key: notOp, Value: tok.Values[i]}}}}
		}
		clause = append(clause, bson.E{Key: k.field, Value: cond})
		or = append(or, clause)
	}
	return bson.D{{Key: "$or", Value: or}}, tok.Values, nil
}

func (qb *QueryBuilder[T]) findOptions(p plan) *options.FindOptionsBuilder {
	opts := options.Find()
	if len(p.sort) > 0 {
		opts.SetSort(p.sort)
	}
	if qb.offset > 0 {
		opts.SetSkip(qb.offset)
	}
	if qb.limit > 0 {
		opts.SetLimit(qb.limit)
	}
	if proj := qb.projectionDoc(p.keys); proj != nil {
		opts.SetProjection(proj)
	}
	return opts
}

// Select runs the query and returns every matching document.
func (qb *QueryBuilder[T]) Select(ctx context.Context) ([]T, error) {
	p, err := qb.plan("select", false)
	if err != nil {
		return nil, err
	}
	cur, err := qb.c().Find(ctx, p.filter, qb.findOptions(p))
	if err != nil {
		return nil, qb.wrap("select", err)
	}
	out := []T{}
	if err := cur.All(ctx, &out); err != nil {
		return nil, qb.wrap("select", err)
	}
	return out, nil
}

// SelectWithCursor runs the query and also returns a cursor for the page
// after it. Pass that cursor to [QueryBuilder.WithCursor] on an identical
// query to continue. When a page comes back empty, the cursor passed in is
// returned unchanged, so a caller never falls back to page one by accident.
//
// This is keyset pagination: the cursor records the last document's sort
// values and the next page asks for documents that sort after them. It costs
// the same at any depth, and it needs three things of the data:
//
//   - every sort field is present, non-null, not NaN and not an array on
//     every matching document;
//   - each sort field holds one type (all numeric types count as one) across
//     the documents that tie on the fields before it, because a range
//     condition does not cross types the way sorting does. For the first
//     field that is every matching document; the _id mgx appends may mix
//     types under a unique field;
//   - an index that covers the filter and the full ordering, ending in _id,
//     which mgx appends as a tie-breaker.
//
// A page holding a document that breaks the first two fails with an error.
// Paging never skips such a document silently: the next page is asked for
// everything that does not sort before the cursor, so one of another type
// surfaces in it rather than being passed over. The other terminals given a
// cursor apply it as a plain range condition, which leaves such documents
// out rather than reporting them.
//
// The cursor is not signed or encrypted. It can only move a reader within the
// results of the query it is used with, but treat it as client-controlled
// input. Anyone holding it can decode the last document's sort values and
// _id, so do not hand a client a cursor over a field it must not see. With a
// projection, the sort fields are fetched and decoded into T as well.
func (qb *QueryBuilder[T]) SelectWithCursor(ctx context.Context) ([]T, string, error) {
	p, err := qb.plan("select", true)
	if err != nil {
		return nil, "", err
	}
	cur, err := qb.c().Find(ctx, p.filter, qb.findOptions(p))
	if err != nil {
		return nil, "", qb.wrap("select", err)
	}
	defer func() { _ = cur.Close(ctx) }()

	out := []T{}
	pg := newPager(p.keys, p.after)
	for cur.Next(ctx) {
		if err := pg.add(cur.Current); err != nil {
			return nil, "", qb.wrap("select", err)
		}
		var v T
		if err := cur.Decode(&v); err != nil {
			return nil, "", qb.wrap("select", err)
		}
		out = append(out, v)
	}
	if err := cur.Err(); err != nil {
		return nil, "", qb.wrap("select", err)
	}
	if len(out) == 0 {
		return out, qb.cursor, nil
	}
	next, err := pg.cursor()
	if err != nil {
		return nil, "", qb.wrap("select", err)
	}
	return out, next, nil
}

// Get returns the first matching document, or [ErrNotFound].
func (qb *QueryBuilder[T]) Get(ctx context.Context) (*T, error) {
	p, err := qb.plan("get", false)
	if err != nil {
		return nil, err
	}
	// FindOne is Find with a limit of one; going through findOptions keeps
	// Get's sort, skip and projection identical to Select's.
	cur, err := qb.c().Find(ctx, p.filter, qb.findOptions(p).SetLimit(1))
	if err != nil {
		return nil, qb.wrap("get", err)
	}
	defer func() { _ = cur.Close(ctx) }()
	if !cur.Next(ctx) {
		if err := cur.Err(); err != nil {
			return nil, qb.wrap("get", err)
		}
		return nil, qb.wrap("get", ErrNotFound)
	}
	var v T
	if err := cur.Decode(&v); err != nil {
		return nil, qb.wrap("get", err)
	}
	return &v, nil
}

// Count returns how many documents match, honouring offset and limit. The
// server counts; no documents are loaded.
func (qb *QueryBuilder[T]) Count(ctx context.Context) (int64, error) {
	p, err := qb.plan("count", false)
	if err != nil {
		return 0, err
	}
	opts := options.Count()
	if qb.offset > 0 {
		opts.SetSkip(qb.offset)
	}
	if qb.limit > 0 {
		opts.SetLimit(qb.limit)
	}
	n, err := qb.c().CountDocuments(ctx, p.filter, opts)
	if err != nil {
		return 0, qb.wrap("count", err)
	}
	return n, nil
}

// unpaged rejects a builder carrying a limit, offset or cursor, for the
// terminals that cannot honour one. Ignoring it would quietly act on more
// documents than were asked for.
func (qb *QueryBuilder[T]) unpaged(op string) error {
	if qb.err == nil && (qb.limit > 0 || qb.offset > 0 || qb.cursor != "") {
		return qb.wrap(op, errors.New("limit, offset and cursor do not apply"))
	}
	return nil
}

// Delete removes every matching document and returns how many were removed.
// Without filters it empties the collection.
//
// A delete cannot be limited or paginated, so a builder carrying a limit,
// offset or cursor is rejected rather than deleting more than was asked for.
func (qb *QueryBuilder[T]) Delete(ctx context.Context) (int64, error) {
	if err := qb.unpaged("delete"); err != nil {
		return 0, err
	}
	p, err := qb.plan("delete", false)
	if err != nil {
		return 0, err
	}
	res, err := qb.c().DeleteMany(ctx, p.filter)
	if err != nil {
		return 0, qb.wrap("delete", err)
	}
	return res.DeletedCount, nil
}

// SelectIDs returns the _id of every matching document, decoded as K, without
// loading the documents.
//
//	ids, err := mgx.SelectIDs[string](ctx, mgx.Query[User](db, "users").WithFilter(...))
func SelectIDs[K, T any](ctx context.Context, qb *QueryBuilder[T]) ([]K, error) {
	p, err := qb.plan("select-ids", false)
	if err != nil {
		return nil, err
	}
	opts := qb.findOptions(p).SetProjection(bson.D{{Key: FieldID, Value: 1}})
	cur, err := qb.c().Find(ctx, p.filter, opts)
	if err != nil {
		return nil, qb.wrap("select-ids", err)
	}
	var rows []struct {
		ID K `bson:"_id"`
	}
	if err := cur.All(ctx, &rows); err != nil {
		return nil, qb.wrap("select-ids", err)
	}
	ids := make([]K, len(rows))
	for i := range rows {
		ids[i] = rows[i].ID
	}
	return ids, nil
}

// Distinct returns the distinct values of one field among the matching
// documents, decoded as V. Ordering and projection are ignored; a builder
// carrying a limit, offset or cursor is rejected, as the server cannot apply
// one to a distinct.
func Distinct[V, T any](ctx context.Context, qb *QueryBuilder[T], field string) ([]V, error) {
	if err := qb.unpaged("distinct"); err != nil {
		return nil, err
	}
	p, err := qb.plan("distinct", false)
	if err != nil {
		return nil, err
	}
	if err := checkField("distinct", field); err != nil {
		return nil, qb.wrap("distinct", err)
	}
	res := qb.c().Distinct(ctx, field, p.filter)
	// Decode does not report a failed command (true of driver v2.9.2 as
	// well), so the command's own error has to be checked first.
	if err := res.Err(); err != nil {
		return nil, qb.wrap("distinct", err)
	}
	out := []V{}
	if err := res.Decode(&out); err != nil {
		return nil, qb.wrap("distinct", err)
	}
	return out, nil
}

// checkID checks an id as it is encoded for the server, so anything that
// encodes as null is caught, however it got there: a nil pointer, map or
// slice at any depth, or a type the client's registry writes as null. Such an
// id would address the one document whose _id is null.
func (db *DB) checkID(id any) error {
	if id == nil {
		return errors.New("id is nil")
	}
	raw, err := db.marshal(bson.D{{Key: FieldID, Value: id}})
	if err != nil {
		return err
	}
	v, err := raw.LookupErr(FieldID)
	if err != nil {
		return err
	}
	return checkRawID(v)
}

// checkRawID checks an encoded id.
func checkRawID(v bson.RawValue) error {
	switch v.Type {
	case bson.TypeNull, bson.TypeUndefined:
		return errors.New("id is null")
	case bson.TypeString:
		if s, ok := v.StringValueOK(); !ok || s == "" {
			return errors.New("id is empty") // Firestore rejects an empty-string _id
		}
	}
	return nil
}

// encodeIDs encodes ids as a $in sends them and checks each.
func encodeIDs[K any](db *DB, ids []K) ([]bson.RawValue, error) {
	raw, err := db.marshal(bson.D{{Key: "ids", Value: ids}})
	if err != nil {
		return nil, err
	}
	encoded, err := raw.Lookup("ids").Array().Values()
	if err != nil {
		return nil, err
	}
	for i, v := range encoded {
		if err := checkRawID(v); err != nil {
			return nil, fmt.Errorf("id %d: %w", i, err)
		}
	}
	return encoded, nil
}

func idFilter(id any) bson.D {
	return bson.D{{Key: FieldID, Value: bson.D{{Key: "$eq", Value: id}}}}
}

func idsFilter[K any](ids []K) bson.D {
	return bson.D{{Key: FieldID, Value: bson.D{{Key: "$in", Value: ids}}}}
}

// withID encodes data and forces its _id to id, so the stored document
// carries the id it was addressed by whatever data's own _id field holds.
func (db *DB) withID(id any, data any) (bson.D, error) {
	raw, err := db.marshal(data)
	if err != nil {
		return nil, err
	}
	elems, err := raw.Elements()
	if err != nil {
		return nil, err
	}
	doc := make(bson.D, 0, len(elems)+1)
	doc = append(doc, bson.E{Key: FieldID, Value: id})
	for _, e := range elems {
		if e.Key() != FieldID {
			doc = append(doc, bson.E{Key: e.Key(), Value: e.Value()})
		}
	}
	return doc, nil
}

// writable rejects a builder carrying anything a write by id cannot honour.
// Dropping a filter silently could overwrite a document the caller meant to
// scope out, such as another tenant's.
func (qb *QueryBuilder[T]) writable(op string) error {
	if qb.err != nil {
		return qb.wrap(op, qb.err)
	}
	if len(qb.filters) > 0 || len(qb.order) > 0 || len(qb.projection) > 0 || qb.limit > 0 || qb.offset > 0 || qb.cursor != "" {
		return qb.wrap(op, errors.New("filters, ordering, projection and pagination do not apply to a write by id"))
	}
	return nil
}

// insertDoc encodes data for an insert and checks the _id it carries, if any;
// without one the driver generates an ObjectID.
func (db *DB) insertDoc(data any) (bson.Raw, error) {
	raw, err := db.marshal(data)
	if err != nil {
		return nil, err
	}
	if id, err := raw.LookupErr(FieldID); err == nil {
		if err := checkRawID(id); err != nil {
			return nil, err
		}
	}
	return raw, nil
}

// Upsert replaces the document with the given id, or inserts it. The id
// argument wins over any _id held in data.
func (qb *QueryBuilder[T]) Upsert(ctx context.Context, id any, data *T) error {
	if err := qb.writable("upsert"); err != nil {
		return err
	}
	if err := qb.db.checkID(id); err != nil {
		return qb.wrap("upsert", err)
	}
	if data == nil {
		return qb.wrap("upsert", errors.New("document is nil"))
	}
	doc, err := qb.db.withID(id, data)
	if err != nil {
		return qb.wrap("upsert", err)
	}
	if _, err := qb.c().ReplaceOne(ctx, idFilter(id), doc, options.Replace().SetUpsert(true)); err != nil {
		return qb.wrap("upsert", err)
	}
	return nil
}

// UpsertMulti upserts documents keyed by string id, in requests of 500.
//
// Ids are written in sorted order, so batch boundaries are stable across runs
// and a failure names the batch it happened in. Each request is independent
// and unordered: a failure part-way through leaves the batches before it
// applied, and may leave some documents of the failing batch applied too. For other
// id types, or for all-or-nothing, loop over Upsert inside [RunInTransaction].
func (qb *QueryBuilder[T]) UpsertMulti(ctx context.Context, items map[string]*T) error {
	if err := qb.writable("upsert-multi"); err != nil {
		return err
	}
	ids := make([]string, 0, len(items))
	for id, data := range items {
		if id == "" || data == nil {
			return qb.wrap("upsert-multi", fmt.Errorf("id %q is empty or has a nil document", id))
		}
		ids = append(ids, id)
	}
	slices.Sort(ids)

	for start := 0; start < len(ids); start += maxWriteBatch {
		end := min(start+maxWriteBatch, len(ids))
		models := make([]mongo.WriteModel, 0, end-start)
		for _, id := range ids[start:end] {
			doc, err := qb.db.withID(id, items[id])
			if err != nil {
				return qb.wrap("upsert-multi", err)
			}
			models = append(models, mongo.NewReplaceOneModel().
				SetFilter(idFilter(id)).SetReplacement(doc).SetUpsert(true))
		}
		// Collection.BulkWrite sends plain update commands. Client.BulkWrite
		// uses the bulkWrite command, which Firestore does not list as supported.
		if _, err := qb.c().BulkWrite(ctx, models, options.BulkWrite().SetOrdered(false)); err != nil {
			return qb.wrap("upsert-multi", fmt.Errorf("batch [%s..%s]: %w", ids[start], ids[end-1], err))
		}
	}
	return nil
}

// Insert adds a new document and returns its _id. When data carries no _id
// the driver generates an ObjectID. Inserting an existing _id fails; test
// that with mongo.IsDuplicateKeyError.
func (qb *QueryBuilder[T]) Insert(ctx context.Context, data *T) (any, error) {
	if err := qb.writable("insert"); err != nil {
		return nil, err
	}
	if data == nil {
		return nil, qb.wrap("insert", errors.New("document is nil"))
	}
	doc, err := qb.db.insertDoc(data)
	if err != nil {
		return nil, qb.wrap("insert", err)
	}
	res, err := qb.c().InsertOne(ctx, doc)
	if err != nil {
		return nil, qb.wrap("insert", err)
	}
	return res.InsertedID, nil
}

// InsertMulti adds new documents in requests of 500 and returns their ids in
// input order. If a request fails, the ids known to be inserted are returned
// alongside the error: those documents exist. When the failure is a write
// error, such as a duplicate key, that includes the failed request's
// documents before the one rejected. Otherwise, such as on a timeout or a
// dropped connection, the failed request may or may not have been applied
// and none of its ids are returned. A document whose _id is null or empty is
// caught as its request is built, so the requests before it stay applied.
func (qb *QueryBuilder[T]) InsertMulti(ctx context.Context, items []*T) ([]any, error) {
	if err := qb.writable("insert-multi"); err != nil {
		return nil, err
	}
	for i, it := range items {
		if it == nil {
			return nil, qb.wrap("insert-multi", fmt.Errorf("document %d is nil", i))
		}
	}
	ids := make([]any, 0, len(items))
	docs := make([]bson.Raw, 0, min(maxWriteBatch, len(items)))
	for start := 0; start < len(items); start += maxWriteBatch {
		end := min(start+maxWriteBatch, len(items))
		docs = docs[:0]
		for i, it := range items[start:end] {
			doc, err := qb.db.insertDoc(it)
			if err != nil {
				return ids, qb.wrap("insert-multi", fmt.Errorf("document %d: %w", start+i, err))
			}
			docs = append(docs, doc)
		}
		res, err := qb.c().InsertMany(ctx, docs)
		if res != nil {
			ids = append(ids, res.InsertedIDs...)
		}
		if err != nil {
			return ids, qb.wrap("insert-multi", fmt.Errorf("batch [%d:%d]: %w", start, end, err))
		}
	}
	return ids, nil
}

// GetByID returns the document with the given id, or [ErrNotFound].
func GetByID[T any](ctx context.Context, db *DB, collection string, id any) (*T, error) {
	if err := db.checkID(id); err != nil {
		return nil, fmt.Errorf("mgx: get %s: %w", collection, err)
	}
	var v T
	err := db.Collection(collection).FindOne(ctx, idFilter(id)).Decode(&v)
	if errors.Is(err, mongo.ErrNoDocuments) {
		return nil, fmt.Errorf("mgx: get %s: %w", collection, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("mgx: get %s: %w", collection, err)
	}
	return &v, nil
}

// idKey reduces an _id to a comparable form. Numbers compare by value in a
// query whatever their BSON width, so 1 as int32, int64 and double share a
// key here too; otherwise a result could not be matched to the id that asked
// for it. A whole double in int64 range converts exactly, so it shares the
// key of the int64 with the same value. The pager uses it to tell whether two
// sort values tie.
func idKey(v bson.RawValue) string {
	switch v.Type {
	case bson.TypeInt32:
		return "n" + strconv.FormatInt(int64(v.Int32()), 10)
	case bson.TypeInt64:
		return "n" + strconv.FormatInt(v.Int64(), 10)
	case bson.TypeDouble:
		if f := v.Double(); f == math.Trunc(f) && f >= math.MinInt64 && f < math.MaxInt64 {
			return "n" + strconv.FormatInt(int64(f), 10)
		}
	}
	return string([]byte{byte(v.Type)}) + string(v.Value)
}

// GetMulti returns the documents with the given ids, in the order of ids,
// reading 1000 per request. A missing document is left zero-valued, as in
// dsx; use [GetByID] to tell a missing document from an empty one.
func GetMulti[T, K any](ctx context.Context, db *DB, collection string, ids []K) ([]T, error) {
	out := make([]T, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	// Encoded once, as the $in sends them, to key each result to its slots.
	encoded, err := encodeIDs(db, ids)
	if err != nil {
		return nil, fmt.Errorf("mgx: get-multi %s: %w", collection, err)
	}
	// An id may be asked for twice; it is fetched once and fills every slot.
	index := make(map[string][]int, len(ids))
	distinct := make([]K, 0, len(ids))
	for i, v := range encoded {
		k := idKey(v)
		if _, seen := index[k]; !seen {
			distinct = append(distinct, ids[i])
		}
		index[k] = append(index[k], i)
	}
	coll := db.Collection(collection)
	for start := 0; start < len(distinct); start += maxLookupBatch {
		end := min(start+maxLookupBatch, len(distinct))
		if err := getBatch(ctx, coll, distinct[start:end], index, out); err != nil {
			return nil, fmt.Errorf("mgx: get-multi %s: distinct ids [%d:%d]: %w", collection, start, end, err)
		}
	}
	return out, nil
}

// getBatch reads the documents with the given ids into out, at the positions
// index records for each id.
func getBatch[T, K any](ctx context.Context, coll *mongo.Collection, ids []K, index map[string][]int, out []T) error {
	cur, err := coll.Find(ctx, idsFilter(ids))
	if err != nil {
		return err
	}
	defer func() { _ = cur.Close(ctx) }()
	for cur.Next(ctx) {
		// Decoded per slot, so an id asked for twice shares no maps or slices.
		for _, i := range index[idKey(cur.Current.Lookup(FieldID))] {
			if err := cur.Decode(&out[i]); err != nil {
				return err
			}
		}
	}
	return cur.Err()
}

// DeleteByID removes the document with the given id. Removing a document
// that does not exist is not an error.
func DeleteByID(ctx context.Context, db *DB, collection string, id any) error {
	if err := db.checkID(id); err != nil {
		return fmt.Errorf("mgx: delete %s: %w", collection, err)
	}
	if _, err := db.Collection(collection).DeleteOne(ctx, idFilter(id)); err != nil {
		return fmt.Errorf("mgx: delete %s: %w", collection, err)
	}
	return nil
}

// DeleteMultiByID removes the documents with the given ids, 500 per request,
// and returns how many were removed.
func DeleteMultiByID[K any](ctx context.Context, db *DB, collection string, ids []K) (int64, error) {
	if _, err := encodeIDs(db, ids); err != nil {
		return 0, fmt.Errorf("mgx: delete-multi %s: %w", collection, err)
	}
	var n int64
	coll := db.Collection(collection)
	for start := 0; start < len(ids); start += maxWriteBatch {
		end := min(start+maxWriteBatch, len(ids))
		res, err := coll.DeleteMany(ctx, idsFilter(ids[start:end]))
		if err != nil {
			return n, fmt.Errorf("mgx: delete-multi %s [%d:%d]: %w", collection, start, end, err)
		}
		n += res.DeletedCount
	}
	return n, nil
}

// RunInTransaction runs fn in a transaction and commits if it returns nil.
//
// The transaction travels in the context handed to fn. Every mgx or driver
// call made with that context joins the transaction; a call made with the
// outer context does not.
//
// Called with a context already inside a transaction on the same client, it
// runs fn in that transaction rather than starting an independent one whose
// writes would commit even if the outer one aborts.
//
// The driver re-runs fn when the server reports a transient conflict, so fn
// must be safe to run more than once. Firestore uses optimistic concurrency
// by default and ends a transaction after 270 seconds, or 60 seconds idle.
func RunInTransaction(ctx context.Context, db *DB, fn func(ctx context.Context) error) error {
	if s := mongo.SessionFromContext(ctx); s != nil && s.Client() == db.client && s.TransactionRunning() {
		return fn(ctx)
	}
	sess, err := db.client.StartSession()
	if err != nil {
		return fmt.Errorf("mgx: transaction: %w", err)
	}
	defer sess.EndSession(ctx)
	if _, err := sess.WithTransaction(ctx, func(ctx context.Context) (any, error) {
		return nil, fn(ctx)
	}); err != nil {
		return fmt.Errorf("mgx: transaction: %w", err)
	}
	return nil
}
