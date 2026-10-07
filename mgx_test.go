package mgx

import (
	"context"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/event"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type user struct {
	ID     string `bson:"_id,omitempty"`
	Name   string `bson:"name"`
	Status string `bson:"status"`
	Age    int    `bson:"age"`
	Bio    string `bson:"bio,omitempty"`
}

// ---- unit tests: no server needed ----

func offline(t *testing.T) *DB {
	t.Helper()
	db, err := Connect("mongodb://127.0.0.1:1/?retryWrites=true", "unit")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	return db
}

func TestBuilderRejectsBadInput(t *testing.T) {
	db := offline(t)
	cases := map[string]*QueryBuilder[user]{
		"empty collection": Query[user](db, ""),
		"empty field":      Query[user](db, "u").WithFilter("", OpEqual, 1),
		"unknown operator": Query[user](db, "u").WithFilter("a", FilterOperator("$where"), 1),
		"in needs slice":   Query[user](db, "u").WithFilter("a", OpIn, "x"),
		"in empty slice":   Query[user](db, "u").WithFilter("a", OpIn, []string{}),
		"in bytes":         Query[user](db, "u").WithFilter("a", OpIn, []byte("ab")),
		"empty order":      Query[user](db, "u").WithOrder(""),
		"order twice":      Query[user](db, "u").WithOrder("a").WithOrderDesc("a"),
		"negative limit":   Query[user](db, "u").WithLimit(-1),
		"negative offset":  Query[user](db, "u").WithOffset(-1),
		"empty projection": Query[user](db, "u").WithProject("a", ""),
		"empty raw filter": Query[user](db, "u").WithRawFilter(nil),
		"operator field":   Query[user](db, "u").WithFilter("$expr", OpEqual, bson.A{"$a", "$b"}),
		"operator order":   Query[user](db, "u").WithOrder("$natural"),
		"operator project": Query[user](db, "u").WithProject("$a"),
	}
	// A dropped build error would reach for the unreachable server; fail fast.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for name, qb := range cases {
		if qb.Err() == nil {
			t.Errorf("%s: expected a build error", name)
		}
		_, err := qb.Select(ctx)
		if !errors.Is(err, qb.Err()) || !strings.HasPrefix(err.Error(), "mgx: select ") {
			t.Errorf("%s: terminal call returned %v, want the build error wrapped with the op", name, err)
		}
	}
}

func TestFilterShape(t *testing.T) {
	db := offline(t)
	p, err := Query[user](db, "u").
		WithFilter("age", OpGreaterEqual, 18).
		WithFilter("age", OpLess, 65).
		WithOrderDesc("age").
		plan("select", false)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := bson.MarshalExtJSON(p.filter, false, false)
	want := `{"$and":[{"age":{"$gte":18}},{"age":{"$lt":65}}]}`
	if string(got) != want {
		t.Errorf("filter = %s, want %s", got, want)
	}
	if s, _ := bson.MarshalExtJSON(p.sort, false, false); string(s) != `{"age":-1}` {
		t.Errorf("sort = %s", s)
	}

	// A value that looks like an operator stays an operand.
	p, _ = Query[user](db, "u").WithFilter("name", OpEqual, bson.D{{Key: "$ne", Value: nil}}).plan("select", false)
	got, _ = bson.MarshalExtJSON(p.filter, false, false)
	if string(got) != `{"name":{"$eq":{"$ne":null}}}` {
		t.Errorf("operator injection not neutralised: %s", got)
	}
}

func TestPaginationConflict(t *testing.T) {
	db := offline(t)
	ctx := context.Background()
	if _, _, err := Query[user](db, "u").WithOffset(5).SelectWithCursor(ctx); !errors.Is(err, ErrPaginationConflict) {
		t.Errorf("offset + SelectWithCursor: %v", err)
	}
	if err := Query[user](db, "u").WithOffset(5).WithCursor("abc").Err(); !errors.Is(err, ErrPaginationConflict) {
		t.Errorf("offset + cursor: %v", err)
	}
	if err := Query[user](db, "u").WithCursor("abc").WithOffset(5).Err(); !errors.Is(err, ErrPaginationConflict) {
		t.Errorf("cursor + offset: %v", err)
	}
	if err := Query[user](db, "u").WithOffset(5).WithCursor("").Err(); err != nil {
		t.Errorf("an empty cursor is not cursor pagination: %v", err)
	}
	if err := Query[user](db, "u").WithCursor("abc").WithOffset(0).Err(); err != nil {
		t.Errorf("a zero offset is not offset pagination: %v", err)
	}
	if qb := Query[user](db, "u").WithCursor("abc").WithCursor(""); qb.cursor != "" || qb.WithOffset(5).Err() != nil {
		t.Errorf("an empty cursor should restart at the first page: cursor %q, %v", qb.cursor, qb.Err())
	}
}

func TestProjectionKeepsSortFields(t *testing.T) {
	db := offline(t)
	keys := []sortKey{{field: "meta.age"}, {field: "rank"}, {field: FieldID}}
	cases := map[string]struct {
		project []string
		want    string
	}{
		"sort fields appended": {[]string{"name"}, `{"name":1,"meta.age":1,"rank":1,"_id":1}`},
		"parent covers key":    {[]string{"meta"}, `{"meta":1,"rank":1,"_id":1}`},
		"key covers child":     {[]string{"meta.age.x", "rank"}, `{"rank":1,"meta.age":1,"_id":1}`},
		"duplicates collapse":  {[]string{"rank", "rank", FieldID}, `{"rank":1,"_id":1,"meta.age":1}`},
	}
	for name, c := range cases {
		got, _ := bson.MarshalExtJSON(Query[user](db, "u").WithProject(c.project...).projectionDoc(keys), false, false)
		if string(got) != c.want {
			t.Errorf("%s: projection = %s, want %s", name, got, c.want)
		}
	}
	if d := Query[user](db, "u").projectionDoc(keys); d != nil {
		t.Errorf("no projection asked for, got %v", d)
	}
}

func TestCursorRoundTrip(t *testing.T) {
	keys := []sortKey{{field: "status"}, {field: "meta.age", desc: true}, {field: FieldID}}
	last, _ := bson.Marshal(bson.D{
		{Key: "_id", Value: "u7"},
		{Key: "status", Value: "active"},
		{Key: "meta", Value: bson.D{{Key: "age", Value: int32(30)}}},
	})
	tok, err := encodeCursor(keys, last)
	if err != nil {
		t.Fatal(err)
	}
	f, err := afterFilter(keys, tok)
	if err != nil {
		t.Fatal(err)
	}
	got, _ := bson.MarshalExtJSON(f, false, false)
	want := `{"$or":[` +
		`{"status":{"$gt":"active"}},` +
		`{"status":{"$eq":"active"},"meta.age":{"$lt":30}},` +
		`{"status":{"$eq":"active"},"meta.age":{"$eq":30},"_id":{"$gt":"u7"}}]}`
	if string(got) != want {
		t.Errorf("after filter =\n %s\nwant\n %s", got, want)
	}

	// A cursor is tied to the ordering it was issued for.
	if _, err := afterFilter(keys[1:], tok); !errors.Is(err, ErrInvalidCursor) {
		t.Errorf("cursor reused with another order: %v", err)
	}
	if _, err := afterFilter(keys, "not base64!"); !errors.Is(err, ErrInvalidCursor) {
		t.Errorf("garbage cursor: %v", err)
	}
	if _, err := afterFilter(keys, "AAAA"); !errors.Is(err, ErrInvalidCursor) {
		t.Errorf("non-bson cursor: %v", err)
	}

	// A missing or null sort value cannot be paged past, so it is an error.
	noStatus, _ := bson.Marshal(bson.D{{Key: "_id", Value: "u1"}, {Key: "meta", Value: bson.D{{Key: "age", Value: 1}}}})
	if _, err := encodeCursor(keys, noStatus); err == nil {
		t.Error("missing sort field: expected an error")
	}
	nullStatus, _ := bson.Marshal(bson.D{{Key: "_id", Value: "u1"}, {Key: "status", Value: nil}, {Key: "meta", Value: bson.D{{Key: "age", Value: 1}}}})
	if _, err := encodeCursor(keys, nullStatus); err == nil {
		t.Error("null sort field: expected an error")
	}
	arrayStatus, _ := bson.Marshal(bson.D{{Key: "_id", Value: "u1"}, {Key: "status", Value: bson.A{"a", "b"}}, {Key: "meta", Value: bson.D{{Key: "age", Value: 1}}}})
	if _, err := encodeCursor(keys, arrayStatus); err == nil {
		t.Error("array sort field: expected an error")
	}
}

func TestWithIDOverridesDocumentID(t *testing.T) {
	doc, err := offline(t).withID("key-1", &user{ID: "other", Name: "A"})
	if err != nil {
		t.Fatal(err)
	}
	got, _ := bson.MarshalExtJSON(doc, false, false)
	if !strings.HasPrefix(string(got), `{"_id":"key-1","name":"A"`) || strings.Contains(string(got), "other") {
		t.Errorf("doc = %s", got)
	}
}

func TestIDKeyNumericWidths(t *testing.T) {
	key := func(v any) string {
		ty, b, err := bson.MarshalValue(v)
		if err != nil {
			t.Fatal(err)
		}
		return idKey(bson.RawValue{Type: ty, Value: b})
	}
	if key(int32(7)) != key(int64(7)) || key(int64(7)) != key(float64(7)) {
		t.Error("7 as int32, int64 and double should share a key")
	}
	if key(int32(7)) == key("7") || key(float64(7.5)) == key(int32(7)) {
		t.Error("distinct ids collided")
	}
	if key(int64(1)<<60) != key(float64(1<<60)) {
		t.Error("a whole double above 2^53 should share the key of the equal int64")
	}
	if key(int64(1)<<60+1) == key(float64(1<<60)) {
		t.Error("an int64 a double cannot represent collided with its neighbour")
	}
}

func TestCheckIDRejectsNil(t *testing.T) {
	var s *string
	var m map[string]any
	var b []byte
	type userID string
	for name, id := range map[string]any{
		"nil": nil, "nil pointer": s, "nil map": m, "nil slice": b,
		"empty string": "", "empty named string": userID(""), "pointer to empty string": new(""),
	} {
		if checkID(id) == nil {
			t.Errorf("%s id accepted", name)
		}
	}
	if err := checkID(new("x")); err != nil {
		t.Errorf("non-nil pointer id rejected: %v", err)
	}

	// The bulk paths apply the same check, before any request is sent.
	db := offline(t)
	ctx := context.Background()
	if _, err := GetMulti[user](ctx, db, "u", []*string{new("a"), nil}); err == nil {
		t.Error("get-multi accepted a nil id")
	}
	if _, err := DeleteMultiByID(ctx, db, "u", []string{"a", ""}); err == nil {
		t.Error("delete-multi accepted an empty id")
	}
}

// Documents mgx encodes itself must follow the client's BSON options, as the
// ones the driver encodes do.
func TestMarshalUsesClientOptions(t *testing.T) {
	type tagged struct {
		Name string `json:"full_name"`
	}
	db, err := Connect("mongodb://127.0.0.1:1/", "unit",
		WithClientOptions(options.Client().SetBSONOptions(&options.BSONOptions{UseJSONStructTags: true})))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	doc, err := db.withID("k", &tagged{Name: "A"})
	if err != nil {
		t.Fatal(err)
	}
	if got, _ := bson.MarshalExtJSON(doc, false, false); string(got) != `{"_id":"k","full_name":"A"}` {
		t.Errorf("doc = %s", got)
	}
}

func TestUnpagedTerminalsRejectPagination(t *testing.T) {
	db := offline(t)
	ctx := context.Background()
	for name, qb := range map[string]*QueryBuilder[user]{
		"limit":  Query[user](db, "u").WithLimit(1),
		"offset": Query[user](db, "u").WithOffset(1),
		"cursor": Query[user](db, "u").WithCursor("abc"),
	} {
		if _, err := qb.Delete(ctx); err == nil {
			t.Errorf("delete with a %s should be rejected", name)
		}
		if _, err := Distinct[string](ctx, qb, "status"); err == nil {
			t.Errorf("distinct with a %s should be rejected", name)
		}
	}
	if _, err := Distinct[string](ctx, Query[user](db, "u"), "$where"); err == nil {
		t.Error("distinct on an operator field should be rejected")
	}
}

// A write by id cannot honour query settings, so it must refuse them: a
// dropped filter could overwrite a document the caller meant to scope out.
func TestWritesRejectQuerySettings(t *testing.T) {
	db := offline(t)
	ctx := context.Background()
	for name, qb := range map[string]*QueryBuilder[user]{
		"filter":     Query[user](db, "u").WithFilter("tenant", OpEqual, "t1"),
		"order":      Query[user](db, "u").WithOrder("name"),
		"projection": Query[user](db, "u").WithProject("name"),
		"limit":      Query[user](db, "u").WithLimit(1),
	} {
		if err := qb.Upsert(ctx, "u1", &user{}); err == nil {
			t.Errorf("upsert with a %s should be rejected", name)
		}
		if err := qb.UpsertMulti(ctx, map[string]*user{"u1": {}}); err == nil {
			t.Errorf("upsert-multi with a %s should be rejected", name)
		}
		if _, err := qb.Insert(ctx, &user{}); err == nil {
			t.Errorf("insert with a %s should be rejected", name)
		}
		if _, err := qb.InsertMulti(ctx, []*user{{}}); err == nil {
			t.Errorf("insert-multi with a %s should be rejected", name)
		}
	}
}

func TestConnect(t *testing.T) {
	// Connect is lazy, so none of this needs a server.
	if _, err := Connect("mongodb://127.0.0.1:1/", ""); err == nil {
		t.Error("empty database name should be rejected")
	}
	// The Firestore workload-identity URI must be accepted by the driver.
	uri := "mongodb://uid.asia-southeast2.firestore.goog:443/db?loadBalanced=true&tls=true&retryWrites=false" +
		"&authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE"
	db, err := Connect(uri, "db")
	if err != nil {
		t.Fatalf("firestore OIDC uri rejected: %v", err)
	}
	_ = db.Close(context.Background())
}

// ---- integration tests: set MGX_TEST_URI to run ----

func testDB() string {
	if name := os.Getenv("MGX_TEST_DB"); name != "" {
		return name
	}
	return "mgx_test"
}

func live(t *testing.T) (*DB, context.Context) {
	t.Helper()
	uri := os.Getenv("MGX_TEST_URI")
	if uri == "" {
		t.Skip("MGX_TEST_URI not set")
	}
	db, err := Connect(uri, testDB())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close(context.Background()) })
	return db, context.Background()
}

func fresh(t *testing.T, db *DB, ctx context.Context) string {
	t.Helper()
	coll := "c_" + strings.ToLower(strings.NewReplacer("/", "_").Replace(t.Name()))
	if _, err := Query[user](db, coll).Delete(ctx); err != nil {
		t.Fatal(err)
	}
	return coll
}

func TestLiveCRUD(t *testing.T) {
	db, ctx := live(t)
	coll := fresh(t, db, ctx)
	q := func() *QueryBuilder[user] { return Query[user](db, coll) }

	if _, err := GetByID[user](ctx, db, coll, "u1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing doc: %v", err)
	}
	if err := q().Upsert(ctx, "u1", &user{ID: "ignored", Name: "Ana", Status: "active", Age: 30}); err != nil {
		t.Fatal(err)
	}
	if err := q().Upsert(ctx, "u1", &user{Name: "Ana B", Status: "active", Age: 31}); err != nil {
		t.Fatal(err)
	}
	got, err := GetByID[user](ctx, db, coll, "u1")
	if err != nil {
		t.Fatal(err)
	}
	if want := (user{ID: "u1", Name: "Ana B", Status: "active", Age: 31}); *got != want {
		t.Errorf("got %+v, want %+v", *got, want)
	}
	if n, _ := q().Count(ctx); n != 1 {
		t.Errorf("upsert twice left %d docs", n)
	}
	if err := q().Upsert(ctx, "", &user{}); err == nil {
		t.Error("empty id accepted")
	}

	id, err := Query[bson.M](db, coll).Insert(ctx, &bson.M{"name": "Auto", "status": "pending", "age": 5})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := id.(bson.ObjectID); !ok {
		t.Errorf("auto id is %T, want ObjectID", id)
	}
	if _, err := q().Insert(ctx, &user{ID: "u1", Name: "dup"}); err == nil {
		t.Error("inserting an existing _id should fail")
	}

	one, err := Query[bson.M](db, coll).WithFilter("status", OpEqual, "pending").Get(ctx)
	if err != nil || (*one)["name"] != "Auto" || (*one)["_id"] != id {
		t.Errorf("Get = %+v, %v", one, err)
	}
	if _, err := q().WithFilter("status", OpEqual, "nope").Get(ctx); !errors.Is(err, ErrNotFound) {
		t.Errorf("Get on no match: %v", err)
	}

	if err := DeleteByID(ctx, db, coll, "u1"); err != nil {
		t.Fatal(err)
	}
	if err := DeleteByID(ctx, db, coll, "u1"); err != nil {
		t.Errorf("deleting a missing doc: %v", err)
	}
	if err := DeleteByID(ctx, db, coll, (*string)(nil)); err == nil {
		t.Error("nil pointer id accepted")
	}
	if n, _ := q().Count(ctx); n != 1 {
		t.Errorf("%d docs left, want 1", n)
	}
}

func seed(t *testing.T, db *DB, ctx context.Context, coll string, n int) {
	t.Helper()
	items := make(map[string]*user, n)
	for i := range n {
		status := []string{"active", "inactive", "pending"}[i%3]
		items[fmt.Sprintf("u%04d", i)] = &user{Name: fmt.Sprintf("n%d", i), Status: status, Age: 20 + i%7}
	}
	if err := Query[user](db, coll).UpsertMulti(ctx, items); err != nil {
		t.Fatal(err)
	}
}

func TestLiveQueries(t *testing.T) {
	db, ctx := live(t)
	coll := fresh(t, db, ctx)
	seed(t, db, ctx, coll, 1203) // crosses two write-batch boundaries
	q := func() *QueryBuilder[user] { return Query[user](db, coll) }

	if n, err := q().Count(ctx); err != nil || n != 1203 {
		t.Fatalf("count = %d, %v", n, err)
	}
	if n, _ := q().WithFilter("status", OpEqual, "active").Count(ctx); n != 401 {
		t.Errorf("active = %d, want 401", n)
	}
	if n, _ := q().WithFilter("status", OpIn, []string{"active", "pending"}).Count(ctx); n != 802 {
		t.Errorf("in = %d, want 802", n)
	}
	if n, _ := q().WithFilter("status", OpNotIn, []string{"active", "pending"}).Count(ctx); n != 401 {
		t.Errorf("not-in = %d, want 401", n)
	}
	if n, _ := q().WithFilter("age", OpGreaterEqual, 22).WithFilter("age", OpLess, 24).Count(ctx); n != 344 {
		t.Errorf("range = %d, want 344", n)
	}
	if n, _ := q().WithRawFilter(bson.D{{Key: "bio", Value: bson.D{{Key: "$exists", Value: false}}}}).Count(ctx); n != 1203 {
		t.Errorf("raw filter = %d", n)
	}

	page, err := q().WithOrderDesc("age").WithOrder(FieldID).WithOffset(2).WithLimit(3).Select(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(page) != 3 || page[0].Age != 26 || page[0].ID != "u0020" {
		t.Errorf("offset page = %+v", page)
	}

	proj, err := q().WithProject("name").WithFilter(FieldID, OpEqual, "u0001").Select(ctx)
	if err != nil || len(proj) != 1 || proj[0].Name != "n1" || proj[0].Status != "" || proj[0].ID != "u0001" {
		t.Errorf("projection = %+v, %v", proj, err)
	}

	ids, err := SelectIDs[string](ctx, q().WithFilter("age", OpEqual, 20).WithOrder(FieldID).WithLimit(2))
	if err != nil || !reflect.DeepEqual(ids, []string{"u0000", "u0007"}) {
		t.Errorf("ids = %v, %v", ids, err)
	}

	statuses, err := Distinct[string](ctx, q().WithFilter("age", OpLess, 23), "status")
	if err != nil || len(statuses) != 3 {
		t.Errorf("distinct = %v, %v", statuses, err)
	}

	many, err := GetMulti[user](ctx, db, coll, []string{"u0003", "missing", "u0001", "u0003"})
	if err != nil {
		t.Fatal(err)
	}
	if len(many) != 4 || many[0].Name != "n3" || many[1] != (user{}) || many[2].Name != "n1" || many[3].Name != "n3" {
		t.Errorf("GetMulti = %+v", many)
	}
	maps, err := GetMulti[bson.M](ctx, db, coll, []string{"u0003", "u0003"})
	if err != nil {
		t.Fatal(err)
	}
	maps[0]["name"] = "changed"
	if maps[1]["name"] != "n3" {
		t.Error("an id asked for twice came back as one shared map")
	}

	if n, err := DeleteMultiByID(ctx, db, coll, []string{"u0000", "u0001", "missing"}); err != nil || n != 2 {
		t.Errorf("delete-multi = %d, %v", n, err)
	}
	if n, err := q().WithFilter("status", OpEqual, "inactive").Delete(ctx); err != nil || n != 400 {
		t.Errorf("delete = %d, %v", n, err)
	}
	if n, _ := q().Count(ctx); n != 801 {
		t.Errorf("left = %d, want 801", n)
	}
}

// Walk a result set page by page under a mixed-direction ordering full of
// ties, and check the pages add up to exactly the unpaged result.
func TestLiveCursorPagination(t *testing.T) {
	db, ctx := live(t)
	coll := fresh(t, db, ctx)
	seed(t, db, ctx, coll, 250)
	q := func() *QueryBuilder[user] {
		return Query[user](db, coll).
			WithFilter("status", OpNotEqual, "inactive").
			WithOrder("status").WithOrderDesc("age")
	}
	want, err := q().WithOrder(FieldID).Select(ctx)
	if err != nil {
		t.Fatal(err)
	}

	var got []user
	cursor, pages := "", 0
	for {
		page, next, err := q().WithLimit(17).WithCursor(cursor).SelectWithCursor(ctx)
		if err != nil {
			t.Fatal(err)
		}
		got = append(got, page...)
		pages++
		if len(page) < 17 {
			break
		}
		cursor = next
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("paged result differs from unpaged: %d vs %d docs", len(got), len(want))
	}
	if len(want) != 167 || pages != 10 {
		t.Errorf("%d docs in %d pages, want 167 in 10", len(want), pages)
	}

	// Past the end: empty page, cursor handed back unchanged.
	page, next, err := q().WithLimit(17).WithCursor(cursor).SelectWithCursor(ctx)
	if err != nil || len(page) != 14 {
		t.Fatalf("last page again = %d, %v", len(page), err)
	}
	page, again, err := q().WithLimit(17).WithCursor(next).SelectWithCursor(ctx)
	if err != nil || len(page) != 0 || again != next {
		t.Errorf("past the end: %d docs, cursor kept = %v, %v", len(page), again == next, err)
	}

	// Projection must not starve the cursor of its sort fields.
	if _, _, err := q().WithProject("name").WithLimit(5).SelectWithCursor(ctx); err != nil {
		t.Errorf("cursor with projection: %v", err)
	}
	// A cursor from one ordering is refused by another.
	if _, _, err := Query[user](db, coll).WithOrder("name").WithCursor(next).SelectWithCursor(ctx); !errors.Is(err, ErrInvalidCursor) {
		t.Errorf("cursor reuse: %v", err)
	}
	// Count honours the cursor.
	if n, err := q().WithCursor(cursor).Count(ctx); err != nil || n != 14 {
		t.Errorf("count after cursor = %d, %v", n, err)
	}
}

// Paging on a nested field while projecting its parent must not send both
// paths: the server rejects that as a path collision.
func TestLiveCursorProjectsParentOfSortKey(t *testing.T) {
	db, ctx := live(t)
	coll := fresh(t, db, ctx)
	type nested struct {
		Meta struct {
			Age int    `bson:"age"`
			Tag string `bson:"tag"`
		} `bson:"meta"`
	}
	q := func() *QueryBuilder[nested] {
		return Query[nested](db, coll).WithProject("meta").WithOrder("meta.age").WithLimit(2)
	}
	for i := range 3 {
		var doc nested
		doc.Meta.Age, doc.Meta.Tag = i, "x"
		if err := Query[nested](db, coll).Upsert(ctx, i, &doc); err != nil {
			t.Fatal(err)
		}
	}
	page, next, err := q().SelectWithCursor(ctx)
	if err != nil || len(page) != 2 || page[1].Meta.Age != 1 || page[1].Meta.Tag != "x" {
		t.Fatalf("first page = %v, %v", page, err)
	}
	if page, _, err = q().WithCursor(next).SelectWithCursor(ctx); err != nil || len(page) != 1 {
		t.Errorf("second page = %v, %v", page, err)
	}
}

func TestLiveInsertMulti(t *testing.T) {
	db, ctx := live(t)
	coll := fresh(t, db, ctx)
	items := make([]*user, 0, 620)
	for i := range 620 {
		items = append(items, &user{ID: fmt.Sprintf("i%04d", i), Name: "x"})
	}
	ids, err := Query[user](db, coll).InsertMulti(ctx, items)
	if err != nil || len(ids) != 620 || ids[0] != "i0000" || ids[619] != "i0619" {
		t.Fatalf("insert-multi: %d ids, %v", len(ids), err)
	}
	// Re-insert with a fresh first batch and a colliding second one: the call
	// fails, and the ids that did go in come back alongside the error.
	for i := range 500 {
		items[i] = &user{ID: fmt.Sprintf("j%04d", i), Name: "y"}
	}
	ids, err = Query[user](db, coll).InsertMulti(ctx, items)
	if err == nil || len(ids) != 500 || ids[499] != "j0499" {
		t.Errorf("partial insert-multi: %d ids, %v", len(ids), err)
	}
	if n, _ := Query[user](db, coll).Count(ctx); n != 1120 {
		t.Errorf("count after partial insert = %d, want 1120", n)
	}
}

func TestLiveTransaction(t *testing.T) {
	db, ctx := live(t)
	if os.Getenv("MGX_TEST_TXN") == "" {
		t.Skip("MGX_TEST_TXN not set (server must support transactions)")
	}
	coll := fresh(t, db, ctx)
	boom := errors.New("boom")
	err := RunInTransaction(ctx, db, func(ctx context.Context) error {
		if err := Query[user](db, coll).Upsert(ctx, "t1", &user{Name: "rolled back"}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if n, _ := Query[user](db, coll).Count(ctx); n != 0 {
		t.Errorf("aborted transaction left %d docs", n)
	}
	err = RunInTransaction(ctx, db, func(ctx context.Context) error {
		return Query[user](db, coll).Upsert(ctx, "t1", &user{Name: "committed"})
	})
	if err != nil {
		t.Fatal(err)
	}
	if n, _ := Query[user](db, coll).Count(ctx); n != 1 {
		t.Errorf("committed transaction left %d docs", n)
	}

	// A nested call joins the outer transaction, so its write rolls back too.
	err = RunInTransaction(ctx, db, func(ctx context.Context) error {
		if err := RunInTransaction(ctx, db, func(ctx context.Context) error {
			return Query[user](db, coll).Upsert(ctx, "t2", &user{Name: "nested"})
		}); err != nil {
			return err
		}
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if _, err := GetByID[user](ctx, db, coll, "t2"); !errors.Is(err, ErrNotFound) {
		t.Errorf("nested write survived the outer abort: %v", err)
	}
}

// A retryable write carries a txnNumber. Connect must keep it off the wire
// even when the URI and the caller's own options both ask for retries.
func TestLiveRetryWritesStayOff(t *testing.T) {
	uri := os.Getenv("MGX_TEST_URI")
	if uri == "" {
		t.Skip("MGX_TEST_URI not set")
	}
	ctx := context.Background()
	sawTxnNumber := func(connect func(m *event.CommandMonitor) (*mongo.Collection, func())) bool {
		saw := false
		coll, closeFn := connect(&event.CommandMonitor{Started: func(_ context.Context, e *event.CommandStartedEvent) {
			if _, err := e.Command.LookupErr("txnNumber"); err == nil {
				saw = true
			}
		}})
		defer closeFn()
		if _, err := coll.InsertOne(ctx, bson.D{{Key: "x", Value: 1}}); err != nil {
			t.Fatal(err)
		}
		return saw
	}

	// Control: the bare driver does retry against this server, so the check
	// below is able to fail.
	control := sawTxnNumber(func(m *event.CommandMonitor) (*mongo.Collection, func()) {
		c, err := mongo.Connect(options.Client().ApplyURI(uri).SetRetryWrites(true).SetMonitor(m))
		if err != nil {
			t.Fatal(err)
		}
		coll := c.Database(testDB()).Collection("c_retry")
		return coll, func() { _, _ = coll.DeleteMany(ctx, bson.D{}); _ = c.Disconnect(ctx) }
	})
	if !control {
		t.Skip("server does not support retryable writes, nothing to prove")
	}

	got := sawTxnNumber(func(m *event.CommandMonitor) (*mongo.Collection, func()) {
		db, err := Connect(uri, testDB(), WithClientOptions(options.Client().SetRetryWrites(true).SetMonitor(m)))
		if err != nil {
			t.Fatal(err)
		}
		coll := db.Collection("c_retry")
		return coll, func() { _, _ = coll.DeleteMany(ctx, bson.D{}); _ = db.Close(ctx) }
	})
	if got {
		t.Error("mgx sent a retryable write")
	}
}
