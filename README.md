# mgx - MongoDB Extended

A type-safe, generic wrapper over the official MongoDB Go driver (`go.mongodb.org/mongo-driver/v2`), restricted to what **Firestore with MongoDB compatibility** supports. It is the sibling of [dsx](https://github.com/louvri/dsx), which does the same for Datastore, and keeps its shape so code moves between the two with few changes.

## Features

- **Type-safe generics** - results come back as `T`, not hand-decoded `bson.D`
- **Fluent query builder** - an operator enum instead of hand-written filter documents
- **Injection-safe filters** - a `WithFilter` value is always an operand, never a query document
- **Keyset (cursor) pagination** - constant cost at any depth, which the driver does not provide
- **Firestore connection rules** - retryable writes are always off, whatever the URI says
- **Honest not-found** - lookups report `ErrNotFound` instead of a nil document
- **Automatic batching** - batch reads, writes and deletes are chunked into small requests
- **Transactions** - carried by the context, so mgx and driver calls join them alike
- **Escape hatch** - `db.Collection(name)` returns the driver collection for anything else

## Installation

```bash
go get github.com/louvri/mgx
```

## Quick Start

```go
type User struct {
    ID     string `bson:"_id,omitempty"`
    Name   string `bson:"name"`
    Status string `bson:"status"`
}

db, err := mgx.Connect(uri, "my-database")
if err != nil {
    log.Fatal(err)
}
defer db.Close(ctx)

users, err := mgx.Query[User](db, "users").
    WithFilter("status", mgx.OpEqual, "active").
    WithOrder("name").
    WithLimit(10).
    Select(ctx)

user, err := mgx.GetByID[User](ctx, db, "users", "user-123")
if errors.Is(err, mgx.ErrNotFound) {
    // no such user
}
```

## Connecting

`Connect` takes a connection string and the database name. It does not dial; like the driver, the connection opens on first use. Ping through `db.Client()` to fail fast.

```go
// GKE, Cloud Run, Compute Engine (workload identity)
uri := "mongodb://UID.LOCATION.firestore.goog:443/DATABASE?loadBalanced=true&tls=true&retryWrites=false" +
    "&authMechanism=MONGODB-OIDC&authMechanismProperties=ENVIRONMENT:gcp,TOKEN_RESOURCE:FIRESTORE"

// Username and password
uri := "mongodb://USER:PASSWORD@UID.LOCATION.firestore.goog:443/DATABASE?loadBalanced=true&tls=true&retryWrites=false&authMechanism=SCRAM-SHA-256"

db, err := mgx.Connect(uri, "DATABASE",
    mgx.WithClientOptions(options.Client().SetAppName("billing")))
```

Options passed with `WithClientOptions` override the URI, except retryable writes, which Firestore does not support and `Connect` always disables.

## Querying

```go
q := mgx.Query[User](db, "users").
    WithFilter("age", mgx.OpGreaterEqual, 18).
    WithFilter("age", mgx.OpLess, 65).          // filters are ANDed; a field may repeat
    WithFilter("status", mgx.OpIn, []string{"active", "pending"}).
    WithOrderDesc("createdAt").
    WithProject("name", "status")

users, err := q.Select(ctx)
first, err := q.Get(ctx)                          // ErrNotFound when nothing matches
n, err     := q.Count(ctx)                        // server-side; honours offset and limit
removed, err := mgx.Query[User](db, "users").
    WithFilter("status", mgx.OpEqual, "deleted").
    Delete(ctx)

ids, err      := mgx.SelectIDs[string](ctx, q)            // ids only, decoded as K
statuses, err := mgx.Distinct[string](ctx, q, "status")
```

Operators: `OpEqual`, `OpNotEqual`, `OpGreater`, `OpGreaterEqual`, `OpLess`, `OpLessEqual`, `OpIn`, `OpNotIn`. `OpIn` and `OpNotIn` take a non-empty slice of any element type. Field names starting with `$` are rejected, since a top-level `$expr` would evaluate the value. For anything else - `$exists`, `$regex`, `$or`, `$elemMatch` - use `WithRawFilter(bson.D{...})`, which is sent as written: never build it from untrusted input.

Builder methods never return an error. The first problem is recorded and returned by the terminal call, or earlier by `Err()`.

`Delete` and `Distinct` cannot be limited or paginated, so a builder carrying a limit, offset or cursor is rejected rather than acting on more documents than asked for. A `Delete` with no filter empties the collection.

## Pagination

Offset pagination is fine for the first few pages; the server still walks every document it skips.

```go
page, err := q.WithOffset(40).WithLimit(20).Select(ctx)
```

Cursor pagination costs the same at any depth:

```go
page, next, err := mgx.Query[User](db, "users").
    WithOrder("status").
    WithLimit(20).
    WithCursor(cursorFromRequest).   // "" means the first page
    SelectWithCursor(ctx)
```

The cursor records the last document's sort values; the next page asks for documents that sort after them. `_id` is appended to the ordering as a tie-breaker. This needs three things of the data:

- every sort field is present, non-null and not an array on every matching document;
- each sort field holds one BSON type across documents;
- an index covering the filter and the full ordering, ending in `_id`.

A cursor is tied to the ordering it was issued for; using it with another returns `ErrInvalidCursor`. When a page comes back empty, the cursor passed in is returned unchanged. The cursor is not signed or encrypted: it can only move a reader within the query it is used with, but treat it as client input, and remember that anyone holding it can decode the last document's sort values and `_id`. Offset and cursor pagination are mutually exclusive (`ErrPaginationConflict`).

## Writing

```go
users := mgx.Query[User](db, "users")

err := users.Upsert(ctx, "user-123", &user)                 // replace or insert; the id argument wins over data's _id
err  = users.UpsertMulti(ctx, map[string]*User{"a": &a, "b": &b})
id, err  := users.Insert(ctx, &user)                        // ObjectID generated when data has no _id
ids, err := users.InsertMulti(ctx, []*User{&a, &b})         // on error, the ids that did go in are returned too

docs, err := mgx.GetMulti[User](ctx, db, "users", []string{"a", "b"}) // input order; a missing doc is zero-valued
err  = mgx.DeleteByID(ctx, db, "users", "user-123")                   // deleting a missing doc is not an error
n, err := mgx.DeleteMultiByID(ctx, db, "users", []string{"a", "b"})
```

The write calls address documents by id only, so a builder carrying a filter, ordering, projection or pagination is rejected rather than having it silently ignored - a dropped tenant filter would otherwise overwrite another tenant's document. A nil or empty id is rejected everywhere, including inside the batch calls, since a nil pointer would otherwise address the document whose `_id` is null. Batch writes go in requests of 500 and lookups in requests of 1000. Each request is independent, so a failure part-way leaves earlier batches applied; for all-or-nothing, write inside a transaction.

## Transactions

```go
err := mgx.RunInTransaction(ctx, db, func(ctx context.Context) error {
    from, err := mgx.GetByID[Account](ctx, db, "accounts", fromID)
    if err != nil {
        return err
    }
    // ...
    return mgx.Query[Account](db, "accounts").Upsert(ctx, fromID, from)
})
```

The transaction travels in the context handed to the callback: calls made with that context join it, calls made with the outer context do not. A nested `RunInTransaction` with the callback's context joins the outer transaction. The driver re-runs the callback on a transient conflict, so it must be safe to run more than once. Firestore ends a transaction after 270 seconds, or 60 seconds idle.

## Differences from dsx

- A kind is a collection and a key is the document's `_id`, which may be any type Firestore accepts (string, ObjectID, int32, int64, double, binary, document).
- There are no namespaces and no ancestor queries.
- Cursors are keyset tokens built by mgx, not server cursors.
- `OpNotEqual` and `OpNotIn` also match documents that lack the field. Datastore skips those.
- `Close` and every I/O call take a context.

## Testing

Unit tests need no server. The live tests run when `MGX_TEST_URI` is set; the transaction test also needs `MGX_TEST_TXN=1` and a server that supports transactions (a replica set).

```bash
go test ./...
MGX_TEST_URI="mongodb://127.0.0.1:27017/?directConnection=true" MGX_TEST_TXN=1 go test -race ./...
```

`MGX_TEST_DB` overrides the database name (default `mgx_test`). The tests delete and rewrite collections named `c_test*` in it.

## License

MIT
