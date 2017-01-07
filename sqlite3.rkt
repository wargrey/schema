#lang digimon

(provide (all-defined-out))

(require digimon/system)

(require typed/db/base)

(require "digitama/schema.rkt")
(require "digitama/virtual-sql.rkt")
(require "digitama/normalize.rkt")

(require/typed db/private/sqlite3/ffi
               [sqlite3_libversion_number (-> Integer)])

(define-type Pragma-Datum (U Real Boolean Symbol String))

(sqlite3-support-without-rowid? (>= (sqlite3_libversion_number) 3008002))

(define-schema SQLite3-Tables
  (define-table [sqlite-master sqlite-master] #:as Sqlite-Master #:with rowid
    ([type     : String        #:not-null]
     [name     : String        #:not-null]
     [tbl-name : String        #:not-null]
     [rootpage : Natural       #:not-null]
     [sql      : String])))

(define sqlite3-create-database-if-not-exists : (->* (Path-String) () Void)
  (lambda [db]
    ; TODO: make the db as a application file format
    (unless (file-exists? db)
      (define-values (base name dir?) (split-path db))
      (cond [(path? base) (make-directory* base)]
            [else (void '|Do nothing with an immediately relative path or a root directory|)])
      (call-with-output-file* db void))))

(define sqlite3-pragma : (->* (Connection Symbol) ((U Pragma-Datum Void) #:schema Symbol) (U Simple-Result Rows-Result))
  ;;; https://www.sqlite.org/pragma.html
  (lambda [sqlite name [argument (void)] #:schema [schema 'main]]
    (define dbms : Symbol (dbsystem-name (connection-dbsystem sqlite)))
    (unless (eq? dbms 'sqlite3) (raise (throw exn:fail:unsupported "sqlite3-pragma: not the target database system: ~a" dbms)))
    (define pragma.sql : String (string-append "PRAGMA " (name->sql schema) "." (name->sql name)))
    (cond [(void? argument) (query sqlite (string-append pragma.sql ";"))]
          [else (query sqlite (format (string-append pragma.sql " = ~a;")
                                      (cond [(boolean? argument) (if argument "true" "false")]
                                            [(symbol? argument) (name->sql argument)]
                                            [else argument])))])))

(define sqlite3-table-info : (->* (Connection Symbol) ((Vectorof SQL-Field) #:schema Symbol) Any) ; FIXME: why SQL-Dictionary is unbound?
  (lambda [dbc table [value (ann #("cid" "name" "type" "notnull" "dflt_value" "pk") (Vectorof SQL-Field))] #:schema [schema 'main]]
    (define info : (U Simple-Result Rows-Result) (sqlite3-pragma #:schema schema dbc 'table-info table))
    (cond [(simple-result? info) #|should not happen|# (make-immutable-hash)]
          [else (rows->dict info #:key "name" #:value value #:value-mode '(preserve-null))])))

(define sqlite3-version : (-> Connection Integer)
  (lambda [dbc]
    #;(define version (query-maybe-value dbc "select sqlite_version();"))
    #;(for/fold ([V : Integer 0])
                ([v (in-list (filter-map string->number (regexp-match* #px"\\d+" (~a version))))])
        (if (exact-integer? v) (+ (* V 1000) v) 0))
    (sqlite3_libversion_number)))
