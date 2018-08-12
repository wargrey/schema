#lang typed/racket/base

(provide (all-defined-out))
(provide (all-from-out typed/db/base))
(provide (all-from-out typed/db/sqlite3))

(require racket/file)
(require typed/db/base)
(require typed/db/sqlite3)

(require "digitama/schema.rkt")
(require "digitama/virtual-sql.rkt")
(require "digitama/normalize.rkt")
(require "digitama/misc.rkt")

(require/typed db/private/sqlite3/ffi
               [sqlite3_libversion_number (-> Integer)])

(define-type Pragma-Datum (U Real Boolean Symbol String))

(sqlite3-support-without-rowid? (>= (sqlite3_libversion_number) 3008002))

(define-schema SQLite3-Tables
  (define-table [sqlite-master sqlite-master] #:as Sqlite-Master #:with rowid
    ([type     : Symbol        #:not-null]
     [name     : String        #:not-null]
     [tbl-name : String        #:not-null]
     [rootpage : Natural       #:not-null]
     [sql      : String])))

(define sqlite3-create-database-if-not-exists : (->* (Path-String) () Void)
  (lambda [db]
    ; TODO: make the db as a application file format
    (unless (file-exists? db)
      (make-parent-directory* db)
      (call-with-output-file* db void))))

(define sqlite3-pragma : (->* (Connection Symbol) ((U Pragma-Datum Void) #:schema Symbol) (U Simple-Result Rows-Result))
  ;;; https://www.sqlite.org/pragma.html
  (lambda [sqlite name [argument (void)] #:schema [schema 'main]]
    (define dbms : Symbol (dbsystem-name (connection-dbsystem sqlite)))
    (unless (eq? dbms 'sqlite3) (throw exn:fail:unsupported "sqlite3-pragma: not the target database system: ~a" dbms))
    (define pragma.sql : String (string-append "PRAGMA " (name->sql schema) "." (name->sql name)))
    (cond [(void? argument) (query sqlite (string-append pragma.sql ";"))]
          [else (query sqlite (format (string-append pragma.sql " = ~a;")
                                      (cond [(boolean? argument) (if argument "true" "false")]
                                            [(symbol? argument) (name->sql argument)]
                                            [else argument])))])))

(define sqlite3-table-info : (->* (Connection Symbol) ((Vectorof SQL-Field) #:schema Symbol) SQL-Dictionary)
  (lambda [dbc table [value (ann #("cid" "name" "type" "notnull" "dflt_value" "pk") (Vectorof SQL-Field))] #:schema [schema 'main]]
    (define info : (U Simple-Result Rows-Result) (sqlite3-pragma #:schema schema dbc 'table-info table))
    (cond [(simple-result? info) #|should not happen|# (make-immutable-hash)]
          [else (rows->dict info #:key "name" #:value value #:value-mode '(preserve-null))])))

(define sqlite3-version : (-> Connection Integer)
  (lambda [dbc]
    (sqlite3_libversion_number)))
