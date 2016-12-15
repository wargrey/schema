#lang typed/racket/base

(provide (all-defined-out))

(require typed/db)
(require digimon/system)

(require "digitama/schema.rkt")
(require "digitama/normalize.rkt")

(define-type Pragma-Datum (U Real Boolean Symbol String))

(define-schema SQLite3-Tables
  (define-table [sqlite-master sqlite-master] #:as Sqlite-Master #:with rowid
    ([type     : String        #:not-null]
     [name     : String        #:not-null]
     [tbl-name : String        #:not-null]
     [rootpage : Natural       #:not-null]
     [sql      : String])))

(define sqlite-pragma : (->* (Connection Symbol) ((U Pragma-Datum Void) #:schema Symbol) (U Simple-Result Rows-Result))
  ;;; https://www.sqlite.org/pragma.html
  (lambda [sqlite name [argument (void)] #:schema [schema 'main]]
    (define dbms : Symbol (dbsystem-name (connection-dbsystem sqlite)))
    (unless (eq? dbms 'sqlite3) (raise-unsupported-error 'sqlite3-pragma "not the target database system: ~a" dbms))
    (define pragma.sql : String (string-append "PRAGMA " (name->sql schema) "." (name->sql name)))
    (cond [(void? argument) (query sqlite (string-append pragma.sql ";"))]
          [else (query sqlite (format (string-append pragma.sql " = ~a;")
                                      (cond [(boolean? argument) (if argument "true" "false")]
                                            [(symbol? argument) (name->sql argument)]
                                            [else argument])))])))

(define sqlite-table-info : (->* (Connection Symbol) ((Vectorof SQL-Field) #:schema Symbol) Any) ; FIXME: why SQL-Dictionary is unbound?
  (lambda [dbc table [value (ann #("cid" "name" "type" "notnull" "dflt_value" "pk") (Vectorof SQL-Field))] #:schema [schema 'main]]
    (define info : (U Simple-Result Rows-Result) (sqlite-pragma #:schema schema dbc 'table-info table))
    (cond [(simple-result? info) #|should not happen|# (make-immutable-hash)]
          [else (rows->dict info #:key "name" #:value value #:value-mode '(preserve-null))])))
