#lang digimon

(provide (all-defined-out))

(require typed/db)
(require "../main.rkt")
(require "../digitama/sqlite3.rkt")

(define types : (Listof Symbol) '(table index view trigger))
(define plan : Index (length types))

(define-schema SchemaTamer
  (define-table master #:as Master #:with uuid racket
    ([uuid     : String        #:default (uuid:timestamp)]
     [type     : Symbol        #:default 'table #:not-null]
     [name     : String        #:not-null #:unique]
     [tbl-name : String        #:not-null]
     [rootpage : Natural       #:default (random 32)]
     [ctime    : Fixnum        #:default (current-microseconds)]
     [mtime    : Fixnum        #:auto (current-microseconds)]))

  (define-table sqlite-master #:as Sqlite-Master #:with rowid
    ([type     : String        #:not-null]
     [name     : String        #:not-null]
     [tbl-name : String        #:not-null]
     [rootpage : Natural       #:not-null]
     [sql      : String])))

(define :memory: : Connection (sqlite3-connect #:database 'memory))

(create-master :memory:)
(sqlite-pragma :memory: 'table-info 'master)
(select-sqlite-master :memory:)

(insert-master :memory: (for/list : (Listof Master) ([i (in-range (* plan 2))])
                         (make-master #:type (list-ref types (remainder (random 256) (length types)))
                                      #:name (symbol->string (gensym 'name:))
                                      #:tbl-name (symbol->string (gensym 'tbl:))
                                      #:rootpage (assert i index?))))

(for ([record (in-master :memory:)] [idx (in-naturals)])
  (when (master? record)
    (cond [(< idx plan) (delete-master :memory: record)]
          [(< idx (+ plan 2)) (update-master #:dbconnection :memory: record)]
          [else (update-master #:dbconnection :memory: record #:uuid (uuid:random))])))

(for ([record (in-master :memory:)])
  (pretty-display record (if (exn? record) /dev/stderr /dev/stdout)))

(disconnect :memory:)
