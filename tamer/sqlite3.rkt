#lang digimon

(provide (all-defined-out))

(require "../main.rkt")
(require "../sqlite3.rkt")

(define types : (Listof Symbol) '(table index view trigger))
(define plan : Index (length types))

(define-schema SchemaTamer
  (define-table master #:as Master #:with uuid racket
    ([uuid     : String        #:default (uuid:timestamp)]
     [type     : Symbol        #:default 'table #:not-null]
     [name     : String        #:not-null #:unique]
     [tbl-name : String        #:not-null #:check (string-prefix? tbl-name "tbl")]
     [rootpage : Natural       #:default (random 32)]
     [ctime    : Fixnum        #:default (current-milliseconds)]
     [mtime    : Fixnum        #:auto (current-milliseconds)])))

(define :memory: : Connection (sqlite3-connect #:database 'memory))

(create-master :memory:)
(sqlite-pragma :memory: 'table-info 'master)
(select-sqlite-master :memory:)

(with-handlers ([exn:schema? (λ [[e : exn:schema]] (pretty-display (exn:fail:sql-info e) /dev/stderr))])
  (make-master #:name "failure" #:tbl-name "BOOM~~~"))

(define masters : (Listof Master)
  (for/list ([i (in-range (* plan 2))])
    (make-master #:type (list-ref types (remainder (random 256) (length types)))
                 #:name (symbol->string (gensym 'name:))
                 #:tbl-name (symbol->string (gensym 'tbl:))
                 #:rootpage (assert i index?))))

(with-handlers ([exn:fail:sql? (λ [[e : exn:fail:sql]] (pretty-print (exn:fail:sql-info e) /dev/stderr))])
  (insert-master :memory: masters)
  (insert-master :memory: masters))

(for ([record (in-master :memory:)] [idx (in-naturals)])
  (when (master? record)
    (cond [(< idx plan) (delete-master :memory: record)]
          [(< idx (+ plan 2)) (update-master :memory: (remake-master record))]
          [else (with-handlers ([exn:fail:sql? (λ [[e : exn:fail:sql]] (pretty-print (exn:fail:sql-info e) /dev/stderr))])
                  (update-master :memory: #:check-first? (odd? idx) (remake-master record #:uuid (uuid:random))))])))

(for ([record (in-master :memory:)])
  (pretty-display record (if (exn? record) /dev/stderr /dev/stdout)))

(disconnect :memory:)
