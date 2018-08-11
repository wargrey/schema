#lang typed/racket

(provide (all-defined-out))

(require "../main.rkt")
(require "../sqlite3.rkt")
(require "../digitama/shadow.rkt")

(define types : (Listof Symbol) '(table index view trigger))
(define plan : Index (length types))

(define launch-time : Integer (utc-seconds 2016 12 10 04 44 37))
(define /dev/stderr : Output-Port (current-error-port))

(define-schema SchemaTamer
  (define-table master #:as Master #:with [uuid name]
    ([uuid     : Integer       #:default (pk64:timestamp launch-time)]
     [type     : Symbol        #:default 'table #:not-null #:% 'table]
     [name     : String        #:not-null #:unique #:check (string-contains? name ":")]
     [ctime    : Fixnum        #:default (current-milliseconds)]
     [mtime    : Fixnum        #:auto (current-milliseconds)])))

(define :memory: : Connection (sqlite3-connect #:database 'memory))
(sqlite3-version :memory:)

(with-handlers ([exn? (λ [[e : exn]] (pretty-write (exn->schema-message e) /dev/stderr))])
  (create-sqlite-master :memory:))

(create-master :memory:)
(sqlite3-table-info :memory: 'master #("type" "notnull" "pk"))
(select-sqlite-master :memory:)

(with-handlers ([exn:schema? (λ [[e : exn:schema]] (pretty-write (exn:fail:sql-info e) /dev/stderr))])
  (make-master #:name "[awkward]"))

(define masters : (Listof Master)
  (for/list ([i (in-range plan)])
    (make-master #:type (list-ref types (remainder (random 256) (length types)))
                 #:name (symbol->string (gensym 'master:)))))

(with-handlers ([exn:fail:sql? (λ [[e : exn:fail:sql]] (pretty-write (exn:fail:sql-info e) /dev/stderr))])
  (insert-master :memory: masters)
  (insert-master :memory: masters))

(select-master :memory:)

(for ([record (in-master :memory:)] [idx (in-naturals)])
  (when (master? record)
    (cond [(< idx (quotient plan 2)) (delete-master :memory: record)]
          [(< idx (* (quotient plan 4) 3)) (update-master :memory: (remake-master record))]
          [else (with-handlers ([exn:fail:sql? (λ [[e : exn:fail:sql]] (pretty-write (exn:fail:sql-info e) /dev/stderr))])
                  (update-master :memory: #:check-first? (odd? idx) (remake-master record #:uuid (pk64:random))))])))

(for/list : (Listof Any) ([m (in-list masters)])
  (define uuids : #%Master (#%master m))
  (with-handlers ([exn? values])
    (define ?m : (Option Master) (seek-master :memory: #:where uuids))
    (cond [(false? ?m) (cons uuids 'deleted)]
          [(eq? (master-ctime ?m) (master-mtime ?m)) (cons uuids 'unchanged)]
          [else ?m])))

(disconnect :memory:)

(define src : Master (remake-master #false #:name "remake:make"))
(cons src (remake-master src #:name "remake:okay"))
(with-handlers ([exn? (λ [e] e)]) (remake-master #false))

(let ([src-bytes (master-serialize src)])
  (values src-bytes
          (with-handlers ([exn:schema? (λ [[e : exn:schema]] (pretty-write (exn:fail:sql-info e) /dev/stderr))])
            (master-deserialize src-bytes))))

sqls
(master-examples)
