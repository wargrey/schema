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
  (define-table master #:as Master #:with [uuid name] #:order-by seed
    ([uuid     : Integer       #:default (pk64:timestamp launch-time)]
     [type     : Symbol        #:default 'table #:not-null #:% 'table]
     [name     : String        #:not-null #:unique #:check (string-contains? name ":")]
     [ctime    : Fixnum        #:default (current-milliseconds)]
     [mtime    : Fixnum        #:auto (current-milliseconds)]
     [seed     : String        #:default (number->string (random 256))])))

(define :memory: : Connection (sqlite3-connect #:database 'memory))
(sqlite3-version :memory:)

(with-handlers ([exn? (λ [[e : exn]] (pretty-write (exn->schema-message e) /dev/stderr))])
  (sqlite-master:create :memory:))

(newline)
(master:create :memory:)
(sqlite3-table-info :memory: 'master #("type" "notnull" "pk"))
(sqlite-master:select :memory:)

(with-handlers ([exn:schema? (λ [[e : exn:schema]] (pretty-write (exn:fail:sql-info e) /dev/stderr))])
  (make-master #:name "[awkward]"))

(newline)
(define masters : (Listof Master)
  (for/list ([i (in-range plan)])
    (make-master #:type (list-ref types (remainder (random 256) (length types)))
                 #:name (symbol->string (gensym 'master:)))))

(with-handlers ([exn:fail:sql? (λ [[e : exn:fail:sql]] (pretty-write (exn:fail:sql-info e) /dev/stderr))])
  (master:insert :memory: masters)
  (master:insert :memory: masters))

(master:select :memory: #:asc? #false #:limit 4)
(cons 'seed (?master-seed :memory:))
(for/list : (Listof Any) ([aggr (in-list (list ?master:count ?master:min ?master:average ?master:max ?master:sum))])
  (cons (object-name aggr) (aggr :memory: 'seed)))

(newline)
(for ([record (in-master :memory:)] [idx (in-naturals)])
  (when (master? record)
    (cond [(< idx (quotient plan 2)) (master:delete :memory: record)]
          [(< idx (* (quotient plan 4) 3)) (master:update :memory: (remake-master record))]
          [else (with-handlers ([exn:fail:sql? (λ [[e : exn:fail:sql]] (pretty-write (exn:fail:sql-info e) /dev/stderr))])
                  (master:update :memory: #:check-first? (odd? idx) (remake-master record #:uuid (pk64:random))))])))

(newline)
(for/list : (Listof Any) ([m (in-list masters)])
  (define uuids : #%Master (#%master m))
  (with-handlers ([exn? values])
    (define ?m : (Option Master) (master:seek :memory: #:where uuids))
    (cond [(false? ?m) (cons uuids 'deleted)]
          [(eq? (master-ctime ?m) (master-mtime ?m)) (cons uuids 'unchanged)]
          [else ?m])))

(disconnect :memory:)

(newline)
(define src : Master (remake-master #false #:name "remake:make"))
(cons src (remake-master src #:name "remake:okay"))
(with-handlers ([exn? (λ [[e : exn]] (pretty-write (exn->info e) /dev/stderr))])
  (remake-master #false))

(newline)
(let ([src-bytes (master-serialize src)])
  (values src-bytes
          (with-handlers ([exn:schema? (λ [[e : exn:schema]] (pretty-write (exn:fail:sql-info e) /dev/stderr))])
            (master-deserialize src-bytes))))

(newline)
(master-examples)
