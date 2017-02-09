#lang digimon

(provide (all-defined-out))

(require "../main.rkt")
(require "../sqlite3.rkt")
(require "../digitama/shadow.rkt")

(define types : (Listof Symbol) '(table index view trigger))
(define plan : Index (length types))

(define launch-time : Integer (utc-seconds 2016 12 10 04 44 37))

(define-schema SchemaTamer
  (define-table master #:as Master #:with [uuid name] racket
    ([uuid     : Integer       #:default (pk64:timestamp launch-time)]
     [type     : Symbol        #:default 'table #:not-null]
     [name     : String        #:not-null #:unique #:check (string-contains? name ":")]
     [ctime    : Fixnum        #:default (current-milliseconds)]
     [mtime    : Fixnum        #:auto (current-milliseconds)])
    #:serialize (λ [[raw : Master]] (tee (~s (master->hash raw #:skip-null? #false))))
    #:deserialize (λ [[raw : SQL-Datum]] : Master (hash->master (read:+? raw hash?) #:unsafe? #true))))

(define :memory: : Connection (sqlite3-connect #:database 'memory))
(sqlite3-version :memory:)

(with-handlers ([exn? (λ [[e : exn]] (make-schema-message struct:sqlite-master 'create e))])
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

(for ([record (in-master :memory:)] [idx (in-naturals)])
  (when (master? record)
    (cond [(< idx (quotient plan 2)) (delete-master :memory: record)]
          [(< idx (* (quotient plan 4) 3)) (update-master :memory: (remake-master record))]
          [else (with-handlers ([exn:fail:sql? (λ [[e : exn:fail:sql]] (pretty-write (exn:fail:sql-info e) /dev/stderr))])
                  (update-master :memory: #:check-first? (odd? idx) (remake-master record #:uuid (pk64:random))))])))

(for/list : (Listof Any) ([m (in-list masters)])
  (define uuids : Master-Rowid (master-rowid m))
  (define ms : (Listof (U exn Master)) (select-master :memory: #:where uuids))
  (cond [(null? ms) (cons uuids 'deleted)]
        [else (let ([m : (U exn Master) (car ms)])
                (cond [(exn? m) m]
                      [(eq? (master-ctime m) (master-mtime m)) (cons uuids 'unchanged)]
                      [else m]))]))

(disconnect :memory:)

(define src : Master (remake-master #false #:name "remake:make"))
(values src (remake-master src #:name "remake:okay") (master->hash src))
(with-handlers ([exn? (λ [e] e)]) (remake-master #false))
(with-handlers ([exn? (λ [e] e)]) (hash->master (make-hasheq)))

sqls
