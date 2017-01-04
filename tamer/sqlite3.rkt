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

(define temporary : Path (build-path (find-system-path 'temp-dir) "sqlite3.db"))
(close-output-port (open-output-file temporary #:exists 'truncate/replace))
(define :temporary: : Connection (sqlite3-connect #:database temporary))

(create-master :temporary:)
(sqlite-table-info :temporary: 'master #("type" "notnull" "pk"))
(select-sqlite-master :temporary:)

(with-handlers ([exn:schema? (λ [[e : exn:schema]] (pretty-display (exn:fail:sql-info e) /dev/stderr))])
  (make-master #:name "failure" #:tbl-name "BOOM~~~"))

(define masters : (Listof Master)
  (for/list ([i (in-range (* plan 2))])
    (make-master #:type (list-ref types (remainder (random 256) (length types)))
                 #:name (symbol->string (gensym 'name:))
                 #:tbl-name (symbol->string (gensym 'tbl:))
                 #:rootpage (assert i index?))))

(with-handlers ([exn:fail:sql? (λ [[e : exn:fail:sql]] (pretty-print (exn:fail:sql-info e) /dev/stderr))])
  (insert-master :temporary: masters)
  (insert-master :temporary: masters))

(for ([record (in-master :temporary:)] [idx (in-naturals)])
  (when (master? record)
    (cond [(< idx plan) (delete-master :temporary: record)]
          [(< idx (+ plan 2)) (update-master :temporary: (remake-master record))]
          [else (with-handlers ([exn:fail:sql? (λ [[e : exn:fail:sql]] (pretty-print (exn:fail:sql-info e) /dev/stderr))])
                  (update-master :temporary: #:check-first? (odd? idx) (remake-master record #:uuid (uuid:random))))])))

(for ([record (in-master :temporary:)])
  (pretty-display record (if (exn? record) /dev/stderr /dev/stdout)))

(with-handlers ([exn? (λ [[e : exn]] (make-schema-message struct:sqlite-master null 'create #:error e))])
  (create-sqlite-master :temporary:))

(disconnect :temporary:)
