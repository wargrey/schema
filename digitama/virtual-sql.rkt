#lang typed/racket/base

(provide (all-defined-out))

(require racket/string)
(require racket/list)

(require typed/db/base)

(require "misc.rkt")
(require "normalize.rkt")

(define sqlite3-support-without-rowid? : (Parameterof Boolean) (make-parameter #true))

(define $? : (-> Symbol Integer String)
  (lambda [dbn idx]
    (cond [(eq? dbn 'postgresql) (string-append "$" (number->string idx))]
          [(eq? dbn 'sqlite3) (string-append "?" (number->string idx))]
          [else "?"])))

(define string-join-map : (-> (Listof String) String String)
  (lambda [cols func]
    (format (string-append func "(~a)")
            (string-join cols (string-append "), " func "(")))))

(define string-join=$i : (->* (Symbol (Listof String) String) (Index) String)
  (lambda [dbn cols sep [i0 0]]
    (string-join (for/list : (Listof String)
                   ([pk (in-list cols)]
                    [idx (in-naturals (add1 i0))])
                   (string-append pk " = " ($? dbn idx)))
                 sep)))

(define order-limit : (-> (Option Symbol) Boolean Natural Natural String)
  (lambda [order-by asc? limit offset]
    (string-append (if (not order-by) "" (format " ORDER BY ~a ~a" (name->sql order-by) (if asc? "ASC" "DESC")))
                   (cond [(and (eq? limit 0) (eq? offset 0)) ""]
                         [(and (> limit 0) (> offset 0)) (format " LIMIT ~a OFFSET ~a" limit offset)]
                         [(> limit 0) (format " Limit ~a" limit)]
                         [else (format " Limit -1 OFFSET ~a" offset)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define create-table.sql : (-> Boolean String (Listof+ String) (Listof String) (Listof String)
                               (Listof Boolean) (Listof Boolean) Virtual-Statement)
  (lambda [silent? table rowid cols types not-nulls uniques]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (define without-rowid? : Boolean #false)
       (define single-pk : (Option String) (and (null? (cdr rowid)) (car rowid)))
       (define (++++ [name : String] [type : String] !null uniq) : String
         (define name+type : String (string-append name " " type))
         (when (and (not without-rowid?) (member name rowid)) (set! without-rowid? #true))
         (cond [(equal? name single-pk) (string-append name+type " PRIMARY KEY")]
               [(and !null uniq) (string-append name+type " UNIQUE NOT NULL")]
               [(and !null) (string-append name+type " NOT NULL")]
               [(and uniq) (string-append name+type " UNIQUE")]
               [else name+type]))
       (case (dbsystem-name dbms)
         [(sqlite3)
          (format "CREATE ~a ~a (~a~a)~a;"
                  (if silent? "TABLE IF NOT EXISTS" "TABLE") table
                  (string-join (map ++++ cols types not-nulls uniques) ", ")
                  (if single-pk "" (format ", PRIMARY KEY (~a)" (string-join rowid ", ")))
                  (if (and without-rowid? (sqlite3-support-without-rowid?)) " WITHOUT ROWID" ""))]
         [else (throw exn:fail:unsupported 'create-table.sql "unknown database system: ~a" (dbsystem-name dbms))])))))

(define insert-into.sql : (-> Boolean String (Listof String) Virtual-Statement)
  (lambda [replace? table cols]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (case (dbsystem-name dbms)
         [(sqlite3)
          (define-values (snmuloc seulva$)
            (for/fold ([snmuloc : (Listof String) null] [seulva$ : (Listof String) null])
                      ([col (in-list (cdr cols))]
                       [idx (in-naturals 2)])
              (values (cons col (cons ", " snmuloc))
                      (cons (number->string idx) (cons ", $" seulva$)))))
          (format "INSERT ~a ~a (~a~a) VALUES ($1~a);" (if replace? "OR REPLACE INTO" "INTO") table (car cols)
                  (apply string-append (reverse snmuloc))
                  (apply string-append (reverse seulva$)))]
         [else (throw exn:fail:unsupported 'insert-into.sql "unknown database system: ~a" (dbsystem-name dbms))])))))

(define update.sql : (-> String (Listof+ String) (Listof String) Virtual-Statement)
  (lambda [table rowid cols]
    (define rowid-size : Index (length rowid))
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (case (dbsystem-name dbms)
         [(sqlite3)
          (format "UPDATE ~a SET ~a WHERE ~a;" table
                  (string-join=$i 'sqlite3 (drop cols rowid-size) ", " rowid-size)
                  (string-join=$i 'sqlite3 rowid " AND " 0))]
         [else (throw exn:fail:unsupported 'update.sql "unknown database system: ~a" (dbsystem-name dbms))])))))

(define simple-select.sql : (-> Symbol String (Listof String) (Listof String) (Option Symbol) Boolean Natural Natural Virtual-Statement)
  (lambda [which table rowid cols by asc? limit offset]
    (define ~select : String "SELECT ~a FROM ~a WHERE ~a~a;")
    (define (rowid-join [dbms : DBSystem]) : String (string-join=$i (dbsystem-name dbms) rowid " AND " 0))
    (virtual-statement
     (case which
       [(byrowid) (λ [[dbms : DBSystem]] (format ~select (string-join cols ", ") table (rowid-join dbms) (order-limit by asc? limit offset)))]
       [(ckrowid) (λ [[dbms : DBSystem]] (format ~select (car rowid) table (rowid-join dbms) (order-limit by asc? limit offset)))]
       [else #|nowhere|# (format "SELECT ~a FROM ~a~a;" (string-join cols ", ") table (order-limit by asc? limit offset))]))))

(define ugly-select.sql : (-> String String Index (Listof String) (Option Symbol) Boolean Natural Natural Virtual-Statement)
  (lambda [table where argn cols by asc? limit offset]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (define dbn : Symbol (dbsystem-name dbms))
       (format "SELECT ~a FROM ~a WHERE ~a~a;"
         (string-join cols ", ") table
         (apply format where (build-list argn (λ [[idx : Index]] ($? dbn (add1 idx)))))
         (order-limit by asc? limit offset))))))

(define delete-from.sql : (-> String (Listof String) Virtual-Statement)
  (lambda [table rowid]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (format "DELETE FROM ~a WHERE ~a;" table
               (string-join=$i (dbsystem-name dbms)
                               rowid " AND " 0))))))

(define aggregate.sql : (-> String Symbol (Option Symbol) Boolean Virtual-Statement)
  (lambda [table function maybe-column distinct?]
    (define column : String (if (symbol? maybe-column) (name->sql maybe-column) "*"))
    (virtual-statement
     (case function
       [(average)
        (λ [[dbms : DBSystem]]
          (case (dbsystem-name dbms)
            [(sqlite3) (format "SELECT avg(~a~a) FROM ~a;" (if distinct? "DISTINCT " "") column table)]
            [else (throw exn:fail:unsupported 'aggregate.sql "unknown database system: ~a" (dbsystem-name dbms))]))]
       [(count)
        (λ [[dbms : DBSystem]]
          (case (dbsystem-name dbms)
            [(sqlite3) (format "SELECT count(~a~a) FROM ~a;" (if distinct? "DISTINCT " "") column table)]
            [else (throw exn:fail:unsupported 'aggregate.sql "unknown database system: ~a" (dbsystem-name dbms))]))]
       [(min)
        (λ [[dbms : DBSystem]]
          (case (dbsystem-name dbms)
            [(sqlite3) (format "SELECT min(~a~a) FROM ~a;" (if distinct? "DISTINCT " "") column table)]
            [else (throw exn:fail:unsupported 'aggregate.sql "unknown database system: ~a" (dbsystem-name dbms))]))]
       [(max)
        (λ [[dbms : DBSystem]]
          (case (dbsystem-name dbms)
            [(sqlite3) (format "SELECT max(~a~a) FROM ~a;" (if distinct? "DISTINCT " "") column table)]
            [else (throw exn:fail:unsupported 'aggregate.sql "unknown database system: ~a" (dbsystem-name dbms))]))]
       [(sum)
        (λ [[dbms : DBSystem]]
          (case (dbsystem-name dbms)
            [(sqlite3) (format "SELECT sum(~a~a) FROM ~a;" (if distinct? "DISTINCT " "") column table)]
            [else (throw exn:fail:unsupported 'aggregate.sql "unknown database system: ~a" (dbsystem-name dbms))]))]
       [else (throw exn:fail:unsupported 'aggregate.sql "unknown aggregate function: ~a" function)]))))
