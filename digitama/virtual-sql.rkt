#lang typed/racket/base

(provide (all-defined-out))

(require racket/format)
(require racket/string)
(require typed/db/base)

(require "misc.rkt")

(define sqlite3-support-without-rowid? : (Parameterof Boolean) (make-parameter #true))

(define-predicate sql-datum? SQL-Datum)

(define racket->sql-pk : (-> Any SQL-Datum)
  (lambda [v]
    (cond [(sql-datum? v) v]
          [else (~s v)])))

(define racket->sql : (-> Any Connection SQL-Datum)
  (lambda [v dbc]
    (cond [(not v) sql-null]
          [(boolean? v) (if (memq (dbsystem-name (connection-dbsystem dbc)) '(mysql sqlite3)) 1 v)]
          [(sql-datum? v) v]
          [else (~s v)])))

(define sql->racket : (->* (SQL-Datum) ((-> String Any)) Any)
  (lambda [v [->racket void]]
    (cond [(sql-null? v) #false]
          [(string? v) (->racket v)]
          [else v])))

(define $? : (-> Symbol Integer String)
  (lambda [dbn idx]
    (cond [(memq dbn '(sqlite3 postgresql)) (string-append "$" (number->string idx))]
          [else "?"])))

(define string-join-map : (-> (Listof String) String String)
  (lambda [cols func]
    (format (string-append func "(~a)")
            (string-join cols (string-append "), " func "(")))))

(define string-join=$i : (->* (Symbol (Listof String) String) (Index) String)
  (lambda [dbn cols seq [i0 0]]
    (string-join (for/list : (Listof String)
                   ([pk (in-list cols)]
                    [idx (in-naturals (add1 i0))])
                   (string-append pk " = " ($? dbn idx)))
                 seq)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define create-table.sql : (-> Any String (Listof+ String) (Listof String) (Listof String)
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

(define insert-into.sql : (-> Any String (Listof String) Virtual-Statement)
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
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (case (dbsystem-name dbms)
         [(sqlite3)
          (format "UPDATE ~a SET ~a WHERE ~a;" table
                  (string-join=$i 'sqlite3 cols ", " (length rowid))
                  (string-join=$i 'sqlite3 rowid " AND " 0))]
         [else (throw exn:fail:unsupported 'update.sql "unknown database system: ~a" (dbsystem-name dbms))])))))

(define simple-select.sql : (-> Symbol String (Listof String) (Listof String) Virtual-Statement)
  (lambda [which table rowid cols]
    (define ~select : String "SELECT ~a FROM ~a WHERE ~a;")
    (define (rowid-join [dbms : DBSystem]) : String (string-join=$i (dbsystem-name dbms) rowid " AND " 0))
    (virtual-statement
     (case which
       [(nowhere) (format "SELECT ~a FROM ~a;" (string-join cols ", ") table)]
       [(byrowid) (λ [[dbms : DBSystem]] (format ~select (string-join cols ", ") table (rowid-join dbms)))]
       [(ckrowid) (λ [[dbms : DBSystem]] (format ~select (car rowid) table (rowid-join dbms)))]
       [else #|not used|# (format "SELECT ~a FROM ~a;" (string-join rowid ", ") table)]))))

(define ugly-select.sql : (-> String String Index (Listof String) Virtual-Statement)
  (lambda [table where argn cols]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (define dbn : Symbol (dbsystem-name dbms))
       (format "SELECT ~a FROM ~a WHERE ~a;" (string-join cols ", ") table
               (apply format where (build-list argn (λ [[idx : Index]] ($? dbn idx)))))))))

(define delete-from.sql : (-> String (Listof String) Virtual-Statement)
  (lambda [table rowid]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (format "DELETE FROM ~a WHERE ~a;" table
               (string-join=$i (dbsystem-name dbms)
                               rowid " AND " 0))))))
