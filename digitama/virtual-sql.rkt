#lang digimon

(provide (all-defined-out))

(require typed/db/base)

(define sqlite3-support-without-rowid? : (Parameterof Boolean) (make-parameter #true))

(define-predicate sql-datum? SQL-Datum)

(define racket->sql : (->* (Any Symbol) ((-> Any Symbol (U Void SQL-Datum))) SQL-Datum)
  (lambda [v dbsystem [->sql void]]
    (define user-defined-datum : (U SQL-Datum Void) (->sql v dbsystem))
    (cond [(sql-datum? user-defined-datum) user-defined-datum]
          [(false? v) sql-null]
          [(boolean? v) (if (memq dbsystem '(mysql sqlite3)) 1 v)]
          [(sql-datum? v) v]
          [else (~s v)])))

(define sql->racket : (->* (SQL-Datum) ((-> String Any)) Any)
  (lambda [v [->racket void]]
    (cond [(sql-null? v) #false]
          [(string? v) (->racket v)]
          [else v])))

(define string-join-map : (-> (Listof String) String String)
  (lambda [cols func]
    (format (string-append func "(~a)")
            (string-join cols (string-append "), " func "(")))))

(define string-join=$i : (->* ((Listof String) String) (Index) String)
  (lambda [cols seq [i0 0]]
    (string-join (for/list : (Listof String) ([pk (in-list cols)] [idx (in-naturals (add1 i0))])
                   (string-append pk " = $" (number->string idx)))
                 seq)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define create-table.sql : (-> Any String (Listof+ String) (Option String) (Listof String) (Listof String)
                               (Listof Boolean) (Listof Boolean) Virtual-Statement)
  (lambda [silent? table rowid eam cols types not-nulls uniques]
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
          (format "CREATE ~a ~a (~a~a~a)~a;"
                  (if silent? "TABLE IF NOT EXISTS" "TABLE") table
                  (string-join (map ++++ cols types not-nulls uniques) ", ")
                  (if (false? eam) "" (format ", ~a BLOB NOT NULL" eam))
                  (if single-pk "" (format ", PRIMARY KEY (~a)" (string-join rowid ", ")))
                  (if (and without-rowid? (sqlite3-support-without-rowid?)) " WITHOUT ROWID" ""))]
         [else (throw exn:fail:unsupported 'create-table.sql "unknown database system: ~a" (dbsystem-name dbms))])))))

(define insert-into.sql : (-> Any String (Option String) (Listof String) Virtual-Statement)
  (lambda [replace? table eam cols]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (case (dbsystem-name dbms)
         [(sqlite3)
          (define-values (snmuloc seulva$)
            (for/fold ([snmuloc : (Listof String) null] [seulva$ : (Listof String) null])
                      ([col (in-list (if (string? eam) cols (cdr cols)))]
                       [idx (in-naturals 2)])
              (values (cons col (cons ", " snmuloc))
                      (cons (number->string idx) (cons ", $" seulva$)))))
          (format "INSERT ~a ~a (~a~a) VALUES ($1~a);" (if replace? "OR REPLACE INTO" "INTO") table
                  (if (string? eam) eam (car cols)) ; $1 is the place for serialized value if present.
                  (apply string-append (reverse snmuloc))
                  (apply string-append (reverse seulva$)))]
         [else (throw exn:fail:unsupported 'insert-into.sql "unknown database system: ~a" (dbsystem-name dbms))])))))

(define update.sql : (-> String (Listof+ String) (Option String) (Listof String) Virtual-Statement)
  (lambda [table rowid eam cols]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (case (dbsystem-name dbms)
         [(sqlite3)
          (format "UPDATE ~a SET ~a WHERE ~a;" table
                  (string-join=$i (if eam (cons eam cols) cols) ", " (length rowid))
                  (string-join=$i rowid " AND " 0))]
         [else (throw exn:fail:unsupported 'update.sql "unknown database system: ~a" (dbsystem-name dbms))])))))

(define simple-select.sql : (-> Symbol String (Listof String) (Option String) (Listof String) Virtual-Statement)
  (lambda [which table rowid eam cols]
    (define ~select : String "SELECT ~a FROM ~a WHERE ~a;")
    (virtual-statement
     (case which
       [(nowhere) (format "SELECT ~a FROM ~a;" (or eam (string-join rowid ", ")) table)]
       [(byrowid) (λ [[dbms : DBSystem]] (format ~select (or eam (string-join rowid ", ")) table (string-join=$i rowid " AND " 0)))]
       [(ckrowid) (λ [[dbms : DBSystem]] (format ~select (car rowid) table (string-join=$i rowid " AND " 0)))]
       [else #|row|# (λ [[dbms : DBSystem]] (format ~select (string-join cols ", ") table (string-join=$i rowid " AND " 0)))]))))

(define delete-from.sql : (-> String (Listof String) Virtual-Statement)
  (lambda [table rowid]
    (virtual-statement (λ [[dbms : DBSystem]] (format "DELETE FROM ~a WHERE ~a;" table (string-join=$i rowid " AND " 0))))))
