#lang digimon

(provide (all-defined-out))

(require typed/db)

(define-predicate sql-datum? SQL-Datum)
(define $? : (-> DBSystem String) (λ [dbms] (if (eq? (dbsystem-name dbms) 'postgresql) "$1" "?")))

(define racket->sql : (-> Symbol Any Symbol SQL-Datum)
  (lambda [field v dbsystem]
    (cond [(false? v) sql-null]
          [(boolean? v) (if (memq dbsystem '(mysql sqlite3)) 1 v)]
          [(sql-datum? v) v]
          [else (~a v)])))

(define sql->racket : (-> Symbol SQL-Datum (U Symbol (List Symbol Integer)) SQL-Datum)
  (lambda [field v sql-type]
    (cond [(sql-null? v) #false]
          [else v])))

(define string-join-map : (-> (Listof String) String String)
  (lambda [columns func]
    (format (string-append func "(~a)")
            (string-join columns (string-append "), " func "(")))))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define create-table.sql : (-> Boolean String String (Option String) (Listof String) (Listof String)
                               (Listof Boolean) (Listof Boolean) Virtual-Statement)
  (lambda [silent? table pk racket columns types not-nulls uniques]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (define &extra : (Boxof String) (box ""))
       (define (++++ [name : String] [type : String] !null uniq) : String
         (define name+type : String (string-append name " " type))
         (cond [(equal? name pk) (set-box! &extra " WITHOUT ROWID") (string-append name+type " PRIMARY KEY")]
               [(and !null uniq) (string-append name+type " UNIQUE NOT NULL")]
               [(and !null) (string-append name+type " NOT NULL")]
               [(and uniq) (string-append name+type " UNIQUE")]
               [else name+type]))
       (case (dbsystem-name dbms)
         [(sqlite3)
          (format "CREATE ~a ~a (~a~a)~a;" (if silent? "TABLE IF NOT EXISTS" "TABLE")
                  table (string-join (map ++++ columns types not-nulls uniques) ", ")
                  (if (false? racket) "" (format ", ~a BLOB NOT NULL" racket)) (unbox &extra))]
         [else (throw exn:fail:unsupported 'create-table.sql "unknown database system: ~a" (dbsystem-name dbms))])))))

(define insert-into.sql : (-> Boolean String (Option String) (Listof String) Virtual-Statement)
  (lambda [replace? table racket columns]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (case (dbsystem-name dbms)
         [(sqlite3)
          (define-values (snmuloc seulva$)
            (for/fold ([snmuloc : (Listof String) null] [seulva$ : (Listof String) null])
                      ([col (in-list (append (cdr columns) (if racket (list racket) null)))]
                       [idx (in-naturals 2)])
              (values (cons col (cons ", " snmuloc))
                      (cons (number->string idx) (cons ", $" seulva$)))))
          (format "INSERT ~a ~a (~a~a) VALUES ($1~a);" (if replace? "OR REPLACE INTO" "INTO") table (car columns)
                  (apply string-append (reverse snmuloc))
                  (apply string-append (reverse seulva$)))]
         [else (throw exn:fail:unsupported 'insert-into.sql "unknown database system: ~a" (dbsystem-name dbms))])))))

(define update.sql : (-> String String (Option String) (Listof String) Virtual-Statement)
  (lambda [table pk racket columns]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (case (dbsystem-name dbms)
         [(sqlite3)
          (define column=$values : (Listof String)
            (for/list : (Listof String) ([col (in-list (append columns (if racket (list racket) null)))] [idx (in-naturals 1)])
              (string-append col " = $" (number->string idx))))
          (format "UPDATE ~a SET ~a WHERE ~a = $0;" table (string-join column=$values ", ") pk)]
         [else (throw exn:fail:unsupported 'update.sql "unknown database system: ~a" (dbsystem-name dbms))])))))

(define simple-select.sql : (-> Symbol String String (Option String) (Listof String) Virtual-Statement)
  (lambda [which table pk racket columns]
    (define ~pk : String "SELECT ~a FROM ~a WHERE ~a = ~a;")
    (virtual-statement
     (case which
       [(nowhere) (format "SELECT ~a FROM ~a;" (or racket pk) table)]
       [(ckrowid) (λ [[dbms : DBSystem]] (format ~pk pk table pk ($? dbms)))]
       [else (λ [[dbms : DBSystem]] (format ~pk (string-join columns ", ") table pk ($? dbms)))]))))

(define delete.sql : (-> String String Virtual-Statement)
  (lambda [table pk]
    (virtual-statement (λ [[dbms : DBSystem]] (format "DELETE FROM ~a WHERE ~a = ~a;" table pk ($? dbms))))))
