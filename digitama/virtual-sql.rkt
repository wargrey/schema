#lang typed/racket

(provide (all-defined-out))

(require digimon/system)

(require typed/db)

(define-predicate sql-datum? SQL-Datum)

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

(define create-table.sql : (-> Boolean String String (Option String) (Listof String)
                               (Listof Boolean) (Listof Boolean)
                               Virtual-Statement)
  (lambda [silent? table pk raw columns not-nulls uniques]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (define &extra : (Boxof String) (box ""))
       (define (+++ [name : String] !null uniq) : String
         (cond [(equal? name pk) (set-box! &extra " WITHOUT ROWID") (string-append name " PRIMARY KEY")]
               [(and !null uniq) (string-append name " UNIQUE NOT NULL")]
               [(and !null) (string-append name " NOT NULL")]
               [(and uniq) (string-append name " UNIQUE")]
               [else name]))
       (case (dbsystem-name dbms)
         [(sqlite3)
          (format "CREATE ~a ~a (~a~a)~a;" (if silent? "TABLE IF NOT EXISTS" "TABLE")
                  table (string-join (map +++ columns not-nulls uniques) ", ")
                  (if (false? raw) "" (format ", ~a TEXT NOT NULL" raw)) (unbox &extra))]
         [else (raise-unsupported-error 'create-table "unknown database system: ~a" (dbsystem-name dbms))])))))

(define insert-into.sql : (-> Boolean String String (Option String) (Listof String) Virtual-Statement)
  (lambda [replace? table pk raw columns]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (case (dbsystem-name dbms)
         [(sqlite3)
          (define-values (snmuloc seulva$)
            (for/fold ([snmuloc : (Listof String) null] [seulva$ : (Listof String) null])
                      ([col (in-list (append (cdr columns) (if raw (list raw) null)))]
                       [idx (in-naturals 2)])
              (values (cons col (cons ", " snmuloc))
                      (cons (number->string idx) (cons ", $" seulva$)))))
          (format "INSERT ~a ~a (~a~a) VALUES ($1~a);" (if replace? "OR REPLACE INTO" "INTO") table (car columns)
                  (apply string-append (reverse snmuloc))
                  (apply string-append (reverse seulva$)))]
         [else (raise-unsupported-error 'create-table "unknown database system: ~a" (dbsystem-name dbms))])))))

(define simple-select.sql : (-> Boolean String String (Option String) (Listof String) Virtual-Statement)
  (lambda [where? table pk raw columns]
    (virtual-statement
     (cond [(false? where?) (format "SELECT ~a FROM ~a;" (or raw pk) table)]
           [else (λ [[dbms : DBSystem]]
                   (format "SELECT ~a FROM ~a WHERE ~a = ~a;" (string-join columns ", ") table pk
                           (if (eq? (dbsystem-name dbms) 'postgresql) "$1" "?")))]))))
