#lang typed/racket/base

(provide (all-defined-out))

(require racket/list)
(require racket/bool)
(require racket/sequence)

(require typed/db/base)

(require "message.rkt")
(require "virtual-sql.rkt")
(require "misc.rkt")

(require/typed "cheat.rkt"
               [in-query-rows (->* (Connection (U Positive-Integer +inf.0) Statement)
                                   ((Listof SQL-Datum))
                                   (Sequenceof (Listof SQL-Datum)))])

(define do-create-table : (-> (Option Symbol) Symbol (Option Symbol) Connection String (Listof+ String)
                              (Listof String) (Listof String) (Listof Boolean) (Listof Boolean) Void)
  (lambda [func create maybe-force dbc dbtable rowid cols types not-nulls uniques]
    (unless (not func) (throw exn:fail:unsupported func "cannot create a temporary view"))
    (define (mksql) : Virtual-Statement (create-table.sql maybe-force dbtable rowid cols types not-nulls uniques))
    (query-exec dbc (hash-ref! sqls (or maybe-force create) mksql))))

(define do-insert-table : (All (a) (-> (Option Symbol) Symbol (Option Symbol) String (Listof String)
                                       Connection (Sequenceof a) (Listof (-> a Any)) Void))
  (lambda [func insert maybe-replace dbtable cols dbc selves refs]
    (define (mksql) : Virtual-Statement (insert-into.sql maybe-replace dbtable cols))
    (unless (not func) (throw exn:fail:unsupported func "cannot insert records into a temporary view"))
    (define insert.sql : Statement (hash-ref! sqls (or maybe-replace insert) mksql))
    (for ([record : a selves])
      (define metrics : (Listof SQL-Datum) (for/list ([ref (in-list refs)]) (racket->sql (ref record) dbc)))
      (apply query-exec dbc insert.sql metrics))))

(define do-delete-from-table : (All (a) (-> Symbol Boolean String (Listof+ String)
                                            Connection (Sequenceof a) (Listof (-> a Any)) Void))
  (lambda [func view? dbtable rowid dbc selves pkrefs]
    (define (mksql) : Virtual-Statement (delete-from.sql dbtable rowid))
    (when view? (throw exn:fail:unsupported func "cannot delete records from a temporary view"))
    (define delete.sql : Statement (hash-ref! sqls func mksql))
    (for ([record : a selves])
      (apply query-exec dbc delete.sql
             (for/list : (Listof SQL-Datum) ([ref (in-list pkrefs)])
               (racket->sql-pk (ref record)))))))

(define do-select-table : (All (a) (-> Symbol Symbol String (U False (Vectorof SQL-Datum) (Pairof String (Listof Any)))
                                       (Listof+ String) (Listof String) (-> (Listof SQL-Datum) a)
                                       Connection (U Positive-Integer +inf.0) (Sequenceof (U a exn))))
  (lambda [select-nowhere select-where dbtable where rowid cols mkrow dbc size]
    (define (mksql [method : Symbol]) : (-> Statement) (λ [] (simple-select.sql method dbtable rowid cols)))
    (define rows : (Sequenceof (Listof SQL-Datum))
      (cond [(not where) (in-query-rows dbc size (hash-ref! sqls select-nowhere (mksql 'nowhere)))]
            [(vector? where) (in-query-rows dbc size (hash-ref! sqls select-where (mksql 'byrowid)) (vector->list where))]
            [else (in-query-rows dbc size (make-ugly-sql dbtable where cols)
                                 (for/list : (Listof SQL-Datum) ([r (in-list (cdr where))])
                                   (racket->sql r dbc)))]))
    (sequence-map (λ [[raw : (Listof SQL-Datum)]] (with-handlers ([exn? (λ [[e : exn]] e)]) (mkrow raw))) rows)))

(define do-seek-table : (All (a) (-> Symbol String (U (Vectorof SQL-Datum) (Pairof String (Listof Any)))
                                     (Listof+ String) (Listof String) (-> (Listof SQL-Datum) a) Connection (Option a)))
  (lambda [select-where dbtable where rowid cols mkrow dbc]
    (define (mksql [method : Symbol]) : (-> Statement) (λ [] (simple-select.sql method dbtable rowid cols)))
    (define (mkugly [fmt : String] [_ : (Listof Any)]) : Statement (ugly-select.sql dbtable fmt (length _) cols))
    (define maybe-raw : (Option (Vectorof SQL-Datum))
      (cond [(vector? where) (apply query-maybe-row dbc (hash-ref! sqls select-where (mksql 'byrowid)) (vector->list where))]
            [else (apply query-maybe-row dbc (make-ugly-sql dbtable where cols)
                         (for/list : (Listof SQL-Datum) ([r (in-list (cdr where))])
                           (racket->sql r dbc)))]))
    (and maybe-raw (mkrow (vector->list maybe-raw)))))

(define do-update-table : (All (a) (-> Symbol Boolean Symbol (Option Symbol) String (Listof+ String) (Listof+ String)
                                       Connection (Sequenceof a) (Listof (-> a Any)) (Listof (-> a Any)) Void))
  (lambda [func view? table maybe-chpk dbtable rowid cols dbc selves refs pkrefs]
    (when view? (throw exn:fail:unsupported func "cannot update records of a temporary view"))
    (define (mkup) : Virtual-Statement (update.sql dbtable rowid cols))
    (define (mkck) : Virtual-Statement (simple-select.sql 'ckrowid dbtable rowid cols))
    (define up.sql : Statement (hash-ref! sqls func mkup))
    (define ck.sql : Statement (if maybe-chpk (hash-ref! sqls maybe-chpk mkck) up.sql))
    (for ([record : a selves])
      (define rowid : (Listof SQL-Datum) (for/list ([ref (in-list pkrefs)]) (racket->sql-pk (ref record))))
      (when (and maybe-chpk (false? (apply query-maybe-value dbc ck.sql rowid)))
        (schema-throw [exn:schema 'norow `((struct . ,table) (record . ,(list->vector rowid)))]
                      func "no such record found in the table"))
      (define metrics : (Listof SQL-Datum) (for/list ([ref (in-list refs)]) (racket->sql (ref record) dbc)))
      (apply query dbc up.sql (append metrics rowid)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define ugly-sqls : (HashTable Any Statement) (make-hash))
(define sqls : (HashTable Symbol Statement) (make-hasheq))

(define make-ugly-sql : (-> String (Pairof String (Listof Any)) (Listof String) Statement)
  (lambda [dbtable where cols]
    (hash-ref! ugly-sqls
               (cons dbtable (car where))
               (λ [] (ugly-select.sql dbtable (car where) (length (cdr where)) cols)))))

(define check-constraint : (-> Symbol Symbol (Listof Symbol) (Listof Any) (Listof Any) Any * Void)
  (lambda [func table fields literals contracts  . givens]
    (when (memq #false contracts)
      (define expected : (Listof Any)
        (for/list ([result (in-list contracts)]
                   [expected (in-list literals)]
                   #:when (false? result))
          expected))
      (define ?fields : (Listof Symbol) (remove-duplicates (filter symbol? (flatten expected))))
      (define given : HashTableTop
        (for/hasheq ([f (in-list fields)]
                     [v (in-list givens)]
                     #:when (memq f ?fields))
            (values f v)))
      (schema-throw [exn:schema 'contract `((struct . ,table) (expected . ,expected) (given . ,given))]
                    func "constraint violation"))))

(define check-default-value : (All (a) (-> Symbol Symbol (U a Void) a))
  (lambda [func field defval]
    (when (void? defval) (error func "missing value for field '~a'" field))
    defval))

(define check-selected-row : (All (a) (-> Symbol Symbol (-> Any Boolean : #:+ a) (Listof SQL-Datum) (Listof (-> String Any)) a))
  (lambda [func table table-row? fields guards]
    (define metrics : (Listof Any) (map sql->racket fields guards))
    (cond [(table-row? metrics) metrics]
          [else (schema-throw [exn:schema 'assertion `((struct . ,table) (got . ,metrics))]
                              func "maybe the database is penetrated")])))

(define check-example : (-> Any (-> (Listof Any)) (Listof Any))
  (lambda [example mkdefval]
    (cond [(null? example) (mkdefval)]
          [(list? example) example]
          [else (list example)])))

(define check-row : (All (a) (-> Symbol (Listof Any) (-> Any Boolean : #:+ a) String Any * a))
  (lambda [func metrics table-row? errfmt . errmsg]
    (cond [(table-row? metrics) metrics]
          [else (apply error func errfmt errmsg)])))

(define field-value : (All (a b c) (-> Symbol Symbol (Option a) (-> a b) (U b Void) (-> (U c Void)) (U b c)))
  (lambda [func field self table-field value mkdefval]
    (cond [(not (void? value)) value]
          [(not self) (check-default-value func field (mkdefval))]
          [else (table-field self)])))

(define dict->record : (All (a) (-> Symbol HashTableTop (Listof Symbol) (Listof (-> Any)) (-> Any Boolean : #:+ a) a))
  (lambda [func src fields mkdefval table-row?]
    (define metrics : (Listof Any)
      (for/list ([field (in-list fields)]
                 [mkval (in-list mkdefval)])
        (hash-ref src field (λ [] (check-default-value func field (mkval))))))
    (check-row func metrics table-row? "mismatched source: ~a" metrics)))

(define make-dict : (All (a) (-> (Listof Symbol) (Listof a) Boolean (HashTable Symbol a)))
  (lambda [fields fvalues skip?]
    (cond [(not skip?) (make-immutable-hasheq (map (inst cons Symbol a) fields fvalues))]
          [else (for/hasheq : (HashTable Symbol a)
                  ([key (in-list fields)]
                   [val (in-list fvalues)]
                   #:when val)
                  (values key val))])))
