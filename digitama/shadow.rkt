#lang typed/racket/base

(provide (all-defined-out))

(require racket/sequence)

(require typed/db/base)

(require "unsafe/query.rkt")
(require "exchange/sql.rkt")
(require "virtual-sql.rkt")
(require "misc.rkt")

(define do-create-table : (-> Symbol Boolean Boolean Connection String (Listof+ String)
                              (Listof String) (Listof String) (Listof Boolean) (Listof Boolean) Void)
  (lambda [func view? silent? dbc dbtable rowid cols types not-nulls uniques]
    (unless (not view?) (throw exn:fail:unsupported func "cannot create a temporary view"))
    (query-exec dbc (create-table.sql silent? dbtable rowid cols types not-nulls uniques))))

(define do-insert-table : (All (a) (-> Symbol Boolean Boolean String (Listof String) Connection (Sequenceof a) (Listof (-> a Any)) Void))
  (lambda [func view? replace? dbtable cols dbc selves refs]
    (unless (not view?) (throw exn:fail:unsupported func "cannot insert records into a temporary view"))
    (define insert.sql : Virtual-Statement (insert-into.sql replace? dbtable cols))
    (define dbname : Symbol (dbsystem-name (connection-dbsystem dbc)))
    (for ([record : a selves])
      (define metrics : (Listof SQL-Datum) (for/list ([ref (in-list refs)]) (racket->sql (ref record) dbname)))
      (apply query-exec dbc insert.sql metrics))))

(define do-delete-from-table : (All (a) (-> Symbol Boolean String (Listof+ String) Connection (Sequenceof a) (Listof (-> a Any)) Void))
  (lambda [func view? dbtable rowid dbc selves pkrefs]
    (unless (not view?) (throw exn:fail:unsupported func "cannot delete records from a temporary view" dbtable))
    (define delete.sql : Virtual-Statement (delete-from.sql dbtable rowid))
    (for ([record : a selves])
      (apply query-exec dbc delete.sql
             (for/list : (Listof SQL-Datum) ([ref (in-list pkrefs)])
               (racket->sql-pk (ref record)))))))

(define do-select-table : (All (a) (-> String (U False (Vectorof SQL-Datum) (Pairof String (Listof Any)))
                                       (Listof+ String) (Listof String) (-> (Listof SQL-Datum) a)
                                       Connection (U Positive-Integer +inf.0) (Option Symbol) Boolean Natural Natural (Sequenceof (U a exn))))
  (lambda [dbtable where rowid cols row->table dbc size order-by asc? limit offset]
    (define rows : (Sequenceof (Listof SQL-Datum))
      (cond [(not where) (in-query-rows dbc size (make-query-sql 'nowhere dbtable rowid cols order-by asc? limit offset))]
            [(vector? where) (in-query-rows dbc size (make-query-sql 'byrowid dbtable rowid cols order-by asc? limit offset) (vector->list where))]
            [else (in-query-rows dbc size (make-ugly-sql dbtable where cols order-by asc? limit offset)
                                 (let ([dbname (dbsystem-name (connection-dbsystem dbc))])
                                   (for/list : (Listof SQL-Datum) ([r (in-list (cdr where))])
                                     (racket->sql r dbname))))]))
    (sequence-map (λ [[raw : (Listof SQL-Datum)]] (with-handlers ([exn? (λ [[e : exn]] e)]) (row->table raw))) rows)))

(define do-seek-table : (All (a) (-> String (U (Vectorof SQL-Datum) (Pairof String (Listof Any)))
                                     (Listof+ String) (Listof String) (-> (Vectorof SQL-Datum) a) Connection (Option a)))
  (lambda [dbtable where rowid cols row->table dbc]
    (define (mksql [method : Symbol]) : Virtual-Statement (make-query-sql method dbtable rowid cols #false #true 0 0))
    (define maybe-raw : (Option (Vectorof SQL-Datum))
      (cond [(vector? where) (apply query-maybe-row dbc (mksql 'byrowid) (vector->list where))]
            [else (apply query-maybe-row dbc (make-ugly-sql dbtable where cols #false #true 0 0)
                         (let ([dbname (dbsystem-name (connection-dbsystem dbc))])
                           (for/list : (Listof SQL-Datum) ([r (in-list (cdr where))])
                             (racket->sql r dbname))))]))
    (and maybe-raw (row->table maybe-raw))))

(define do-update-table : (All (a) (-> Symbol Boolean Symbol Boolean String (Listof+ String) (Listof+ String)
                                       Connection (Sequenceof a) (Listof (-> a Any)) (Listof (-> a Any)) Void))
  (lambda [func view? table check-pk? dbtable rowid cols dbc selves refs pkrefs]
    (unless (not view?) (throw exn:fail:unsupported func "cannot update records of a temporary view"))
    (define up.sql : Virtual-Statement (update.sql dbtable rowid cols))
    (define ck.sql : Virtual-Statement (if check-pk? (make-query-sql 'ckrowid dbtable rowid cols #false #true 0 0) up.sql))
    (define dbname : Symbol (dbsystem-name (connection-dbsystem dbc)))
    (for ([record : a selves])
      (define rowid : (Listof SQL-Datum) (for/list ([ref (in-list pkrefs)]) (racket->sql-pk (ref record))))
      (when (and check-pk? (not (apply query-maybe-value dbc ck.sql rowid)))
        (schema-throw [exn:schema 'norow `((struct . ,table) (record . ,(list->vector rowid)))]
                      func "no such record found in the table"))
      (define metrics : (Listof SQL-Datum) (for/list ([ref (in-list refs)]) (racket->sql (ref record) dbname)))
      (apply query dbc up.sql metrics))))

(define do-table-aggregate : (-> String Symbol (Option Symbol) Boolean Connection SQL-Datum)
  (lambda [dbtable function column distinct? dbc]
    (define aggr.sql : Virtual-Statement (aggregate.sql dbtable function column distinct?))
    (query-maybe-value dbc aggr.sql)))

(define do-select-column : (All (a) (-> Symbol Symbol Symbol Any String String (-> String Any) (-> Any Boolean : a)
                                        Connection (Option Symbol) Boolean Natural Natural
                                        (Listof (U a exn))))
  (lambda [func table field types dbtable dbfield guard fieldtype? dbc order-by asc? limit offset]
    (define select.sql : Virtual-Statement (simple-select.sql 'nowhere dbtable null (list dbfield) order-by asc? limit offset))
    (for/list ([datum : SQL-Datum (in-list (query-list dbc select.sql))])
      (define v : Any (sql->racket datum guard))
      (cond [(fieldtype? v) v]
            [else (make-schema-error [exn:schema 'assertion `((struct . ,table) (field . ,field) (expected . ,types) (got . ,v))]
                                     func "impossible value found in the column")]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
;; TODO: does it need to cache these statements?
(define make-query-sql : (-> Symbol String (Listof+ String) (Listof String) (Option Symbol) Boolean Natural Natural Virtual-Statement)
  (lambda [method dbtable rowid cols order-by asc? limit offset]
    (simple-select.sql method dbtable rowid cols order-by asc? limit offset)))

(define make-ugly-sql : (-> String (Pairof String (Listof Any)) (Listof String) (Option Symbol) Boolean Natural Natural Virtual-Statement)
  (lambda [dbtable where cols order-by asc? limit offset]
    (ugly-select.sql dbtable (car where) (length (cdr where)) cols order-by asc? limit offset)))
