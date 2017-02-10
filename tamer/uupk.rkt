#lang digimon

(require "../main.rkt")

; WARNING: This kind of tasks defeat futures!

(define-schema UUPKTamer
  (define-table uupk #:as UUPK #:with pk
    ([pk     : Integer          #:default (pk64:timestamp)]
     [type   : Symbol           #:not-null]
     [urgent : (Listof Integer) #:not-null])))

(define :memory: : Connection (sqlite3-connect #:database 'memory))

(define make-job : (-> (-> Integer) Index (-> (Listof UUPK)))
  (lambda [pk64 fid]
    (thunk (time (build-list 8 (λ [[seq : Index]]
                                  (make-uupk #:pk (pk64)
                                             #:type (value-name pk64)
                                             #:urgent (list fid seq
                                                            (current-milliseconds)
                                                            (current-memory-use)))))))))

(define do-insert : (-> UUPK Void)
  (lambda [record]
    (with-handlers ([exn:fail:sql? (λ [[e : exn:fail:sql]] (pretty-write (cons record (exn:fail:sql-info e)) /dev/stderr))])
      (insert-uupk :memory: record))))

(create-uupk :memory:)

(define pks : (Listof (Listof (Futureof (Listof UUPK))))
  (for/list ([pk64 (in-list (list pk64:timestamp pk64:random))])
    (build-list (processor-count) (λ [[fid : Index]] (future (make-job pk64 fid))))))

(for ([jobs : (Listof (Futureof (Listof UUPK))) (in-list pks)])
  (for ([workers : (Futureof (Listof UUPK)) (in-list jobs)])
    (map do-insert (touch workers))))

(select-uupk :memory: #:where (list "pk <  ~a" 100000000000000)) ; timestamp PK will not larger than this number.
(select-uupk :memory: #:where (list "pk >= ~a and type = ~a" 100000000000000 'pk64:random))

(disconnect :memory:)
