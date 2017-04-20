#lang typed/racket

(require "../main.rkt")

; WARNING: This kind of tasks defeat futures!

(define-schema UUPKTamer
  (define-table uupk #:as UUPK #:with pk
    ([pk     : Integer                      #:not-null]
     [rep    : String                       #:not-null]
     [type   : Symbol                       #:not-null]
     [urgent : (Pairof Integer Integer)     #:not-null])))

(define :memory: : Connection (sqlite3-connect #:database 'memory))

(define make-job : (-> (-> Integer) Index (-> (Listof UUPK)))
  (lambda [pk64 fid]
    (thunk (let ([pks (time (build-list (processor-count) (λ _ (pk64))))]
                 [type (assert (object-name pk64) symbol?)])
             (for/list : (Listof UUPK) ([pk (in-list pks)] [seq (in-naturals)])
               (make-uupk #:pk pk
                          #:type type
                          #:rep (number->string pk 16)
                          #:urgent (cons fid seq)))))))

(define do-insert : (-> UUPK Void)
  (lambda [record]
    (with-handlers ([exn:fail:sql? (λ [[e : exn:fail:sql]] (pretty-write (cons record (exn:fail:sql-info e)) (current-error-port)))])
      (insert-uupk :memory: record))))

(create-uupk :memory:)

(define pks : (Listof (Listof (Futureof (Listof UUPK))))
  (for/list ([pk64 (in-list (list pk64:timestamp pk64:random))])
    (build-list (* (processor-count) 2) (λ [[fid : Index]] (future (make-job pk64 fid))))))

(for ([jobs : (Listof (Futureof (Listof UUPK))) (in-list pks)])
  (for ([workers : (Futureof (Listof UUPK)) (in-list jobs)])
    (map do-insert (touch workers))))

(select-uupk :memory: #:where (list "pk <  ~a" 5000000000000000000))
(select-uupk :memory: #:where (list "pk >= ~a and type = ~a" 5000000000000000000 'pk64:random))

(disconnect :memory:)
