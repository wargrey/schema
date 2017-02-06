#lang digimon

(require "../main.rkt")

(define-schema UUPKTamer
  (define-table uupk #:as UUPK #:with pk
    ([pk     : Integer       #:default (pk64:timestamp)]
     [type   : Symbol        #:not-null])))

(define :memory: : Connection (sqlite3-connect #:database 'memory))

(define plan : Index 512)
(define tspks : (Listof UUPK) (time (build-list plan (λ _ (make-uupk #:pk (pk64:timestamp) #:type 'timestamp)))))
(define rndpks : (Listof UUPK) (time (build-list plan (λ _ (make-uupk #:pk (pk64:random) #:type 'random)))))

(define do-insert : (-> UUPK Void)
  (lambda [record]
    (with-handlers ([exn:fail:sql? (λ [[e : exn:fail:sql]] (pretty-write (cons record (exn:fail:sql-info e)) /dev/stderr))])
      (insert-uupk :memory: record))))

(create-uupk :memory:)
(for-each do-insert tspks)
(for-each do-insert rndpks)
(select-uupk :memory:)
(disconnect :memory:)
