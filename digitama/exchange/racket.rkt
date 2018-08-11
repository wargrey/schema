#lang typed/racket/base

(provide (all-defined-out))

(require "base.rkt")
(require "../misc.rkt")

(define dict->record : (-> Symbol HashTableTop (Listof Symbol) (Listof (-> Any)) (Listof Any))
  (lambda [func src fields mkdefval]
    (for/list ([field (in-list fields)]
               [mkval (in-list mkdefval)])
      (hash-ref src field (λ [] (check-default-value func field (mkval)))))))

(define make-dict : (All (a) (-> (Listof Symbol) (Listof a) Boolean (Immutable-HashTable Symbol a)))
  (lambda [fields fvalues skip?]
    (cond [(not skip?) (make-immutable-hasheq (map (inst cons Symbol a) fields fvalues))]
          [else (for/hasheq : (Immutable-HashTable Symbol a)
                  ([key (in-list fields)]
                   [val (in-list fvalues)]
                   #:when val)
                  (values key val))])))

(define table-dict? : (-> HashTableTop (Listof Symbol) Boolean)
  (lambda [src fields]
    (for/and : Boolean ([field (in-list fields)])
      (hash-has-key? src field))))

(define table->racket : Schema-Serialize
  (lambda [tablename fields fvalues]
    (make-immutable-hasheq
     (cons (cons '|| tablename)
           (map (inst cons Symbol Any)
                fields fvalues)))))

(define racket->table : Schema-Deserialize
  (lambda [tablename src fields mkdefval]
    (define maybe : Any (read (open-input-bytes src)))
    (cond [(and (hash? maybe)
                (table-dict? maybe (cons '|| fields))
                (eq? (hash-ref maybe '||) tablename))
           (for/list : (Listof Any) ([field (in-list fields)]
                                     [mkval (in-list mkdefval)])
             (hash-ref maybe field (λ [] (check-default-value tablename field (mkval)))))]
          [(hash? maybe)
           (schema-throw [exn:schema 'assertion `((struct . ,tablename) (got . ,src))]
                         'deserialize "not an accurate occurrence of ~a" tablename)]
          [else
           (schema-throw [exn:schema 'assertion `((struct . ,tablename) (got . ,src))]
                         'deserialize "not an occurrence of ~a" tablename)])))
