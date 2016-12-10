#lang typed/racket

(provide (all-defined-out))

(define dictionary : (HashTable Symbol String) (make-hasheq))

(define name->sql : (-> Symbol String)
  (lambda [name]
    (hash-ref! dictionary name
               (thunk (cond [(eq? name '||) (symbol->string (gensym '_))]
                            [else (let* ([patterns '([#rx"(.*)\\?$" "is_\\1"]
                                                     [#rx"[^A-Za-z0-9_]" "_"])]
                                         [name (regexp-replaces (symbol->string name) patterns)])
                                    (if (string? name) name (bytes->string/utf-8 name)))])))))

(define id->sql : (-> Identifier Syntax)
  (lambda [<id>]
    (datum->syntax <id> (name->sql (syntax-e <id>)))))
