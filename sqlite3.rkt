#lang typed/racket/base

(provide (all-defined-out))

(require typed/db)
(require digimon/system)

(require "digitama/normalize.rkt")

(define-type Pragma-Datum (U Real Boolean Symbol String))

(define sqlite-pragma : (->* (Connection Symbol) ((U Pragma-Datum Void) #:schema (Option Symbol)) (U Simple-Result Rows-Result))
  ;;; https://www.sqlite.org/pragma.html
  (lambda [sqlite name [argument (void)] #:schema [schema #false]]
    (define dbms : Symbol (dbsystem-name (connection-dbsystem sqlite)))
    (unless (eq? dbms 'sqlite3)
      (raise-unsupported-error 'sqlite3-pragma "not the target database system: ~a" dbms))
    (define pragma.sql : String (string-append "PRAGMA " (if schema (string-append (name->sql schema) ".") "") (name->sql name)))
    (cond [(void? argument) (query sqlite (string-append pragma.sql ";"))]
          [else (query sqlite (format (string-append pragma.sql " = ~a;")
                                      (cond [(boolean? argument) (if argument "true" "false")]
                                            [(symbol? argument) (name->sql argument)]
                                            [else argument])))])))
