#lang typed/racket/base

(provide (all-defined-out))

(require typed/db/base)

(require "digitama/message.rkt")

(struct: msg:schema : Schema-Message ([level : Log-Level] [brief : String] [urgent : Any] [topic : Symbol]))
(struct: msg:schema:table : Schema-Table-Message msg:schema ([manipulation : Symbol] [raw : Bytes]))
(struct: msg:schema:error : Schema-Error-Message msg:schema ([info :(Listof (Pairof Symbol Any))]))

(define make-schema-message : (->* ((U Struct-TypeTop Symbol) Symbol)
                                   (Bytes #:urgent Any #:brief (U False String (Pairof String (Listof Any))))
                                   Schema-Message)
  (lambda [table manipulation [raw #""] #:urgent [urgent #false] #:brief [messages ""]]
    (cond [(exn? urgent) (exn->schema-message urgent table manipulation)]
          [else (let ([brief : String (if messages (rest->message messages) (format "(~a ~a ~s)" manipulation table urgent))])
                  (msg:schema:table 'info brief urgent (if (symbol? table) table (struct-name table)) manipulation raw))])))

(define exn->schema-message : (->* (exn) ((U Struct-TypeTop Symbol False) (Option Symbol) #:level Log-Level) Schema-Error-Message)
  (lambda [e [table #false] [manipulation #false] #:level [level 'error]]
    (define smart-table : Any
      (cond [(not table) (and (exn:fail:sql? e) (exn:sql-info-ref e 'struct))]
            [(symbol? table) table]
            [else (struct-name table)]))
    (msg:schema:error level (exn-message e) manipulation (if (symbol? smart-table) smart-table (struct-name e)) (exn->info e))))

(define schema-message-replace-urgent : (-> Schema-Message Any Schema-Message)
  (lambda [message urgent]
    (cond [(Schema-Table-Message? message)
           (struct-copy msg:schema:table message [urgent #:parent msg:schema urgent])]
          [(Schema-Error-Message? message)
           (struct-copy msg:schema:error message [urgent #:parent msg:schema urgent])]
          [else (struct-copy msg:schema message [urgent urgent])])))

(define schema-log-message : (-> Schema-Message [#:logger Logger] [#:alter-topic (U Symbol Struct-TypeTop False)] Void)
  (lambda [self #:logger [logger (current-logger)] #:alter-topic [topic #false]]
    (log-message logger
                 (msg:schema-level self)
                 (cond [(not topic) (msg:schema-topic self)]
                       [(symbol? topic) topic]
                       [else (struct-name topic)])
                 (msg:schema-brief self)
                 self)))

(define schema-log-message* : (->* ((U Struct-TypeTop Symbol) Symbol)
                                   (Bytes #:urgent Any #:brief (U False String (Pairof String (Listof Any)))
                                          #:logger Logger #:alter-topic (U Symbol Struct-TypeTop False))
                                   Void)
  (lambda [table manipulation [raw #""]
                 #:urgent [urgent #false] #:brief [messages ""]
                 #:logger [logger (current-logger)] #:alter-topic [topic #false]]
    (define self : Schema-Message (make-schema-message table manipulation raw #:urgent urgent #:brief messages))
    (log-message logger
                 (msg:schema-level self)
                 (cond [(not topic) (msg:schema-topic self)]
                       [(symbol? topic) topic]
                       [else (struct-name topic)])
                 (msg:schema-brief self)
                 self)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define exn:sql-info-ref : (->* ((U exn:fail:sql Schema-Error-Message) Symbol) ((-> Any Any)) Any)
  (lambda [e key [-> values]]
    (define info : (Listof (Pairof Any Any))
      (cond [(exn:fail:sql? e) (exn:fail:sql-info e)]
            [else (msg:schema:error-info e)]))
    (define pinfo : (Option (Pairof Any Any)) (assq key info))
    (and pinfo (-> (cdr pinfo)))))
