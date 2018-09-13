#lang typed/racket/base

(provide (all-defined-out) CSV-Field CSV-Row CSV-Row*)
(provide (all-from-out "digitama/exchange/csv/dialect.rkt"))

(require "digitama/exchange/csv/reader.rkt")
(require "digitama/exchange/csv/readline.rkt")
(require "digitama/exchange/csv/string.rkt")
(require "digitama/exchange/csv/dialect.rkt")
(require "digitama/exchange/csv/misc.rkt")

;;; Note
; For a 60MB CSV:
; 1. The performance of `read-csv` and `sequence->list . in-csv` can be considered identical (e.g. the difference is within [-200ms, +200ms])
; 2. In contrast to `csv-readline`, reading all chars before parsing is a little slower, anti-intuition!

(define read-csv : (-> CSV-StdIn Positive-Integer Boolean [#:dialect (Option CSV-Dialect)]
                       [#:strict? Boolean] [#:skip-empty-line? Boolean]
                       (Listof (Vectorof CSV-Field)))
  (lambda [/dev/stdin col skip-header? #:dialect [maybe-dialect #false] #:strict? [strict? #false] #:skip-empty-line? [trim-line? #true]]
    (parameterize ([current-custodian (make-custodian)])
      (dynamic-wind
       (λ [] (void))
       (λ [] (let ([/dev/csvin : (U Input-Port String) (csv-input-source /dev/stdin #false)]
                   [dialect : CSV-Dialect (or maybe-dialect csv::rfc)]
                   [n : Positive-Index (assert col index?)])
               (reverse (cond [(string? /dev/csvin) (csv-split/reverse /dev/csvin n dialect skip-header? strict? trim-line?)]
                              [(port-counts-lines? /dev/csvin) (csv-read/reverse /dev/csvin n dialect skip-header? strict? trim-line?)]
                              [else (csv-readline/reverse /dev/csvin n dialect skip-header? strict? trim-line?)]))))
       (λ [] (custodian-shutdown-all (current-custodian)))))))

(define read-csv* : (-> CSV-StdIn Boolean [#:dialect (Option CSV-Dialect)]
                        [#:strict? Boolean] [#:skip-empty-line? Boolean]
                        (Listof (Pairof CSV-Field (Listof CSV-Field))))
  (lambda [/dev/stdin skip-header? #:dialect [maybe-dialect #false] #:strict? [strict? #false] #:skip-empty-line? [trim-line? #true]]
    (parameterize ([current-custodian (make-custodian)])
      (dynamic-wind
       (λ [] (void))
       (λ [] (let ([/dev/csvin : (U Input-Port String) (csv-input-source /dev/stdin #false)]
                   [dialect : CSV-Dialect (or maybe-dialect csv::rfc)])
               (reverse (cond [(string? /dev/csvin) (csv-split*/reverse /dev/csvin dialect skip-header? strict? trim-line?)]
                              [(port-counts-lines? /dev/csvin) (csv-read*/reverse /dev/csvin dialect skip-header? strict? trim-line?)]
                              [else (csv-readline*/reverse /dev/csvin dialect skip-header? strict? trim-line?)]))))
       (λ [] (custodian-shutdown-all (current-custodian)))))))

(define in-csv : (-> CSV-StdIn Positive-Integer Boolean [#:dialect (Option CSV-Dialect)]
                     [#:strict? Boolean] [#:skip-empty-line? Boolean]
                     (Sequenceof (Vectorof CSV-Field)))
  (lambda [/dev/stdin col skip-header? #:dialect [maybe-dialect #false] #:strict? [strict? #false] #:skip-empty-line? [trim-line? #true]]
    (define /dev/csvin : (U Input-Port String) (csv-input-source /dev/stdin #true))
    (define dialect : CSV-Dialect (or maybe-dialect csv::rfc))
    (define n : Positive-Index (assert col index?))
    (cond [(string? /dev/csvin) (in-csv-string /dev/csvin n dialect skip-header? strict? trim-line?)]
          [(port-counts-lines? /dev/csvin) (in-csv-port /dev/csvin n dialect skip-header? strict? trim-line?)]
          [else (in-csv-line-port /dev/csvin n dialect skip-header? strict? trim-line?)])))

(define in-csv* : (-> CSV-StdIn Boolean [#:dialect (Option CSV-Dialect)]
                      [#:strict? Boolean] [#:skip-empty-line? Boolean]
                      (Sequenceof (Pairof CSV-Field (Listof CSV-Field))))
  (lambda [/dev/stdin skip-header? #:dialect [maybe-dialect #false] #:strict? [strict? #false] #:skip-empty-line? [trim-line? #true]]
    (define /dev/csvin : (U Input-Port String) (csv-input-source /dev/stdin #true))
    (define dialect : CSV-Dialect (or maybe-dialect csv::rfc))
    (cond [(string? /dev/csvin) (in-csv-string* /dev/csvin dialect skip-header? strict? trim-line?)]
          [(port-counts-lines? /dev/csvin) (in-csv-port* /dev/csvin dialect skip-header? strict? trim-line?)]
          [else (in-csv-line-port* /dev/csvin dialect skip-header? strict? trim-line?)])))

(define csv-empty-line? : (-> (Listof CSV-Field) Boolean)
  (lambda [row]
    (eq? row empty-row)))
