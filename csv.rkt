#lang typed/racket/base

(provide (all-defined-out) CSV-Field CSV-Row CSV-Row*)
(provide default-csv-progress-handler default-csv-progress-topic-resolver csv-resolve-progress-topic)
(provide (all-from-out "digitama/exchange/csv/dialect.rkt"))

(require "digitama/exchange/csv/reader/port.rkt")
(require "digitama/exchange/csv/reader/line.rkt")
(require "digitama/exchange/csv/reader/string.rkt")
(require "digitama/exchange/csv/reader/misc.rkt")
(require "digitama/exchange/csv/reader/progress.rkt")
(require "digitama/exchange/csv/dialect.rkt")

;;; Note
; For a 70MB CSV:
; 1. The performance of `read-csv` and `sequence->list . in-csv` can be considered identical (e.g. the oscillation is less than 200ms)
; 2. In contrast to `csv-readline`, reading all chars before parsing is a little slower, anti-intuition!
; 3. Deal with bytes directly does not perform well, it is slower than dealing with string.

(define read-csv : (-> CSV-Stdin Positive-Integer Boolean [#:dialect (Option CSV-Dialect)]
                       [#:strict? Boolean] [#:skip-empty-line? Boolean] [#:progress-topic (Option Symbol)]
                       (Listof (Vectorof CSV-Field)))
  (lambda [/dev/stdin col skip-header? #:dialect [maybe-dialect #false]
                      #:strict? [strict? #false] #:skip-empty-line? [trim-line? #true]
                      #:progress-topic [maybe-topic #false]]
    (parameterize ([current-custodian (make-custodian)])
      (dynamic-wind
       (λ [] (void))
       (λ [] (let ([/dev/csvin : (U Input-Port String) (csv-input-source /dev/stdin #false)]
                   [dialect : CSV-Dialect (or maybe-dialect csv::rfc)]
                   [n : Positive-Index (assert col index?)])
               (reverse (cond [(string? /dev/csvin) (csv-split/reverse /dev/csvin n dialect skip-header? strict? trim-line? maybe-topic)]
                              [(port-counts-lines? /dev/csvin) (csv-read/reverse /dev/csvin n dialect skip-header? strict? trim-line? maybe-topic)]
                              [else (csv-readline/reverse /dev/csvin n dialect skip-header? strict? trim-line? maybe-topic)]))))
       (λ [] (custodian-shutdown-all (current-custodian)))))))

(define read-csv* : (-> CSV-Stdin Boolean [#:dialect (Option CSV-Dialect)]
                        [#:strict? Boolean] [#:skip-empty-line? Boolean] [#:progress-topic (Option Symbol)]
                        (Listof (Pairof CSV-Field (Listof CSV-Field))))
  (lambda [/dev/stdin skip-header? #:dialect [maybe-dialect #false]
                      #:strict? [strict? #false] #:skip-empty-line? [trim-line? #true]
                      #:progress-topic [maybe-topic #false]]
    (parameterize ([current-custodian (make-custodian)])
      (dynamic-wind
       (λ [] (void))
       (λ [] (let ([/dev/csvin : (U Input-Port String) (csv-input-source /dev/stdin #false)]
                   [dialect : CSV-Dialect (or maybe-dialect csv::rfc)])
               (reverse (cond [(string? /dev/csvin) (csv-split*/reverse /dev/csvin dialect skip-header? strict? trim-line? maybe-topic)]
                              [(port-counts-lines? /dev/csvin) (csv-read*/reverse /dev/csvin dialect skip-header? strict? trim-line? maybe-topic)]
                              [else (csv-readline*/reverse /dev/csvin dialect skip-header? strict? trim-line? maybe-topic)]))))
       (λ [] (custodian-shutdown-all (current-custodian)))))))

(define in-csv : (-> CSV-Stdin Positive-Integer Boolean [#:dialect (Option CSV-Dialect)]
                     [#:strict? Boolean] [#:skip-empty-line? Boolean] [#:progress-topic (Option Symbol)]
                     (Sequenceof (Vectorof CSV-Field)))
  (lambda [/dev/stdin col skip-header? #:dialect [maybe-dialect #false]
                      #:strict? [strict? #false] #:skip-empty-line? [trim-line? #true]
                      #:progress-topic [maybe-topic #false]]
    (define /dev/csvin : (U Input-Port String) (csv-input-source /dev/stdin #true))
    (define dialect : CSV-Dialect (or maybe-dialect csv::rfc))
    (define n : Positive-Index (assert col index?))
    (cond [(string? /dev/csvin) (in-csv-string /dev/csvin n dialect skip-header? strict? trim-line? maybe-topic)]
          [(port-counts-lines? /dev/csvin) (in-csv-port /dev/csvin n dialect skip-header? strict? trim-line? maybe-topic)]
          [else (in-csv-line-port /dev/csvin n dialect skip-header? strict? trim-line? maybe-topic)])))

(define in-csv* : (-> CSV-Stdin Boolean [#:dialect (Option CSV-Dialect)]
                      [#:strict? Boolean] [#:skip-empty-line? Boolean] [#:progress-topic (Option Symbol)]
                      (Sequenceof (Pairof CSV-Field (Listof CSV-Field))))
  (lambda [/dev/stdin skip-header? #:dialect [maybe-dialect #false]
                      #:strict? [strict? #false] #:skip-empty-line? [trim-line? #true]
                      #:progress-topic [maybe-topic #false]]
    (define /dev/csvin : (U Input-Port String) (csv-input-source /dev/stdin #true))
    (define dialect : CSV-Dialect (or maybe-dialect csv::rfc))
    (cond [(string? /dev/csvin) (in-csv-string* /dev/csvin dialect skip-header? strict? trim-line? maybe-topic)]
          [(port-counts-lines? /dev/csvin) (in-csv-port* /dev/csvin dialect skip-header? strict? trim-line? maybe-topic)]
          [else (in-csv-line-port* /dev/csvin dialect skip-header? strict? trim-line? maybe-topic)])))

(define csv-empty-line? : (-> (Listof CSV-Field) Boolean)
  (lambda [row]
    (eq? row empty-row)))

(define csv-empty-line*? : (-> (Listof CSV-Field) Boolean)
  (lambda [row]
    (eq? row empty-row*)))

