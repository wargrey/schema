#lang typed/racket/base

(provide (all-defined-out) CSV-Field CSV-Row CSV-Row*)
(provide (all-from-out "digitama/exchange/csv/dialect.rkt"))

(require "digitama/exchange/csv/reader.rkt")
(require "digitama/exchange/csv/readline.rkt")
(require "digitama/exchange/csv/dialect.rkt")
(require "digitama/exchange/csv/misc.rkt")

;;; Note
; The performance of `read-csv` and `sequence->list . in-csv` can be considered identity
;   (e.g. for a 60MB CSV, the difference is within [-200ms, +200ms]).

(define read-csv : (-> CSV-StdIn Positive-Integer Boolean [#:dialect (Option CSV-Dialect)]
                       [#:strict? Boolean] [#:skip-empty-line? Boolean]
                       (Listof (Vectorof CSV-Field)))
  (lambda [/dev/stdin n skip-header? #:dialect [dialect #false] #:strict? [strict? #false] #:skip-empty-line? [trim-line? #true]]
    (parameterize ([current-custodian (make-custodian)])
      (dynamic-wind
       (λ [] (void))
       (λ [] (let ([/dev/csvin (csv-input-port /dev/stdin skip-header? #false)])
               (reverse (if (not (port-counts-lines? /dev/csvin))
                            (csv-readline/reverse /dev/csvin (assert n index?) (or dialect csv::rfc) strict? trim-line?)
                            (csv-read/reverse /dev/csvin (assert n index?) (or dialect csv::rfc) skip-header? strict? trim-line?)))))
       (λ [] (custodian-shutdown-all (current-custodian)))))))

(define read-csv* : (-> CSV-StdIn Boolean [#:dialect (Option CSV-Dialect)]
                        [#:strict? Boolean] [#:skip-empty-line? Boolean]
                        (Listof (Pairof CSV-Field (Listof CSV-Field))))
  (lambda [/dev/stdin skip-header? #:dialect [dialect #false] #:strict? [strict? #false] #:skip-empty-line? [trim-line? #true]]
    (parameterize ([current-custodian (make-custodian)])
      (dynamic-wind
       (λ [] (void))
       (λ [] (let ([/dev/csvin (csv-input-port /dev/stdin skip-header? #false)])
               (reverse (if (not (port-counts-lines? /dev/csvin))
                            (csv-readline*/reverse /dev/csvin (or dialect csv::rfc) strict? trim-line?)
                            (csv-read*/reverse /dev/csvin (or dialect csv::rfc) skip-header? strict? trim-line?)))))
       (λ [] (custodian-shutdown-all (current-custodian)))))))

(define in-csv : (-> CSV-StdIn Positive-Integer Boolean [#:dialect (Option CSV-Dialect)]
                     [#:strict? Boolean] [#:skip-empty-line? Boolean]
                     (Sequenceof (Vectorof CSV-Field)))
  (lambda [/dev/stdin n skip-header? #:dialect [dialect #false] #:strict? [strict? #false] #:skip-empty-line? [trim-line? #true]]
    (define /dev/csvin : Input-Port (csv-input-port /dev/stdin skip-header? #true))
    (if (not (port-counts-lines? /dev/csvin))
        (in-csv-line-port /dev/csvin (assert n index?) (or dialect csv::rfc) strict? trim-line?)
        (in-csv-port /dev/csvin (assert n index?) (or dialect csv::rfc) skip-header? strict? trim-line?))))

(define in-csv* : (-> CSV-StdIn Boolean [#:dialect (Option CSV-Dialect)]
                      [#:strict? Boolean] [#:skip-empty-line? Boolean]
                      (Sequenceof (Pairof CSV-Field (Listof CSV-Field))))
  (lambda [/dev/stdin skip-header? #:dialect [dialect #false] #:strict? [strict? #false] #:skip-empty-line? [trim-line? #true]]
    (define /dev/csvin : Input-Port (csv-input-port /dev/stdin skip-header? #true))
    (if (not (port-counts-lines? /dev/csvin))
        (in-csv-line-port* /dev/csvin (or dialect csv::rfc) strict? trim-line?)
        (in-csv-port* /dev/csvin (or dialect csv::rfc) skip-header? strict? trim-line?))))

(define csv-empty-line? : (-> (Listof CSV-Field) Boolean)
  (lambda [row]
    (eq? row empty-row)))
