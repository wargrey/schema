#lang typed/racket/base

(provide (all-defined-out))

(require "misc.rkt")

(require racket/path)

(define-type CSV-Progress-Handler (-> Symbol Natural Void))
(define-type Maybe-CSV-Progress-Handler (U CSV-Progress-Handler False))

(define csv-resolve-progress-topic : (-> (U Input-Port String) Symbol)
  (lambda [/dev/csvin]
    (define name : Any (object-name /dev/csvin))
    (cond [(symbol? name) name]
          [(path? name) (string->symbol (format "~a" (file-name-from-path name)))]
          [(string? name) (string->symbol name)]
          [else 'topic:csv:progress])))

(define default-csv-progress-handler : (Parameterof Maybe-CSV-Progress-Handler) (make-parameter #false))

(define default-csv-progress-topic-resolver : (Parameterof (Option (-> (U Input-Port String) Symbol)) (-> (U Input-Port String) Symbol))
  (make-parameter csv-resolve-progress-topic
                  (λ [[resolver : (Option (-> (U Input-Port String) Symbol))]]
                    (or resolver csv-resolve-progress-topic))))

(define csv-report-position : (-> Natural CSV-Progress-Handler Symbol Void)
  (lambda [position maybe-progress-handler topic]
    (maybe-progress-handler topic position)))

(define csv-report-progress : (-> Input-Port Maybe-CSV-Progress-Handler Symbol Void)
  (lambda [/dev/csvin maybe-progress-handler topic]
    (unless (not maybe-progress-handler)
      (define-values (line column position+1) (port-next-location /dev/csvin))
      (unless (not position+1)
        (csv-report-position (- position+1 1)
                             maybe-progress-handler topic)))))

(define csv-report-position* : (-> Natural Maybe-CSV-Progress-Handler Symbol Void)
  (lambda [position maybe-progress-handler topic]
    (unless (not maybe-progress-handler)
      (csv-report-position position maybe-progress-handler topic))))

(define csv-report-final-progress : (-> Input-Port Maybe-CSV-Progress-Handler Symbol Boolean Void)
  (lambda [/dev/csvin maybe-progress-handler symbol close?]
    (csv-report-progress /dev/csvin maybe-progress-handler symbol)
    (unless (not close?)
      (csv-close-input-port /dev/csvin))))
