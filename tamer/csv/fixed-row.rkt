#lang typed/racket/base

(require "../../csv.rkt")

(require racket/logging)

(port-count-lines-enabled #true)

(define csv::test : CSV-Dialect (make-csv-dialect #:delimiter #\, #:quote-char #\' #:skip-empty-line? #false))

(define examples : (Listof String)
  (list "Year,Make,Model,Price"
        "1997,Ford,E350,'ac, abs, moon',3000.00"
        "1996,Jeep,Grand Cherokee,'Venture ''Extended Edition''',4799.00,'MUST SELL!\r\nair, moon roof, loaded'"))

((inst with-logging-to-port (Listof (Vectorof CSV-Field)))
 (current-error-port)
 (λ [] (for/list : (Listof (Vectorof CSV-Field)) ([row (in-list examples)])
         (define /dev/csvin : Input-Port (open-input-string row))
         (car (read-csv /dev/csvin 5 #false #:dialect csv::test))))
 'debug)
