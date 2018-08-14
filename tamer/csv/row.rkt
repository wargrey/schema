#lang typed/racket/base

(require "../../digitama/exchange/csv.rkt")

(require racket/logging)

(port-count-lines-enabled #true)

(define wikipedia-examples : (Listof String)
  (list ",Year,Make,Model,,Description,Price,"
        "1997,Ford,E350,'ac, abs, moon',3000.00\r"
        "2000,Mercury,Cougar,2.38\r\n"
        "1999,Chevy,'Venture ''Extended Edition''','',4900.00\r\n\r"
        "1996,Jeep,Grand Cherokee,'MUST SELL!
air, moon roof, loaded',4799.00\n\n"
        "\nwhatever"
        "\r"))

((inst with-logging-to-port Void)
 (current-error-port)
 (λ [] (for ([row (in-list wikipedia-examples)])
         (define /dev/csvin : Input-Port (open-input-string row))
         (define-values (fields more?) (read-csv-row /dev/csvin #\, #\'))
         (printf "~s: ~s[~a]~n" row fields more?)))
 'debug)
