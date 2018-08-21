#lang typed/racket/base

(require "../../digitama/exchange/csv.rkt")

(require racket/logging)

(define examples : (Listof String)
  (list ",Year,Make,Model,,Description,Price,"
        "1997,Ford,E350,'ac, abs, moon',3000.00\r"
        "1999,Chevy,'Venture ''Extended Edition''','',4900.00\r\n\r"
        "1996,Jeep,Grand Cherokee,'MUST SELL!\r\nair, moon roof, loaded',4799.00\n\n"
        "\nwhatever"))

((inst with-logging-to-port Void)
 (current-error-port)
 (λ [] (for ([row (in-list examples)])
         (define /dev/csvin : Input-Port (open-input-string row))
         (define-values (fields maybe-char) (read-csv-row* /dev/csvin (read-char /dev/csvin) #false #\, #\' #false #false #false #false))
         (printf "~s~n ==> ~s~n #:n ~a~n #:next-leader? ~a~n~n"
                 row fields (if (pair? fields) (length fields) 0) maybe-char)))
 'debug)
