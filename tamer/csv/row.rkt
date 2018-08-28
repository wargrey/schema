#lang typed/racket/base

(require "../../digitama/exchange/csv/reader.rkt")
(require "../../digitama/exchange/csv/readline.rkt")

(require racket/logging)

(define examples : (Listof String)
  (list ",Year,Make,Model,,Description,Price,"
        "1997,Ford,E350,'ac, abs, moon',3000.00\r"
        "1999,Chevy,'Venture ''Extended Edition''','',4900.00\r\n\r"
        "1996,Jeep,Grand Cherokee,'MUST SELL!\n\rair, moon roof, loaded',4799.00\n\n"
        "\nwhatever"))

(displayln (object-name read-csv-row*))
((inst with-logging-to-port Void)
 (current-error-port)
 (λ [] (for ([row (in-list examples)])
         (define /dev/csvin : Input-Port (open-input-string row))
         (define-values (fields maybe-char) (read-csv-row* /dev/csvin (read-char /dev/csvin) #false #\, #\' #false #false #false #false #false))
         (printf "~s ==> ~s~n #:n ~a #:next? ~a~n~n"
                 row fields
                 (if (pair? fields) (length fields) 0)
                 (or maybe-char eof))))
 'debug)

(displayln '===================================================================)
(displayln (object-name line->csv-row*))
((inst with-logging-to-port Void)
 (current-error-port)
 (λ [] (for ([row (in-list examples)])
         (define /dev/csvin : Input-Port (open-input-string row))
         (define maybe-line : (U String EOF) (read-line /dev/csvin 'any))
         (when (string? maybe-line)
           (define fields (line->csv-row* /dev/csvin maybe-line #false #\, #\' #false #false #false #false #false))
           (printf "~s ==> ~s~n #:n ~a #:next-leader? ~a~n~n"
                   row fields (if (pair? fields) (length fields) 0) (read-char /dev/csvin)))))
 'debug)

