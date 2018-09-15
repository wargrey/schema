#lang typed/racket/base

(provide (all-defined-out))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define xlsx-topic : Symbol 'exn:xlsx:syntax)
(define xlsx-ports : (HashTable Input-Port Boolean) (make-hasheq))

(define csv-close-input-port : (-> Input-Port Void)
  (lambda [/dev/xlsin]
    (when (hash-has-key? xlsx-ports /dev/xlsin)
      (hash-remove! xlsx-ports /dev/xlsin)
      (close-input-port /dev/xlsin))))

(define csv-log-syntax-error : (-> Input-Port Log-Level String Void)
  (lambda [/dev/xlsin level brief]
    (define-values (line column position) (port-next-location /dev/xlsin))
    (define message : String
      (cond [(and line column) (format "~a:~a:~a: ~a" (object-name /dev/xlsin) line column brief)]
            [else (format "~a: ~a" (object-name /dev/xlsin) brief)]))
    (log-message (current-logger) level xlsx-topic message #false)
    (csv-close-input-port /dev/xlsin)
    (raise-user-error 'csv "~a" message)))
