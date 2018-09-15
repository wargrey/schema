#lang typed/racket/base

(provide (all-defined-out))

(require racket/port)

(require typed/racket/unsafe)

(unsafe-require/typed
 racket/base
 [procedure-rename (All (f) (-> f Symbol f))])

(unsafe-require/typed
 file/unzip
 [unzip (->* ((U Input-Port Path-String))
             ((->* (Bytes Boolean Input-Port) ((Option Natural)) Any)
              #:preserve-timestamps? Boolean
              #:utc-timestamps? Boolean)
             Void)])

(define-type XLSX-StdIn (U String Path Bytes))
(define-type XLSX-Package (HashTable Bytes (-> Input-Port)))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define xlsx-input-package : (-> XLSX-StdIn XLSX-Package)
  (lambda [/dev/stdin]
    (define package : XLSX-Package (make-hash))
    (define /dev/zipin : Input-Port
      (cond [(input-port? /dev/stdin) /dev/stdin]
            [(bytes? /dev/stdin) (open-input-file (bytes->path /dev/stdin))]
            [else (open-input-file /dev/stdin)]))

    (unzip /dev/zipin
           (λ [[entry : Bytes] [dir? : Boolean] [/dev/xlsxin : Input-Port] [timestamp : (Option Natural) #false]] : Any
             (when (not dir?)
               (define xlsx::// : Symbol (string->symbol (format "xlsx:///~a" entry)))
               (hash-set! package entry
                          ;;; the port must be read here, or `unzip` will keep waiting...
                          (procedure-rename (λ [] (open-input-bytes (port->bytes /dev/xlsxin) xlsx:://))
                                            xlsx:://)))))

    (unless (eq? /dev/zipin /dev/stdin)
      (close-input-port /dev/zipin))
    
    package))
