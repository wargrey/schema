#lang typed/racket/base

(provide (all-defined-out))

(require racket/port)

(require xml/dom)

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
(define-type XLSX-Package (HashTable Bytes (U XML-Document (-> Input-Port))))

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
             ;;; There is no folder in Office Open XML Package
             ;;; The input port must be read here, or `unzip` will keep waiting...
             (with-handlers ([exn? (λ [[e : exn]] (port->bytes /dev/xlsxin))])
               (hash-set! package entry
                          (cond [(regexp-match? #px"[.][Xx][Mm][Ll]$" entry) (read-xml-document /dev/xlsxin)]
                                [else (let ([xlsx::// (string->symbol (format "xlsx:///~a" entry))]
                                            [raw (port->bytes /dev/xlsxin)])
                                        (procedure-rename (λ [] (open-input-bytes raw xlsx:://)) xlsx:://))])))))

    (unless (eq? /dev/zipin /dev/stdin)
      (close-input-port /dev/zipin))
    
    package))
