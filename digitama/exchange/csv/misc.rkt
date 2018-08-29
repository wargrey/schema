#lang typed/racket/base

(provide (all-defined-out))

(define-type CSV-Field String)
(define-type CSV-Row* (Pairof CSV-Field (Listof CSV-Field)))
(define-type CSV-StdIn (U Input-Port Bytes Path-String))

(define empty-field : CSV-Field "")
(define empty-row : (Vectorof CSV-Field) (vector))
(define empty-row* : CSV-Row* (list empty-field))

(struct CSV-Dialect
  ([delimiter : Char]
   [quote-char : (Option Char)]
   [escape-char : (Option Char)]
   [comment-char : (Option Char)]
   [skip-empty-line? : Boolean]
   [skip-leading-space? : Boolean]
   [skip-trailing-space? : Boolean])
  #:transparent)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define char-oct-digit? : (-> Any Boolean : #:+ Char)
  (lambda [ch]
    (and (char? ch)
         (char<=? #\0 ch #\7))))

(define char-hex-digit? : (-> Any Boolean : #:+ Char)
  (lambda [ch]
    (and (char? ch)
         (or (char-numeric? ch)
             (char-ci<=? #\a ch #\f)))))

(define unicode->char : (-> Fixnum Char)
  (lambda [n]
    (cond [(> n #x10FFFF) #\uFFFD] ; #\nul and max unicode
          [(<= #xD800 n #xDFFF) #\uFFFD] ; surrogate
          [else (integer->char n)])))

(define char->decimal : (-> Char Fixnum)
  (lambda [hexch]
    (cond [(char<=? #\a hexch) (- (char->integer hexch) #x57)]
          [(char<=? #\A hexch) (- (char->integer hexch) #x37)]
          [else (- (char->integer hexch) #x30)])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-topic : Symbol 'exn:csv:syntax)
(define csv-ports : (HashTable Input-Port Boolean) (make-hasheq))

(define csv-input-port : (-> CSV-StdIn Boolean Boolean Input-Port)
  (lambda [/dev/stdin skipheader? trace?]
    (define /dev/csvin : Input-Port
      (cond [(input-port? /dev/stdin) /dev/stdin]
            [(path? /dev/stdin) (open-input-file /dev/stdin)]
            [(regexp-match? #px"\\.csv$" /dev/stdin) (open-input-file (format "~a" /dev/stdin))]
            [(bytes? /dev/stdin) (open-input-bytes /dev/stdin '/dev/csvin/bytes)]
            [else (open-input-string /dev/stdin '/dev/csvin/string)]))

    (unless (not skipheader?)
      (read-line /dev/csvin))
    
    (when (and trace?)
      (unless (eq? /dev/csvin /dev/stdin)
        (hash-set! csv-ports /dev/csvin #true)))
    
    /dev/csvin))

(define csv-close-input-port : (-> Input-Port Void)
  (lambda [/dev/csvin]
    (when (hash-has-key? csv-ports /dev/csvin)
      (hash-remove! csv-ports /dev/csvin)
      (close-input-port /dev/csvin))))

(define csv-log-escape-error : (-> Input-Port Void)
  (lambda [/dev/csvin]
    (csv-log-syntax-error /dev/csvin 'warning #false "invalid escape sequence")))

(define csv-log-length-error : (-> Input-Port Integer Integer (U (Vectorof CSV-Field) (Listof CSV-Field)) Boolean Void)
  (lambda [/dev/csvin expected given in strict?]
    (csv-log-syntax-error /dev/csvin 'error strict?
                          (format "field length mismatch: expected ~a, given ~a ~a ~s"
                            expected given (if (< given expected) 'in 'with) in))))

(define csv-log-eof-error : (-> Input-Port Boolean Void)
  (lambda [/dev/csvin strict?]
    (csv-log-syntax-error /dev/csvin 'warning strict? "unexpected eof of file")))

(define csv-log-out-quotes-error : (-> Input-Port Boolean Symbol Void)
  (lambda [/dev/csvin strict? position]
    (csv-log-syntax-error /dev/csvin 'warning strict?
                          (format "ignored non-whitespace chars ~a quote char" position))))

(define csv-log-if-invalid : (-> Input-Port Boolean Boolean Void)
  (lambda [/dev/csvin valid? strict?]
    (when (not valid?)
      (csv-log-out-quotes-error /dev/csvin strict? 'after))))

(define csv-log-syntax-error : (-> Input-Port Log-Level Boolean String Void)
  (lambda [/dev/csvin level strict? brief]
    (define-values (line column position) (port-next-location /dev/csvin))
    (define message : String
      (cond [(and line column) (format "~a:~a:~a: ~a" (object-name /dev/csvin) line column brief)]
            [else (format "~a: ~a" (object-name /dev/csvin) brief)]))
    (log-message (current-logger) level csv-topic message #false)
    (unless (not strict?)
      (csv-close-input-port /dev/csvin)
      (raise-user-error 'csv "~a" message))))
