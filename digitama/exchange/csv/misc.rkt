#lang typed/racket/base

(provide (all-defined-out))

(require racket/vector)

(define-type CSV-Field String)
(define-type CSV-Row (Vectorof CSV-Field))
(define-type CSV-Row* (Pairof CSV-Field (Listof CSV-Field)))
(define-type CSV-StdIn (U Input-Port Bytes Path-String))

(define empty-field : CSV-Field "")
(define empty-row : CSV-Row (vector))
(define empty-row* : CSV-Row* (list empty-field))

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
    (cond [(char<=? hexch #\9) (- (char->integer hexch) #x30)]
          [(char<=? #\a hexch) (- (char->integer hexch) #x57)]
          [else #;[#\A, #\F]   (- (char->integer hexch) #x37)])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-topic : Symbol 'exn:csv:syntax)
(define csv-bytes-name : Symbol '/dev/csvin/bytes)
(define csv-string-name : Symbol '/dev/csvin/string)
(define csv-ports : (HashTable Input-Port Boolean) (make-hasheq))

(define csv-input-name : (-> CSV-StdIn Any)
  (lambda [/dev/stdin]
    (cond [(input-port? /dev/stdin) (object-name /dev/stdin)]
          [(or (path? /dev/stdin) (regexp-match? #px"\\.csv$" /dev/stdin)) /dev/stdin]
          [(bytes? /dev/stdin) csv-bytes-name]
          [else csv-string-name])))

(define csv-input-port : (-> CSV-StdIn Boolean Boolean Input-Port)
  (lambda [/dev/stdin skipheader? trace?]
    (define /dev/csvin : Input-Port
      (cond [(input-port? /dev/stdin) /dev/stdin]
            [(path? /dev/stdin) (open-input-file /dev/stdin)]
            [(regexp-match? #px"\\.csv$" /dev/stdin) (open-input-file (format "~a" /dev/stdin))]
            [(bytes? /dev/stdin) (open-input-bytes /dev/stdin csv-bytes-name)]
            [else (open-input-string /dev/stdin csv-bytes-name)]))

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

(define csv-log-escape-error : (case-> [Input-Port -> Void]
                                       [CSV-StdIn String Nonnegative-Fixnum -> Void]
                                       [String Index Index Nonnegative-Fixnum -> Void])
  (let ([brief : String "invalid escape sequence"])
    (case-lambda
      [(/dev/csvin)
       (csv-log-syntax-error /dev/csvin 'warning #false brief)]
      [(/dev/stdin src idx)
       (csv-log-syntax-error /dev/stdin src idx 'warning #false brief)]
      [(/dev/strin start end idx)
       (csv-log-syntax-error /dev/strin start end idx 'warning #false brief)])))

(define csv-log-length-error : (case-> [Input-Port Integer Integer (U CSV-Row (Listof CSV-Field)) Boolean -> Void]
                                       [CSV-StdIn String Nonnegative-Fixnum Integer Integer (U CSV-Row (Listof CSV-Field)) Boolean -> Void]
                                       [String Index Index Nonnegative-Fixnum Integer Integer (U CSV-Row (Listof CSV-Field)) Boolean -> Void])
  (let ([brief (λ [[expected : Integer] [given : Integer] [in : (U CSV-Row (Listof CSV-Field))]] : String
                 (format "field length mismatch: expected ~a, given ~a ~a ~s"
                   expected given (if (< given expected) 'in 'with)
                   (if (vector? in) (vector-take in given) in)))])
    (case-lambda
    [(/dev/csvin expected given in strict?)
     (csv-log-syntax-error /dev/csvin 'error strict? (brief expected given in))]
    [(/dev/stdin src idx expected given in strict?)
     (csv-log-syntax-error /dev/stdin src idx 'error strict? (brief expected given in))]
    [(/dev/strin start end idx expected given in strict?)
     (csv-log-syntax-error /dev/strin start end idx 'error strict? (brief expected given in))])))

(define csv-log-eof-error : (case-> [Input-Port Boolean -> Void]
                                    [CSV-StdIn String Nonnegative-Fixnum Boolean -> Void]
                                    [String Index Index Nonnegative-Fixnum Boolean -> Void])
  (let ([brief : String "unexpected eof of file"])
    (case-lambda
      [(/dev/csvin strict?)
       (csv-log-syntax-error /dev/csvin 'warning strict? brief)]
      [(/dev/stdin src idx strict?)
       (csv-log-syntax-error /dev/stdin src idx 'warning strict? brief)]
      [(/dev/strin start end idx strict?)
       (csv-log-syntax-error /dev/strin start end idx 'warning strict? brief)])))

(define csv-log-out-quotes-error : (case-> [Input-Port Boolean Symbol -> Void]
                                           [CSV-StdIn String Nonnegative-Fixnum Boolean Symbol -> Void]
                                           [String Index Index Nonnegative-Fixnum Boolean Symbol -> Void])
  (let ([brief (λ [[position : Symbol]] : String (format "ignored non-whitespace chars ~a quote char" position))])
    (case-lambda
      [(/dev/csvin strict? position)
       (csv-log-syntax-error /dev/csvin 'warning strict? (brief position))]
      [(/dev/stdin src idx strict? position)
       (csv-log-syntax-error /dev/stdin src idx 'warning strict? (brief position))]
      [(/dev/strin start end idx strict? position)
       (csv-log-syntax-error /dev/strin start end idx 'warning strict? (brief position))])))

(define csv-log-if-invalid : (case-> [Input-Port Boolean Boolean -> Void]
                                     [CSV-StdIn String Nonnegative-Fixnum Boolean Boolean -> Void]
                                     [String Index Index Nonnegative-Fixnum Boolean Boolean -> Void])
  (case-lambda
    [(/dev/csvin valid? strict?)
     (when (not valid?) (csv-log-out-quotes-error /dev/csvin strict? 'after))]
    [(/dev/stdin src idx valid? strict?)
     (when (not valid?) (csv-log-out-quotes-error /dev/stdin src idx strict? 'after))]
    [(/dev/strin start end idx valid? strict?)
     (when (not valid?) (csv-log-out-quotes-error /dev/strin start end idx strict? 'after))]))

(define csv-log-syntax-error : (case-> [Input-Port Log-Level Boolean String -> Void]
                                       [CSV-StdIn String Nonnegative-Fixnum Log-Level Boolean String -> Void]
                                       [String Index Index Nonnegative-Fixnum Log-Level Boolean String -> Void])
  (case-lambda
    [(/dev/csvin level strict? brief)
     (define-values (line column position) (port-next-location /dev/csvin))
     (define message : String
       (cond [(and line column) (format "~a:~a:~a: ~a" (object-name /dev/csvin) line column brief)]
             [else (format "~a: ~a" (object-name /dev/csvin) brief)]))
     (log-message (current-logger) level csv-topic message #false)
     (unless (not strict?)
       (csv-close-input-port /dev/csvin)
       (raise-user-error 'csv "~a" message))]
    [(/dev/stdin src idx level strict? brief)
     (define src-name : Any (csv-input-name /dev/stdin))
     (define message : String (format "~a: @{~a}[~a]: ~a" src-name src (+ idx 1) brief))
     (log-message (current-logger) level csv-topic message #false)
     (unless (not strict?)
       (when (input-port? /dev/stdin)
         (csv-close-input-port /dev/stdin))
       (raise-user-error 'csv "~a" message))]
    [(/dev/strin start end idx level strict? brief)
     (csv-log-syntax-error /dev/strin
                           (substring /dev/strin start end)
                           (assert (- idx start) index?)
                           level strict? brief)]))
