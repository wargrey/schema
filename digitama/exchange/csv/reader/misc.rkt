#lang typed/racket/base

(provide (all-defined-out))

(require racket/vector)

(define-type CSV-Field String)
(define-type CSV-Row (Vectorof CSV-Field))
(define-type CSV-Row* (Pairof CSV-Field (Listof CSV-Field)))
(define-type CSV-StdIn (U Input-Port Bytes Path-String))
(define-type CSV-StdIn* (Option CSV-StdIn))

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
          [else csv-string-name])))

(define csv-input-port : (-> CSV-StdIn Boolean Input-Port)
  (lambda [/dev/stdin trace?]
    (define /dev/csvin : Input-Port
      (cond [(input-port? /dev/stdin) /dev/stdin]
            [(path? /dev/stdin) (open-input-file /dev/stdin)]
            [(bytes? /dev/stdin) (open-input-bytes /dev/stdin csv-bytes-name)]
            [else (open-input-string /dev/stdin csv-bytes-name)]))

    (when (and trace?)
      (unless (eq? /dev/csvin /dev/stdin)
        (hash-set! csv-ports /dev/csvin #true)))
    
    /dev/csvin))

(define csv-input-source : (-> CSV-StdIn Boolean (U Input-Port String))
  (lambda [/dev/stdin trace?]
    (cond [(input-port? /dev/stdin) /dev/stdin]
          [(path? /dev/stdin) (csv-input-port /dev/stdin trace?)]
          [(regexp-match? #px"\\.csv$" /dev/stdin) (csv-input-port (string->path (format "~a" /dev/stdin)) trace?)]
          [(bytes? /dev/stdin) (parameterize ([port-count-lines-enabled #false]) (csv-input-port /dev/stdin trace?))]
          [else /dev/stdin])))

(define csv-close-input-port : (-> Input-Port Void)
  (lambda [/dev/csvin]
    (when (hash-has-key? csv-ports /dev/csvin)
      (hash-remove! csv-ports /dev/csvin)
      (close-input-port /dev/csvin))))

(define csv-log-escape-error : (case-> [Input-Port -> Void]
                                       [CSV-StdIn* String Nonnegative-Fixnum -> Void])
  (let ([brief : String "invalid escape sequence"])
    (case-lambda
      [(/dev/csvin)
       (csv-log-syntax-error /dev/csvin 'warning #false brief)]
      [(/dev/stdin src idx)
       (csv-log-syntax-error /dev/stdin src idx 1 'warning #false brief)])))

(define csv-log-length-error : (case-> [Input-Port Integer Integer (U CSV-Row (Listof CSV-Field)) Boolean -> Void]
                                       [CSV-StdIn* String Nonnegative-Fixnum Integer Integer (U CSV-Row (Listof CSV-Field)) Boolean -> Void])
  (let ([brief (λ [[expected : Integer] [given : Integer] [in : (U CSV-Row (Listof CSV-Field))]] : String
                 (format "field length mismatch: expected ~a, given ~a ~a ~s"
                   expected given (if (< given expected) 'in 'with)
                   (if (vector? in) (vector-take in given) in)))])
    (case-lambda
    [(/dev/csvin expected given in strict?)
     (csv-log-syntax-error /dev/csvin 'error strict? (brief expected given in))]
    [(/dev/stdin src idx expected given in strict?)
     (csv-log-syntax-error /dev/stdin src idx 1 'error strict? (brief expected given in))])))

(define csv-log-eof-error : (case-> [Input-Port Boolean -> Void]
                                    [CSV-StdIn* String Nonnegative-Fixnum Boolean -> Void])
  (let ([brief : String "unexpected eof of file"])
    (case-lambda
      [(/dev/csvin strict?)
       (csv-log-syntax-error /dev/csvin 'warning strict? brief)]
      [(/dev/stdin src idx strict?)
       (csv-log-syntax-error /dev/stdin src idx 0 'warning strict? brief)])))

(define csv-log-out-quotes-error : (case-> [Input-Port Boolean Symbol -> Void]
                                           [CSV-StdIn* String Nonnegative-Fixnum Boolean Symbol -> Void])
  (let ([brief (λ [[position : Symbol]] : String (format "ignored non-whitespace chars ~a quote char" position))])
    (case-lambda
      [(/dev/csvin strict? position)
       (csv-log-syntax-error /dev/csvin 'warning strict? (brief position))]
      [(/dev/stdin src idx strict? position)
       (csv-log-syntax-error /dev/stdin src idx 0 'warning strict? (brief position))])))

(define csv-log-if-invalid : (case-> [Input-Port Boolean Boolean -> Void]
                                     [CSV-StdIn* String Nonnegative-Fixnum Boolean Boolean -> Void])
  (case-lambda
    [(/dev/csvin valid? strict?)
     (when (not valid?) (csv-log-out-quotes-error /dev/csvin strict? 'after))]
    [(/dev/stdin src idx valid? strict?)
     (when (not valid?) (csv-log-out-quotes-error /dev/stdin src (+ (assert idx index?) 1) strict? 'after))]))

(define csv-log-syntax-error : (case-> [Input-Port Log-Level Boolean String -> Void]
                                       [CSV-StdIn* String Nonnegative-Fixnum Byte Log-Level Boolean String -> Void])
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
    [(/dev/stdin src idx idx0 level strict? brief)
     (define-values (src-name strin pos)
       (if (not /dev/stdin)
           (let ([start (search-sol src (- idx 1))]
                 [end (search-eol src idx)])
             (values csv-string-name (substring src start end) (- idx start)))
           (values (csv-input-name /dev/stdin) src idx)))
     (define message : String (format "~a: @{~a}[~a]: ~a" src-name strin (+ pos idx0) brief))
     (log-message (current-logger) level csv-topic message #false)
     (unless (not strict?)
       (when (input-port? /dev/stdin)
         (csv-close-input-port /dev/stdin))
       (raise-user-error 'csv "~a" message))]))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define search-sol : (-> String Fixnum Integer)
  (lambda [/dev/strin pos]
    (cond [(< pos 0) 0]
          [else (let ([ch (string-ref /dev/strin pos)])
                  (cond [(or (eq? ch #\linefeed) (eq? ch #\return)) (+ pos 1)]
                        [else (search-sol /dev/strin (- pos 1))]))])))

(define search-eol : (-> String Nonnegative-Fixnum Index)
  (lambda [/dev/strin pos]
    (define eos : Index (string-length /dev/strin))
    (let search ([pos : Nonnegative-Fixnum pos])
      (cond [(>= pos eos) eos]
            [else (let ([ch (string-ref /dev/strin pos)])
                    (cond [(or (eq? ch #\linefeed) (eq? ch #\return)) pos]
                          [else (search (+ pos 1))]))]))))
