#lang typed/racket/base

;;; https://tools.ietf.org/html/rfc4180
;;; https://frictionlessdata.io/specs/csv-dialect

(provide (all-defined-out))

;;; Performance hints
;; 1. Comparing with `eq?` is significantly faster than with `eqv?`
;; 2. Passing a procedure to the reader is more difficult to be optimized than passing boolean to achieve the same effects
;; 3. Avoiding `peek-char`s
;; 4. Single line comment is an empty line

(define-type CSV-Field String)
(define-type CSV-Row (Pairof CSV-Field (Listof CSV-Field)))

(define empty-field : CSV-Field "")
(define empty-row : CSV-Row (list empty-field))

(struct CSV-Dialect
  ([delimiter : Char]
   [quote-char : Char]
   [escape-char : (Option Char)]
   [comment-char : (Option Char)]
   [skip-empty-line? : Boolean]
   [skip-leading-space? : Boolean]
   [skip-tailing-space? : Boolean])
  #:transparent)

(define csv-read/reversed : (-> Input-Port Positive-Index Boolean CSV-Dialect Boolean (Listof (Vectorof CSV-Field)))
  (lambda [/dev/csvin n skipheader? dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define </> : Char (CSV-Dialect-quote-char dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    (define trim-line? : Boolean (CSV-Dialect-skip-empty-line? dialect))
    (define trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (CSV-Dialect-skip-tailing-space? dialect))

    (unless (not skipheader?)
      (read-line /dev/csvin))

    (let read-this-csv ([swor : (Listof (Vectorof CSV-Field)) null]
                        [maybe-char : (U EOF Char) (read-char /dev/csvin)])
      (define-values (maybe-row maybe-leader) (read-csv-row /dev/csvin n maybe-char <#> <:> </> <\> strict? trim-line? trim-left? trim-right?))
      (cond [(not maybe-row) (if (char? maybe-leader) (read-this-csv swor maybe-leader) swor)]
            [(char? maybe-leader) (read-this-csv (cons maybe-row swor) maybe-leader)]
            [else (cons maybe-row swor)]))))

(define csv-read*/reversed : (-> Input-Port Boolean CSV-Dialect Boolean (Listof CSV-Row))
  (lambda [/dev/csvin skipheader? dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define </> : Char (CSV-Dialect-quote-char dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    (define trim-line? : Boolean (CSV-Dialect-skip-empty-line? dialect))
    (define trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (CSV-Dialect-skip-tailing-space? dialect))

    (unless (not skipheader?)
      (read-line /dev/csvin))

    (let read-this-csv ([swor : (Listof CSV-Row) null]
                        [maybe-char : (U EOF Char) (read-char /dev/csvin)])
      (define-values (maybe-row maybe-leader) (read-csv-row* /dev/csvin maybe-char <#> <:> </> <\> strict? trim-left? trim-right?))
      (cond [(not maybe-leader) (if (pair? maybe-row) (cons maybe-row swor) swor)]
            [(not trim-line?) (read-this-csv (if (pair? maybe-row) (cons empty-row swor) swor) maybe-leader)]
            [else (read-this-csv swor maybe-leader)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define read-csv-row : (-> Input-Port Positive-Index (U Char EOF) (Option Char) Char Char (Option Char) Boolean Boolean Boolean Boolean
                           (Values (Option (Vectorof CSV-Field)) (U Char Boolean)))
  (lambda [/dev/csvin n leading-char <#> <:> </> <\> strict? trim-line? trim-left? trim-right?]
    (define row : (Vectorof CSV-Field) (make-vector n empty-field))
    (let read-this-row ([maybe-char : (U Char EOF) leading-char]
                        [idx : Index 0])
      (define-values (field more?) (csv-read-field/trim-left /dev/csvin maybe-char <#> <:> </> <\> strict? trim-left? trim-right?))
      (define nidx : Positive-Fixnum (+ idx 1))
      (if (eq? more? #true)
          (cond [(>= nidx n) (values #false (csv-skip-exceeded-fields /dev/csvin n nidx <#> <:> </> <\> strict?))]
                [else (vector-set! row idx field) (read-this-row (read-char /dev/csvin) nidx)])
          (cond [(= nidx n) (vector-set! row idx field) (values row more?)]
                [(> nidx 1) (vector-set! row idx field) (csv-log-length-error /dev/csvin n nidx row strict?) (values #false more?)]
                [(not (eq? field empty-field)) (csv-log-length-error /dev/csvin n nidx (vector field) strict?) (values #false more?)]
                [(not trim-line?) (csv-log-length-error /dev/csvin n nidx (vector empty-field) strict?) (values #false more?)]
                [else (values #false more?)])))))

(define read-csv-row* : (-> Input-Port (U Char EOF) (Option Char) Char Char (Option Char) Boolean Boolean Boolean
                            (Values (Listof CSV-Field) (Option Char)))
  (lambda [/dev/csvin leading-char <#> <:> </> <\> strict? trim-left? trim-right?]
    (let read-this-row ([maybe-char : (U Char EOF) leading-char]
                        [sdleif : (Listof CSV-Field) null])
      (define-values (field more?) (csv-read-field/trim-left /dev/csvin maybe-char <#> <:> </> <\> strict? trim-left? trim-right?))
      (cond [(eq? more? #true) (read-this-row (read-char /dev/csvin) (cons field sdleif))]
            [else (values (sdleif->row field sdleif) more?)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-read-field/trim-left : (-> Input-Port (U Char EOF) (Option Char) Char Char (Option Char) Boolean Boolean Boolean
                                       (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin leading-char <#> <:> </> <\> strict? trim-left? trim-right?]
    (if (not trim-left?)
        (csv-read-field /dev/csvin leading-char <#> <:> </> <\> strict? trim-right?)
        (let read-without-whitespace ([maybe-char : (U Char EOF) leading-char])
          (cond [(eq? maybe-char <:>) (values empty-field #true)]
                [(eq? maybe-char </>) (csv-read-quoted-field /dev/csvin <#> <:> </> <\> strict?)]
                [(csv-try-newline* maybe-char /dev/csvin <#>) => (csv-newline-values empty-field)]
                [(eof-object? maybe-char) (values empty-field #false)]
                [(char-whitespace? maybe-char) (read-without-whitespace (read-char /dev/csvin))]
                [(eq? maybe-char <\>) (read-without-whitespace (read-char /dev/csvin))]
                [else (csv-read-field /dev/csvin maybe-char <#> <:> </> <\> strict? trim-right?)])))))

(define csv-read-field : (-> Input-Port (U Char EOF) (Option Char) Char Char (Option Char) Boolean Boolean
                             (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin leading-char <#> <:> </> <\> strict? trim-right?]
    (let read-this-field ([srahc : (Listof Char) null]
                          [maybe-char : (U Char EOF) leading-char])
      (cond [(eq? maybe-char <:>) (values (srahc->field/trim-right srahc trim-right?) #true)]
            [(eq? maybe-char </>) (csv-read-quoted-field/check /dev/csvin srahc <#> <:> </> <\> strict?)]
            [(csv-try-newline* maybe-char /dev/csvin <#>) => (csv-newline-values (srahc->field/trim-right srahc trim-right?))]
            [(eof-object? maybe-char) (values (srahc->field/trim-right srahc trim-right?) #false)]
            [(eq? maybe-char <\>) ; must have another escape char set
             (define-values (escaped-char next-char) (csv-read-escaped-char /dev/csvin))
             (read-this-field (if (char? escaped-char) (cons escaped-char srahc) srahc) next-char)]
            [else (read-this-field (cons maybe-char srahc) (read-char /dev/csvin))]))))

(define csv-read-quoted-field/check : (-> Input-Port (Listof Char) (Option Char) Char Char (Option Char) Boolean
                                          (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin leading-srahc <#> <:> </> <\> strict?]
    ;; NOTE
    ; No matter the leading and trailing whitespaces should be skipped or not,
    ; we tolerate the whitespaces around the quoted field but do not count them as part of the field.
    (unless (for/and : Boolean ([ch : Char (in-list leading-srahc)]) (char-whitespace? ch))
      (csv-log-out-quotes-error /dev/csvin strict? 'before))
    (csv-read-quoted-field /dev/csvin <#> <:> </> <\> strict?)))

(define csv-read-quoted-field : (-> Input-Port (Option Char) Char Char (Option Char) Boolean (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin <#> <:> </> <\> strict?]
    (let read-this-quoted-field ([srahc : (Listof Char) null]
                                 [maybe-char : (U Char EOF) (read-char /dev/csvin)])
      (cond [(eof-object? maybe-char)
             (csv-log-eof-error /dev/csvin strict?)
             (values (srahc->field srahc) #false)]
            [(eq? maybe-char <\>) ; must have another escape char set
             (define-values (escaped-char next-char) (csv-read-escaped-char /dev/csvin))
             (read-this-quoted-field (if (char? escaped-char) (cons escaped-char srahc) srahc) next-char)]
            [(eq? maybe-char </>)
             (define next-char : (U Char EOF) (read-char /dev/csvin))
             (cond [(char? <\>) (values (srahc->field srahc) (csv-skip-quoted-rest /dev/csvin <#> <:> next-char strict?))]
                   [(eq? next-char </>) (read-this-quoted-field (cons maybe-char srahc) (read-char /dev/csvin))]
                   [else (values (srahc->field srahc) (csv-skip-quoted-rest /dev/csvin <#> <:> next-char strict?))])]
            [(csv-try-newline maybe-char /dev/csvin)
             => (λ [[maybe-leader : (U Char EOF)]]
                  (read-this-quoted-field (cons #\newline srahc) maybe-leader))]
            [else (read-this-quoted-field (cons maybe-char srahc) (read-char /dev/csvin))]))))

(define csv-skip-quoted-rest : (-> Input-Port (Option Char) Char (U Char EOF) Boolean (U Char Boolean))
  (lambda [/dev/csvin <#> <:> leading-char strict?]
    ;; NOTE
    ; No matter the leading and trailing whitespaces should be skipped or not,
    ; we tolerate the whitespaces around the quoted field but do not count them as part of the field.
    (let skip-this-quoted-field ([maybe-char : (U Char EOF) leading-char]
                                 [valid? : Boolean #true])
      ;;; TODO: what if the quote char or escape char shows again?
      (cond [(eq? maybe-char <:>) (csv-log-if-invalid /dev/csvin valid? strict?) #true]
            [(csv-try-newline* maybe-char /dev/csvin <#>) => (csv-newline-identity (csv-log-if-invalid /dev/csvin valid? strict?))]
            [(eof-object? maybe-char) (csv-log-if-invalid /dev/csvin valid? strict?) #false]
            [else (skip-this-quoted-field (read-char /dev/csvin) (and valid? (char-whitespace? maybe-char)))]))))

(define csv-skip-exceeded-fields : (-> Input-Port Positive-Index Integer (Option Char) Char Char (Option Char) Boolean (U Char Boolean))
  (lambda [/dev/csvin n count <#> <:> </> <\> strict?]
    (let skip-this-row ([maybe-char : (U Char EOF) (read-char /dev/csvin)]
                        [extras : (Listof CSV-Field) null]
                        [total : Integer count])
      (define-values (field more?) (csv-read-field /dev/csvin maybe-char <#> <:> </> <\> #false #false))
      (cond [(eq? more? #true) (skip-this-row (read-char /dev/csvin) (cons field extras) (+ total 1))]
            [else (csv-log-length-error /dev/csvin n (+ total 1) (reverse (cons field extras)) strict?) more?]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-read-escaped-char : (-> Input-Port (Values (U Char EOF) (U Char EOF)))
  (lambda [/dev/csvin]
    (define maybe-char : (U Char EOF) (read-char /dev/csvin))
    (define maybe-leader : (Option (U Char EOF)) (csv-try-newline maybe-char /dev/csvin))
    (cond [(and maybe-leader) (values #\newline maybe-leader)]
          [else (values maybe-char (read-char /dev/csvin))])))

(define csv-try-newline : (-> (U Char EOF) Input-Port (U Char EOF False))
  (lambda [ch /dev/csvin]
    (cond [(eq? ch #\return)
           (define next-char : (U Char EOF) (read-char /dev/csvin))
           (if (eq? next-char #\linefeed) (read-char /dev/csvin) next-char)]
          [(eq? ch #\linefeed)
           (define next-char : (U Char EOF) (read-char /dev/csvin))
           (if (eq? next-char #\return) (read-char /dev/csvin) next-char)]
          [else #false])))

(define csv-try-newline* : (-> (U Char EOF) Input-Port (Option Char) (U Char EOF False))
  (lambda [ch /dev/csvin <#>]
    (or (csv-try-newline ch /dev/csvin)
        (and (eq? ch <#>)
             (read-line /dev/csvin)
             (read-char /dev/csvin)))))

(define csv-newline-identity : (All (a) (-> a (-> (U Char EOF) (Option Char))))
  (lambda [v]
    (λ [[maybe-char : (U Char EOF)]]
      (and (char? maybe-char) maybe-char))))

(define csv-newline-values : (All (a) (-> a (-> (U Char EOF) (Values a (Option Char)))))
  (lambda [v]
    (λ [[maybe-char : (U Char EOF)]]
      (values v (and (char? maybe-char) maybe-char)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define srahc->field : (-> (Listof Char) CSV-Field)
  (lambda [srahc]
    (cond [(null? srahc) empty-field]
          [else (list->string (reverse srahc))])))

(define srahc->field/trim-right : (-> (Listof Char) Boolean CSV-Field)
  (lambda [srahc trim?]
    (cond [(not trim?) (srahc->field srahc)]
          [else (let trim ([rest : (Listof Char) srahc])
                  (cond [(null? rest) empty-field] ; there should be at least one non-whitespaces regardless whether trimming the leading ones 
                        [(char-whitespace? (car rest)) (trim (cdr rest))]
                        [else (srahc->field rest)]))])))

(define sdleif->row : (->  CSV-Field (Listof CSV-Field) (Listof CSV-Field))
  (lambda [field sdleif]
    (cond [(pair? sdleif) (reverse (cons field sdleif))]
          [(eq? field empty-field) null]
          [else (list field)])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-topic : Symbol 'exn:csv:syntax)

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
      (raise-user-error 'csv "~a" message))))
