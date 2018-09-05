#lang typed/racket/base

;;; https://tools.ietf.org/html/rfc4180

(provide (all-defined-out))

(require "dialect.rkt")
(require "misc.rkt")

(require racket/unsafe/ops)

;;; Performance hints
;; 0. Comparing with `eq?` is significantly faster than with `eqv?`
;; 1. Disable port lines counting improves the second most
;; 2. Passing a procedure to the reader is more difficult to be optimized than passing boolean to achieve the same effects
;; 3. Avoiding `peek-char`s
;; 4. Single line comment is an empty line
;; 5. `#false` is esier to be optimized than `eof`, but filtering `eof` as late as possible
;; 6. Unboxing the CSV-Dialect early is not helpful

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define in-csv-port : (-> Input-Port Positive-Index CSV-Dialect Boolean Boolean Boolean (Sequenceof (Vectorof CSV-Field)))
  (lambda [/dev/csvin n dialect skip-header? strict? trim-line?]
    (define sentinel : (Pairof (U Char EOF) (Vectorof CSV-Field)) (cons eof empty-row))
    (define (read-csv [hint : (Pairof (U Char EOF) (Vectorof CSV-Field))]) : (Pairof (U Char EOF) (Vectorof CSV-Field))
      (let read-with ([maybe-char : (U Char EOF) (car hint)])
        (define-values (maybe-row maybe-leader) (read-csv-row /dev/csvin n maybe-char dialect strict? trim-line?))
        (cond [(and maybe-leader) (if (and maybe-row) (cons maybe-leader maybe-row) (read-with maybe-leader))]
              [(not maybe-row) (csv-close-input-port /dev/csvin) sentinel]
              [else (cons eof maybe-row)])))

    ((inst make-do-sequence (Pairof (U Char EOF) (Vectorof CSV-Field)) (Vectorof CSV-Field))
     (λ [] (values unsafe-cdr
                   read-csv
                   (read-csv (cons (read-char /dev/csvin) empty-row))
                   (λ [v] (not (eq? v sentinel)))
                   #false
                   #false)))))

(define in-csv-port* : (-> Input-Port CSV-Dialect Boolean Boolean Boolean (Sequenceof CSV-Row*))
  (lambda [/dev/csvin dialect skip-header? strict? trim-line?]
    (define sentinel : (Pairof (U Char EOF) CSV-Row*) (cons eof empty-row*))
    (define (read-csv [hint : (Pairof (U Char EOF) CSV-Row*)]) : (Pairof (U Char EOF) CSV-Row*)
      (let read-with ([leader : (U Char EOF) (car hint)])
        (define-values (maybe-row maybe-leader) (read-csv-row* /dev/csvin leader dialect strict? trim-line?))
        (cond [(and maybe-leader) (if (pair? maybe-row) (cons maybe-leader maybe-row) (read-with maybe-leader))]
              [(null? maybe-row) (csv-close-input-port /dev/csvin) sentinel]
              [else (cons eof maybe-row)])))

    ((inst make-do-sequence (Pairof (U Char EOF) CSV-Row*) CSV-Row*)
     (λ [] (values unsafe-cdr
                   read-csv
                   (read-csv (cons (read-char /dev/csvin) empty-row*))
                   (λ [v] (not (eq? v sentinel)))
                   #false
                   #false)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-read/reverse : (-> Input-Port Positive-Index CSV-Dialect Boolean Boolean Boolean (Listof (Vectorof CSV-Field)))
  (lambda [/dev/csvin n dialect skip-header? strict? trim-line?]
    (let read-csv ([swor : (Listof (Vectorof CSV-Field)) null]
                   [maybe-char : (U Char EOF) (read-char /dev/csvin)])
      (define-values (maybe-row maybe-leader) (read-csv-row /dev/csvin n maybe-char dialect strict? trim-line?))
      (cond [(and maybe-leader) (read-csv (if (and maybe-row) (cons maybe-row swor) swor) maybe-leader)]
            [(and maybe-row) (cons maybe-row swor)]
            [else swor]))))

(define csv-read*/reverse : (-> Input-Port CSV-Dialect Boolean Boolean Boolean (Listof CSV-Row*))
  (lambda [/dev/csvin dialect skip-header? strict? trim-line?]
    (let read-csv ([swor : (Listof CSV-Row*) null]
                   [maybe-char : (U Char EOF) (read-char /dev/csvin)])
      (define-values (maybe-row maybe-leader) (read-csv-row* /dev/csvin maybe-char dialect strict? trim-line?))
      (cond [(and maybe-leader) (read-csv (if (pair? maybe-row) (cons maybe-row swor) swor) maybe-leader)]
            [(pair? maybe-row) (cons maybe-row swor)]
            [else swor]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define read-csv-row : (-> Input-Port Positive-Index (U Char EOF) CSV-Dialect Boolean Boolean (Values (Option (Vectorof CSV-Field)) (Option Char)))
  (lambda [/dev/csvin n leading-char dialect strict? trim-line?]
    (define row : (Vectorof CSV-Field) (make-vector n empty-field))
    (let read-row ([maybe-char : (U Char EOF) leading-char]
                   [idx : Index 0])
      (define-values (field more?) (csv-read-field /dev/csvin maybe-char dialect strict?))
      (define nidx : Positive-Fixnum (+ idx 1))
      (if (eq? more? #true)
          (cond [(>= nidx n) (read-row (or (csv-discard-exceeded-fields /dev/csvin n nidx dialect strict?) eof) 0)]
                [else (vector-set! row idx field) (read-row (read-char /dev/csvin) nidx)])
          (cond [(= nidx n) (vector-set! row idx field) (values row more?)]
                [(> nidx 1) (vector-set! row idx field) (csv-log-length-error /dev/csvin n nidx row strict?) (values #false more?)]
                [(not (eq? field empty-field)) (csv-log-length-error /dev/csvin n nidx (vector field) strict?) (values #false more?)]
                [(or trim-line? (not more?)) (values #false more?)]
                [else (csv-log-length-error /dev/csvin n nidx (vector empty-field) strict?) (values #false more?)])))))

(define read-csv-row* : (-> Input-Port (U Char EOF) CSV-Dialect Boolean Boolean (Values (Listof CSV-Field) (Option Char)))
  (lambda [/dev/csvin leading-char dialect strict? trim-line?]
    (let read-row ([maybe-char : (U Char EOF) leading-char]
                   [sdleif : (Listof CSV-Field) null])
      (define-values (field more?) (csv-read-field /dev/csvin maybe-char dialect strict?))
      (cond [(eq? more? #true) (read-row (read-char /dev/csvin) (cons field sdleif))]
            [(pair? sdleif) (values (reverse (cons field sdleif)) more?)]
            [(not (eq? field empty-field)) (values (list field) more?)]
            [(or trim-line? (not more?)) (values null more?)]
            [else (values empty-row* more?)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-read-field : (-> Input-Port (U Char EOF) CSV-Dialect Boolean (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin leading-char dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define </> : (Option Char) (CSV-Dialect-quote-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    (define trim-right? : Boolean (CSV-Dialect-skip-trailing-space? dialect))

    (let read-field ([srahc : (Listof Char) null]
                     [trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect)]
                     [maybe-char : (U Char EOF) leading-char])
      (cond [(eq? maybe-char <:>) (values (srahc->field/trim-right srahc trim-right?) #true)]
            [(eq? maybe-char </>) (csv-read-quoted-field /dev/csvin srahc </> <\> dialect strict?)]
            [(csv-try-newline* maybe-char /dev/csvin <#>) => (csv-newline-values (srahc->field/trim-right srahc trim-right?))]
            [(eof-object? maybe-char) (values (srahc->field/trim-right srahc trim-right?) #false)]
            [(eq? maybe-char <\>) ; `#true` -> c style escape char has been set'
             (define-values (escaped-char next-char) (csv-read-escaped-char /dev/csvin))
             (cond [(char? escaped-char) (read-field (cons escaped-char srahc) #false next-char)]
                   [else (csv-log-eof-error /dev/csvin strict?) (read-field srahc #false next-char)])]
            [(char-blank? maybe-char) (read-field (if (not trim-left?) (cons maybe-char srahc) srahc) trim-left? (read-char /dev/csvin))]
            [else (read-field (cons maybe-char srahc) #false (read-char /dev/csvin))]))))

(define csv-read-quoted-field : (-> Input-Port (Listof Char) (Option Char) (Option Char) CSV-Dialect Boolean (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin leading-srahc </> <\> dialect strict?]
    ;; NOTE
    ; No matter the leading and trailing whitespaces should be skipped or not,
    ; we tolerate the whitespaces around the quoted field but do not count them as part of the field.
    (unless (for/and : Boolean ([ch : Char (in-list leading-srahc)]) (char-blank? ch))
      (csv-log-out-quotes-error /dev/csvin strict? 'before))
    (let read-quoted-field ([srahc : (Listof Char) null]
                            [maybe-char : (U Char EOF) (read-char /dev/csvin)])
      (cond [(eof-object? maybe-char)
             (csv-log-eof-error /dev/csvin strict?)
             (values (srahc->field srahc) #false)]
            [(eq? maybe-char <\>) ; `#true` -> 'c style escape char has been set'
             (define-values (escaped-char next-char) (csv-read-escaped-char /dev/csvin))
             (read-quoted-field (if (char? escaped-char) (cons escaped-char srahc) srahc) next-char)]
            [(eq? maybe-char </>)
             (define next-char : (U Char EOF) (read-char /dev/csvin))
             (cond [(and (not <\>) (eq? next-char </>)) (read-quoted-field (cons maybe-char srahc) (read-char /dev/csvin))]
                   [else (values (srahc->field srahc) (csv-discard-quoted-rest /dev/csvin next-char dialect strict?))])]
            [(csv-try-newline maybe-char /dev/csvin)
             => (λ [[maybe-leader : (U Char EOF)]]
                  (read-quoted-field (cons #\newline srahc) maybe-leader))]
            [else (read-quoted-field (cons maybe-char srahc) (read-char /dev/csvin))]))))

(define csv-discard-quoted-rest : (-> Input-Port (U Char EOF) CSV-Dialect Boolean (U Char Boolean))
  (lambda [/dev/csvin leading-char dialect strict?]
    ;; NOTE
    ; No matter the leading and trailing whitespaces should be skipped or not,
    ; we tolerate the whitespaces around the quoted field but do not count them as part of the field.
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))

    (let discard ([maybe-char : (U Char EOF) leading-char]
                  [valid? : Boolean #true])
      ;;; TODO: what if the quote char or escape char shows again?
      (cond [(eq? maybe-char <:>) (csv-log-if-invalid /dev/csvin valid? strict?) #true]
            [(csv-try-newline* maybe-char /dev/csvin <#>) => (csv-newline-identity (csv-log-if-invalid /dev/csvin valid? strict?))]
            [(eof-object? maybe-char) (csv-log-if-invalid /dev/csvin valid? strict?) #false]
            [else (discard (read-char /dev/csvin) (and valid? (char-blank? maybe-char)))]))))

(define csv-discard-exceeded-fields : (-> Input-Port Positive-Index Positive-Fixnum CSV-Dialect Boolean (Option Char))
  (lambda [/dev/csvin n count dialect strict?]
    (let discard ([maybe-char : (U Char EOF) (read-char /dev/csvin)]
                  [extras : (Listof CSV-Field) null]
                  [total : Positive-Fixnum count])
      (define-values (field more?) (csv-read-field /dev/csvin maybe-char dialect #false))
      (cond [(eq? more? #true) (discard (read-char /dev/csvin) (cons field extras) (unsafe-fx+ total 1))]
            [else (csv-log-length-error /dev/csvin n (+ total 1) (reverse (cons field extras)) strict?) more?]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-try-newline : (-> (U Char EOF) Input-Port (U Char EOF False))
  (lambda [ch /dev/csvin]
    (cond [(eq? ch #\return)
            (define next-char : (U Char EOF) (read-char /dev/csvin))
            (if (eq? next-char #\linefeed) (read-char /dev/csvin) next-char)]
          [(eq? ch #\linefeed) (read-char /dev/csvin)]
          [else #false])))

(define csv-try-newline* : (-> (U Char EOF) Input-Port (Option Char) (U Char EOF False))
  (lambda [ch /dev/csvin <#>]
    (or (csv-try-newline ch /dev/csvin)
        (and (eq? ch <#>)
             (read-line /dev/csvin 'any)
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
(define csv-read-escaped-char : (-> Input-Port (Values (U Char EOF) (U Char EOF)))
  ;; https://en.wikipedia.org/wiki/Escape_sequences_in_C#Table_of_escape_sequences
  (lambda [/dev/csvin]
    (define maybe-char : (U Char EOF) (read-char /dev/csvin))
    (case maybe-char
      [(#\a) (values #\u07 (read-char /dev/csvin))]
      [(#\b) (values #\u08 (read-char /dev/csvin))]
      [(#\f) (values #\u0C (read-char /dev/csvin))]
      [(#\n) (values #\u0A (read-char /dev/csvin))]
      [(#\r) (values #\u0D (read-char /dev/csvin))]
      [(#\t) (values #\u09 (read-char /dev/csvin))]
      [(#\v) (values #\u0B (read-char /dev/csvin))]
      [(#\e) (values #\u1B (read-char /dev/csvin))]
      [(#\x) (csv-read-hexadecimal-char /dev/csvin)]
      [(#\u) (csv-read-unicode-char /dev/csvin 4)]
      [(#\U) (csv-read-unicode-char /dev/csvin 8)]
      [(#\0 #\1 #\2 #\3 #\4 #\5 #\6 #\7) (csv-read-octal-char /dev/csvin (assert maybe-char char?))]
      [else (let ([maybe-leader : (Option (U Char EOF)) (csv-try-newline maybe-char /dev/csvin)])
              (cond [(and maybe-leader) (values #\newline maybe-leader)]
                    [else (values maybe-char (read-char /dev/csvin))]))])))

(define csv-read-octal-char : (-> Input-Port Char (Values (U Char EOF) (U Char EOF)))
  (lambda [/dev/csvin leading-char]
    (let read-octal ([n : Fixnum (char->decimal leading-char)]
                     [count : Index 1])
      (define maybe-char : (U Char EOF) (read-char /dev/csvin))
      (cond [(>= count 3) (values (unicode->char n) maybe-char)]
            [(char-oct-digit? maybe-char) (read-octal (unsafe-fx+ (unsafe-fxlshift n 3) (char->decimal maybe-char)) (+ count 1))]
            [else (values (unicode->char n) maybe-char)]))))

(define csv-read-hexadecimal-char : (-> Input-Port (Values (U Char EOF) (U Char EOF)))
  (lambda [/dev/csvin]
    (define maybe-char : (U Char EOF) (read-char /dev/csvin))
    (cond [(not (char-hex-digit? maybe-char)) (csv-log-escape-error /dev/csvin) (values #\uFFFD maybe-char)]
          [else (let read-hexa ([n : Fixnum (char->decimal maybe-char)])
                  (define maybe-char : (U Char EOF) (read-char /dev/csvin))
                  (cond [(not (char-hex-digit? maybe-char)) (values (unicode->char n) maybe-char)]
                        [(< n #x10FFFF) (read-hexa (unsafe-fx+ (unsafe-fxlshift n 4) (char->decimal maybe-char)))]
                        [else (read-hexa n)]))])))

(define csv-read-unicode-char : (-> Input-Port Positive-Byte (Values (U Char EOF) (U Char EOF)))
  (lambda [/dev/csvin total]
    (let read-unicode ([n : Fixnum 0]
                       [count : Index 0])
      (define maybe-char : (U Char EOF) (read-char /dev/csvin))
      (cond [(>= count total) (values (unicode->char n) maybe-char)]
            [(not (char-hex-digit? maybe-char)) (csv-log-escape-error /dev/csvin) (values #\uFFFD maybe-char)]
            [(< n #x10FFFF) (read-unicode (unsafe-fx+ (unsafe-fxlshift n 4) (char->decimal maybe-char)) (+ count 1))]
            [else (read-unicode n (+ count 1))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define srahc->field : (-> (Listof Char) CSV-Field)
  (lambda [srahc]
    (cond [(null? srahc) empty-field]
          [else (list->string (reverse srahc))])))

(define srahc->field/trim-right : (-> (Listof Char) Boolean CSV-Field)
  (lambda [srahc trim?]
    (cond [(not trim?) (srahc->field srahc)]
          [else (let trim ([rest : (Listof Char) srahc])
                  (cond [(null? rest) empty-field] 
                        [(char-blank? (car rest)) (trim (cdr rest))]
                        [else (srahc->field rest)]))])))
