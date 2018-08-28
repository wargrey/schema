#lang typed/racket/base

;;; https://tools.ietf.org/html/rfc4180

(provide (all-defined-out))

(require "misc.rkt")

(require racket/unsafe/ops)

;;; Performance hints
;; 0. Comparing with `eq?` is significantly faster than with `eqv?`
;; 1. Disable port lines counting improves the second most
;; 2. Passing a procedure to the reader is more difficult to be optimized than passing boolean to achieve the same effects
;; 3. Avoiding `peek-char`s
;; 4. Single line comment is an empty line
;; 5. `#false` is esier to be optimized than `eof`, but filtering `eof` as late as possible.

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define in-csv-port : (-> CSV-StdIn Positive-Index Boolean CSV-Dialect Boolean (Sequenceof (Vectorof CSV-Field)))
  (lambda [/dev/stdin n skipheader? dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define </> : (Option Char) (CSV-Dialect-quote-char dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    (define trim-line? : Boolean (CSV-Dialect-skip-empty-line? dialect))
    (define trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (CSV-Dialect-skip-trailing-space? dialect))

    (define /dev/csvin : Input-Port (csv-input-port /dev/stdin skipheader? #true))
    (define sentinel : (Pairof (U Char EOF) (Vectorof CSV-Field)) (cons eof empty-row))
    (define (read-csv [hint : (Pairof (U Char EOF) (Vectorof CSV-Field))]) : (Pairof (U Char EOF) (Vectorof CSV-Field))
      (define-values (maybe-row maybe-leader) (read-csv-row /dev/csvin n (car hint) <#> <:> </> <\> strict? trim-line? trim-left? trim-right?))
      (cond [(and maybe-row) (cons (if (char? maybe-leader) maybe-leader eof) maybe-row)]
            [(not maybe-leader) (csv-close-input-port /dev/csvin) sentinel]
            [else (read-csv (cons maybe-leader empty-row))]))

    ((inst make-do-sequence (Pairof (U Char EOF) (Vectorof CSV-Field)) (Vectorof CSV-Field))
     (λ [] (values unsafe-cdr
                   read-csv
                   (read-csv (cons (read-char /dev/csvin) empty-row))
                   (λ [v] (not (eq? v sentinel)))
                   #false
                   #false)))))

(define in-csv-port* : (-> CSV-StdIn Boolean CSV-Dialect Boolean (Sequenceof CSV-Row*))
  (lambda [/dev/stdin skipheader? dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define </> : (Option Char) (CSV-Dialect-quote-char dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    (define trim-line? : Boolean (CSV-Dialect-skip-empty-line? dialect))
    (define trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (CSV-Dialect-skip-trailing-space? dialect))

    (define /dev/csvin : Input-Port (csv-input-port /dev/stdin skipheader? #true))
    (define sentinel : (Pairof (U Char EOF) CSV-Row*) (cons eof empty-row*))
    (define (read-csv [hint : (Pairof (U Char EOF) CSV-Row*)]) : (Pairof (U Char EOF) CSV-Row*)
      (define-values (maybe-row maybe-leader) (read-csv-row* /dev/csvin (car hint) <#> <:> </> <\> strict? trim-line? trim-left? trim-right?))
      (cond [(pair? maybe-row) (cons (if (char? maybe-leader) maybe-leader eof) maybe-row)]
            [(not maybe-leader) (csv-close-input-port /dev/csvin) sentinel]
            [else (read-csv (cons maybe-leader empty-row*))]))

    ((inst make-do-sequence (Pairof (U Char EOF) CSV-Row*) CSV-Row*)
     (λ [] (values unsafe-cdr
                   read-csv
                   (read-csv (cons (read-char /dev/csvin) empty-row*))
                   (λ [v] (not (eq? v sentinel)))
                   #false
                   #false)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-read/reversed : (-> CSV-StdIn Positive-Index Boolean CSV-Dialect Boolean (Listof (Vectorof CSV-Field)))
  (lambda [/dev/stdin n skipheader? dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define </> : (Option Char) (CSV-Dialect-quote-char dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    (define trim-line? : Boolean (CSV-Dialect-skip-empty-line? dialect))
    (define trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (CSV-Dialect-skip-trailing-space? dialect))
    
    (define /dev/csvin : Input-Port (csv-input-port /dev/stdin skipheader? #false))
    (let read-csv ([swor : (Listof (Vectorof CSV-Field)) null]
                   [maybe-char : (U Char EOF) (read-char /dev/csvin)])
      (define-values (maybe-row maybe-leader) (read-csv-row /dev/csvin n maybe-char <#> <:> </> <\> strict? trim-line? trim-left? trim-right?))
      (cond [(not maybe-row) (if (char? maybe-leader) (read-csv swor maybe-leader) swor)]
            [(char? maybe-leader) (read-csv (cons maybe-row swor) maybe-leader)]
            [else (cons maybe-row swor)]))))

(define csv-read*/reversed : (-> CSV-StdIn Boolean CSV-Dialect Boolean (Listof CSV-Row*))
  (lambda [/dev/stdin skipheader? dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define </> : (Option Char) (CSV-Dialect-quote-char dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    (define trim-line? : Boolean (CSV-Dialect-skip-empty-line? dialect))
    (define trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (CSV-Dialect-skip-trailing-space? dialect))

    (define /dev/csvin : Input-Port (csv-input-port /dev/stdin skipheader? #false))
    (let read-csv ([swor : (Listof CSV-Row*) null]
                   [maybe-char : (U Char EOF) (read-char /dev/csvin)])
      (define-values (maybe-row maybe-leader) (read-csv-row* /dev/csvin maybe-char <#> <:> </> <\> strict? trim-line? trim-left? trim-right?))
      (cond [(null? maybe-row) (if (char? maybe-leader) (read-csv swor maybe-leader) swor)]
            [(char? maybe-leader) (read-csv (cons maybe-row swor) maybe-leader)]
            [else (cons maybe-row swor)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define read-csv-row : (-> Input-Port Positive-Index (U Char EOF) (Option Char) Char (Option Char) (Option Char) Boolean Boolean Boolean Boolean
                           (Values (Option (Vectorof CSV-Field)) (Option Char)))
  (lambda [/dev/csvin n leading-char <#> <:> </> <\> strict? trim-line? trim-left? trim-right?]
    (define row : (Vectorof CSV-Field) (make-vector n empty-field))
    (let read-row ([maybe-char : (U Char EOF) leading-char]
                   [idx : Index 0])
      (define-values (field more?) (csv-read-field/trim-left /dev/csvin maybe-char <#> <:> </> <\> strict? trim-left? trim-right?))
      (define nidx : Positive-Fixnum (+ idx 1))
      (if (eq? more? #true)
          (cond [(>= nidx n) (values #false (csv-discard-exceeded-fields /dev/csvin n nidx <#> <:> </> <\> strict?))]
                [else (vector-set! row idx field) (read-row (read-char /dev/csvin) nidx)])
          (cond [(= nidx n) (vector-set! row idx field) (values row more?)]
                [(> nidx 1) (vector-set! row idx field) (csv-log-length-error /dev/csvin n nidx row strict?) (values #false more?)]
                [(not (eq? field empty-field)) (csv-log-length-error /dev/csvin n nidx (vector field) strict?) (values #false more?)]
                [(not trim-line?) (csv-log-length-error /dev/csvin n nidx (vector empty-field) strict?) (values #false more?)]
                [else (values #false more?)])))))

(define read-csv-row* : (-> Input-Port (U Char EOF) (Option Char) Char (Option Char) (Option Char) Boolean Boolean Boolean Boolean
                            (Values (Listof CSV-Field) (Option Char)))
  (lambda [/dev/csvin leading-char <#> <:> </> <\> strict? trim-line? trim-left? trim-right?]
    (let read-row ([maybe-char : (U Char EOF) leading-char]
                   [sdleif : (Listof CSV-Field) null])
      (define-values (field more?) (csv-read-field/trim-left /dev/csvin maybe-char <#> <:> </> <\> strict? trim-left? trim-right?))
      (cond [(eq? more? #true) (read-row (read-char /dev/csvin) (cons field sdleif))]
            [(pair? sdleif) (values (reverse (cons field sdleif)) more?)]
            [(not (eq? field empty-field)) (values (list field) more?)]
            [(not trim-line?) (values empty-row* more?)]
            [(not more?) (values null more?)]
            [else (read-row more? null)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-read-field/trim-left : (-> Input-Port (U Char EOF) (Option Char) Char (Option Char) (Option Char) Boolean Boolean Boolean
                                       (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin leading-char <#> <:> </> <\> strict? trim-left? trim-right?]
    (if (not trim-left?)
        (csv-read-field /dev/csvin leading-char <#> <:> </> <\> strict? trim-right?)
        (let read-without-whitespace ([maybe-char : (U Char EOF) leading-char])
          (cond [(eq? maybe-char <:>) (values empty-field #true)]
                [(eq? maybe-char </>) (csv-read-quoted-field /dev/csvin <#> <:> (assert </> char?) <\> strict?)]
                [(eof-object? maybe-char) (values empty-field #false)]
                [(char-blank? maybe-char) (read-without-whitespace (read-char /dev/csvin))]
                [(csv-try-newline* maybe-char /dev/csvin <#>) => (csv-newline-values empty-field)]
                [else (csv-read-field /dev/csvin maybe-char <#> <:> </> <\> strict? trim-right?)])))))

(define csv-read-field : (-> Input-Port (U Char EOF) (Option Char) Char (Option Char) (Option Char) Boolean Boolean
                             (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin leading-char <#> <:> </> <\> strict? trim-right?]
    (let read-field ([srahc : (Listof Char) null]
                     [maybe-char : (U Char EOF) leading-char])
      (cond [(eq? maybe-char <:>) (values (srahc->field/trim-right srahc trim-right?) #true)]
            [(eq? maybe-char </>) (csv-read-quoted-field/check /dev/csvin srahc <#> <:> </> <\> strict?)]
            [(csv-try-newline* maybe-char /dev/csvin <#>) => (csv-newline-values (srahc->field/trim-right srahc trim-right?))]
            [(eof-object? maybe-char) (values (srahc->field/trim-right srahc trim-right?) #false)]
            [(eq? maybe-char <\>) ; `#true` -> c style escape char has been set'
             (define-values (escaped-char next-char) (csv-read-escaped-char /dev/csvin))
             (read-field (if (char? escaped-char) (cons escaped-char srahc) srahc) next-char)]
            [else (read-field (cons maybe-char srahc) (read-char /dev/csvin))]))))

(define csv-read-quoted-field/check : (-> Input-Port (Listof Char) (Option Char) Char (Option Char) (Option Char) Boolean
                                          (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin leading-srahc <#> <:> </> <\> strict?]
    ;; NOTE
    ; No matter the leading and trailing whitespaces should be skipped or not,
    ; we tolerate the whitespaces around the quoted field but do not count them as part of the field.
    (unless (for/and : Boolean ([ch : Char (in-list leading-srahc)]) (char-blank? ch))
      (csv-log-out-quotes-error /dev/csvin strict? 'before))
    (csv-read-quoted-field /dev/csvin <#> <:> </> <\> strict?)))

(define csv-read-quoted-field : (-> Input-Port (Option Char) Char  (Option Char) (Option Char) Boolean (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin <#> <:> </> <\> strict?]
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
             (cond [(char? <\>) (values (srahc->field srahc) (csv-discard-quoted-rest /dev/csvin <#> <:> next-char strict?))]
                   [(eq? next-char </>) (read-quoted-field (cons maybe-char srahc) (read-char /dev/csvin))]
                   [else (values (srahc->field srahc) (csv-discard-quoted-rest /dev/csvin <#> <:> next-char strict?))])]
            [(csv-try-newline maybe-char /dev/csvin)
             => (λ [[maybe-leader : (U Char EOF)]]
                  (read-quoted-field (cons #\newline srahc) maybe-leader))]
            [else (read-quoted-field (cons maybe-char srahc) (read-char /dev/csvin))]))))

(define csv-discard-quoted-rest : (-> Input-Port (Option Char) Char (U Char EOF) Boolean (U Char Boolean))
  (lambda [/dev/csvin <#> <:> leading-char strict?]
    ;; NOTE
    ; No matter the leading and trailing whitespaces should be skipped or not,
    ; we tolerate the whitespaces around the quoted field but do not count them as part of the field.
    (let discard ([maybe-char : (U Char EOF) leading-char]
                  [valid? : Boolean #true])
      ;;; TODO: what if the quote char or escape char shows again?
      (cond [(eq? maybe-char <:>) (csv-log-if-invalid /dev/csvin valid? strict?) #true]
            [(csv-try-newline* maybe-char /dev/csvin <#>) => (csv-newline-identity (csv-log-if-invalid /dev/csvin valid? strict?))]
            [(eof-object? maybe-char) (csv-log-if-invalid /dev/csvin valid? strict?) #false]
            [else (discard (read-char /dev/csvin) (and valid? (char-blank? maybe-char)))]))))

(define csv-discard-exceeded-fields : (-> Input-Port Positive-Index Integer (Option Char) Char (Option Char) (Option Char) Boolean (Option Char))
  (lambda [/dev/csvin n count <#> <:> </> <\> strict?]
    (let skip-row ([maybe-char : (U Char EOF) (read-char /dev/csvin)]
                   [extras : (Listof CSV-Field) null]
                   [total : Integer count])
      (define-values (field more?) (csv-read-field /dev/csvin maybe-char <#> <:> </> <\> #false #false))
      (cond [(eq? more? #true) (skip-row (read-char /dev/csvin) (cons field extras) (+ total 1))]
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

(define unicode->char : (-> Fixnum Char)
  (lambda [n]
    (cond [(> n #x10FFFF) #\uFFFD] ; #\nul and max unicode
          [(<= #xD800 n #xDFFF) #\uFFFD] ; surrogate
          [else (integer->char n)])))

(define char-oct-digit? : (-> Any Boolean : #:+ Char)
  (lambda [ch]
    (and (char? ch)
         (char<=? #\0 ch #\7))))

(define char-hex-digit? : (-> Any Boolean : #:+ Char)
  (lambda [ch]
    (and (char? ch)
         (or (char-numeric? ch)
             (char-ci<=? #\a ch #\f)))))

(define char->decimal : (-> Char Fixnum)
  (lambda [hexch]
    (cond [(char<=? #\a hexch) (- (char->integer hexch) #x57)]
          [(char<=? #\A hexch) (- (char->integer hexch) #x37)]
          [else (- (char->integer hexch) #x30)])))

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
