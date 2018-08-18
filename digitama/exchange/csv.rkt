#lang typed/racket/base

;;; https://tools.ietf.org/html/rfc4180
;;; https://frictionlessdata.io/specs/csv-dialect

(provide (all-defined-out))

(define-type CSV-Field String)
(define-type CSV-Dialect csv-dialect)

(define empty-field : CSV-Field "")
(define empty-row : (Listof CSV-Field) (list empty-field))

(struct csv-dialect
  ([delimiter : Char]
   [quotes : Char]
   [comment-char : (Option Char)]
   [escape-char : (Option Char)]
   [skip-leading-space? : Boolean]
   [skip-tailing-space? : Boolean])
  #:transparent)

(define csv-read/reversed : (-> Input-Port Positive-Index Boolean CSV-Dialect Boolean (Listof (Vectorof CSV-Field)))
  (lambda [/dev/csvin n skipheader? dialect strict?]
    (define delimiter : Char (csv-dialect-delimiter dialect))
    (define quotechar : Char (csv-dialect-quotes dialect))
    (define commentchar : (Option Char) (csv-dialect-comment-char dialect))
    (define escapechar : (Option Char) (csv-dialect-escape-char dialect))
    (define trim-left? : Boolean (csv-dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (csv-dialect-skip-tailing-space? dialect))

    (unless (not skipheader?)
      (read-line /dev/csvin))

    (let read-row ([swor : (Listof (Vectorof CSV-Field)) null]
                   [maybe-char : (U EOF Char) (read-char /dev/csvin)])
      (cond [(eqv? maybe-char commentchar) (read-line /dev/csvin) (read-row swor (read-char /dev/csvin))]
            [else (let-values ([(maybe-row maybe-char) (read-csv-row /dev/csvin n maybe-char delimiter quotechar strict? trim-left? trim-right?)])
                    (cond [(vector? maybe-row) (if (char? maybe-char) (read-row (cons maybe-row swor) maybe-char) (cons maybe-row swor))]
                          [else (csv-length-error /dev/csvin strict?)
                                (when (and maybe-row) (read-line /dev/csvin))
                                (if (char? maybe-char) (read-row swor maybe-char) swor)]))]))))

(define csv-read*/reversed : (-> Input-Port Boolean CSV-Dialect Boolean (Listof (Listof CSV-Field)))
  (lambda [/dev/csvin skipheader? dialect strict?]
    (define delimiter : Char (csv-dialect-delimiter dialect))
    (define quotechar : Char (csv-dialect-quotes dialect))
    (define commentchar : (Option Char) (csv-dialect-comment-char dialect))
    (define escapechar : (Option Char) (csv-dialect-escape-char dialect))
    (define trim-left? : Boolean (csv-dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (csv-dialect-skip-tailing-space? dialect))

    (unless (not skipheader?)
      (read-line /dev/csvin))

    (let read-row ([swor : (Listof (Listof CSV-Field)) null]
                   [maybe-char : (U EOF Char) (read-char /dev/csvin)])
      (cond [(eqv? maybe-char commentchar) (read-line /dev/csvin) (read-row swor (read-char /dev/csvin))]
            [else (let-values ([(row maybe-char) (read-csv-row* /dev/csvin maybe-char delimiter quotechar strict? trim-left? trim-right?)])
                    (cond [(char? maybe-char) (read-row (cons row swor) maybe-char)]
                          [(csv-empty-line? row) swor]
                          [else (cons row swor)]))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define read-csv-row : (-> Input-Port Positive-Index (U Char EOF) Char Char Boolean Boolean Boolean
                           (Values (Option (Vectorof CSV-Field)) (U Char Boolean)))
  (lambda [/dev/csvin n leading-char delimiter quotechar strict? trim-left? trim-right?]
    (define row : (Vectorof CSV-Field) (make-vector n empty-field))
    (let read-this-row ([idx : Index 0]
                        [maybe-char : (U Char EOF) leading-char])
      (define-values (field more?) (csv-read-field/trim-left /dev/csvin maybe-char delimiter quotechar strict? trim-left? trim-right?))
      (define nidx : Positive-Fixnum (+ idx 1))
      (if (eq? more? #true)
          (cond [(>= nidx n) (values #false #true)]
                [else (vector-set! row idx field)
                      (read-this-row nidx (read-char /dev/csvin))])
          (cond [(< nidx n) (values #false more?)]
                [else (vector-set! row idx field)
                      (values row more?)])))))

(define read-csv-row* : (-> Input-Port (U Char EOF) Char Char Boolean Boolean Boolean (Values (Listof CSV-Field) (Option Char)))
  (lambda [/dev/csvin leading-char delimiter quotechar strict? trim-left? trim-right?]
    (let read-this-row ([sdleif : (Listof CSV-Field) null]
                        [maybe-char : (U Char EOF) leading-char])
      (define-values (field more?) (csv-read-field/trim-left /dev/csvin maybe-char delimiter quotechar strict? trim-left? trim-right?))
      (cond [(eq? more? #true) (read-this-row (cons field sdleif) (read-char /dev/csvin))]
            [else (values (sdleif->row field sdleif) more?)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-read-field/trim-left : (-> Input-Port (U Char EOF) Char Char Boolean Boolean Boolean (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin leading-char delimiter quotechar strict? trim-left? trim-right?]
    (cond [(not trim-left?) (csv-read-field /dev/csvin leading-char delimiter quotechar strict? trim-right?)]
          [else (let read-without-whitespace ([maybe-char : (U Char EOF) leading-char])
                  (cond [(eof-object? maybe-char) (values empty-field #false)]
                        [(eqv? maybe-char delimiter) (values empty-field #true)]
                        [(eqv? maybe-char quotechar) (csv-read-quoted-field /dev/csvin delimiter quotechar strict?)]
                        [(csv-try-newline maybe-char /dev/csvin) => (csv-newline-values empty-field)]
                        [(char-whitespace? maybe-char) (read-without-whitespace (read-char /dev/csvin))]
                        [else (csv-read-field /dev/csvin maybe-char delimiter quotechar strict? trim-right?)]))])))

(define csv-read-field : (-> Input-Port (U Char EOF) Char Char Boolean Boolean (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin leading-char delimiter quotechar strict? trim-right?]
    (let read-this-field ([srahc : (Listof Char) null]
                          [maybe-char : (U Char EOF) leading-char])
      (cond [(eof-object? maybe-char) (values (srahc->field/trim-right srahc trim-right?) #false)]
            [(eqv? maybe-char delimiter) (values (srahc->field/trim-right srahc trim-right?) #true)]
            [(eqv? maybe-char quotechar) (csv-read-quoted-field/check /dev/csvin srahc delimiter quotechar strict?)]
            [(csv-try-newline maybe-char /dev/csvin) => (csv-newline-values (srahc->field/trim-right srahc trim-right?))]
            [else (read-this-field (cons maybe-char srahc) (read-char /dev/csvin))]))))

(define csv-read-quoted-field/check : (-> Input-Port (Listof Char) Char Char Boolean (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin leading-srahc delimiter quotechar strict?]
    ;; NOTE
    ; No matter the leading and trailing whitespaces should be skipped or not,
    ; we tolerate the whitespaces around the quoted field but do not count them as part of the field.
    (unless (for/and : Boolean ([ch : Char (in-list leading-srahc)]) (char-whitespace? ch))
      (csv-out-quotes-error /dev/csvin strict? 'before))
    (csv-read-quoted-field /dev/csvin delimiter quotechar strict?)))

(define csv-read-quoted-field : (-> Input-Port Char Char Boolean (Values CSV-Field (U Char Boolean)))
  (lambda [/dev/csvin delimiter quotechar strict?]
    (let read-this-quoted-field ([srahc : (Listof Char) null])
      (define maybe-char : (U Char EOF) (read-char /dev/csvin))
      (cond [(eof-object? maybe-char)
             (csv-eof-error /dev/csvin strict?)
             (values (srahc->field srahc) #false)]
            [(eqv? maybe-char quotechar)
             (define next-char : (U Char EOF) (read-char /dev/csvin))
             (cond [(eof-object? next-char) (values (srahc->field srahc) #false)]
                   [(eqv? next-char delimiter) (values (srahc->field srahc) #true)]
                   [(csv-try-newline next-char /dev/csvin) => (csv-newline-values (srahc->field srahc))]
                   [(eqv? next-char quotechar) (read-this-quoted-field (cons maybe-char srahc))]
                   [else (values (srahc->field srahc) (csv-skip-quoted-rest /dev/csvin delimiter next-char strict?))])]
            [else (read-this-quoted-field (cons maybe-char srahc))]))))

(define csv-skip-quoted-rest : (-> Input-Port Char Char Boolean (U Char Boolean))
  (lambda [/dev/csvin delimiter leading-char strict?]
    ;; NOTE
    ; No matter the leading and trailing whitespaces should be skipped or not,
    ; we tolerate the whitespaces around the quoted field but do not count them as part of the field.
    (let skip-this-quoted-field ([srahc : (Listof Char) (list leading-char)]
                                 [valid? : Boolean (char-whitespace? leading-char)])
      (define maybe-char : (U Char EOF) (read-char /dev/csvin))
      ;;; TODO: what if the quotechar char shows again?
      (cond [(eof-object? maybe-char) (csv-log-if-invalid /dev/csvin valid? strict?) #false]
            [(eqv? maybe-char delimiter) (csv-log-if-invalid /dev/csvin valid? strict?) #true]
            [(csv-try-newline maybe-char /dev/csvin) => (csv-newline-identity (csv-log-if-invalid /dev/csvin valid? strict?))]
            [else (skip-this-quoted-field (cons maybe-char srahc) (and valid? (char-whitespace? maybe-char)))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-try-newline : (-> Char Input-Port (U Char EOF False))
  (lambda [ch /dev/csvin]
    (cond [(eq? ch #\return)
           (define next-char : (U Char EOF) (read-char /dev/csvin))
           (if (eq? next-char #\linefeed) (read-char /dev/csvin) next-char)]
          [(eq? ch #\linefeed)
           (define next-char : (U Char EOF) (read-char /dev/csvin))
           (if (eq? next-char #\return) (read-char /dev/csvin) next-char)]
          [else #false])))

(define csv-newline-identity : (All (a) (-> a (-> (U Char EOF) (Option Char))))
  (lambda [v]
    (λ [[maybe-char : (U Char EOF)]]
      (and (char? maybe-char) maybe-char))))

(define csv-newline-values : (All (a) (-> a (-> (U Char EOF) (Values a (Option Char)))))
  (lambda [v]
    (λ [[maybe-char : (U Char EOF)]]
      (values v (and (char? maybe-char) maybe-char)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-empty-line? : (-> (Listof CSV-Field) Boolean)
  (lambda [fields]
    (eq? fields empty-row)))

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
          [(eq? field empty-field) empty-row]
          [else (list field)])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-topic : Symbol 'exn:csv:syntax)

(define csv-length-error : (-> Input-Port Boolean Void)
  (lambda [/dev/csvin strict?]
    (csv-log-syntax-error /dev/csvin 'error strict? "field length mismatch")))

(define csv-eof-error : (-> Input-Port Boolean Void)
  (lambda [/dev/csvin strict?]
    (csv-log-syntax-error /dev/csvin 'warning strict? "unexpected eof of file")))

(define csv-out-quotes-error : (-> Input-Port Boolean Symbol Void)
  (lambda [/dev/csvin strict? position]
    (csv-log-syntax-error /dev/csvin 'warning strict?
                          (format "ignored non-whitespace chars ~a quote char" position))))

(define csv-log-if-invalid : (-> Input-Port Boolean Boolean Void)
  (lambda [/dev/csvin valid? strict?]
    (when (not valid?)
      (csv-out-quotes-error /dev/csvin strict? 'after))))


(define csv-log-syntax-error : (-> Input-Port Log-Level Boolean String Void)
  (lambda [/dev/csvin level strict? brief]
    (define-values (line column position) (port-next-location /dev/csvin))
    (define message : String
      (cond [(and line column) (format "~a:~a:~a: ~a" (object-name /dev/csvin) line column brief)]
            [else (format "~a: ~a" (object-name /dev/csvin) brief)]))
    (log-message (current-logger) level csv-topic message #false)
    (unless (not strict?)
      (raise-user-error 'csv "~a" message))))
