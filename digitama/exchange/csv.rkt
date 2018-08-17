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

(define csv-read : (-> Input-Port Positive-Index Boolean CSV-Dialect Boolean (Listof (Vectorof CSV-Field)))
  (lambda [/dev/csvin n skipheader? dialect strict?]
    (define delimiter : Char (csv-dialect-delimiter dialect))
    (define quotechar : Char (csv-dialect-quotes dialect))
    (define commentchar : (Option Char) (csv-dialect-comment-char dialect))
    (define escapechar : (Option Char) (csv-dialect-escape-char dialect))
    (define trim-left? : Boolean (csv-dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (csv-dialect-skip-tailing-space? dialect))

    (unless (not skipheader?)
      (read-line /dev/csvin))

    (let read-row ([swor : (Listof (Vectorof CSV-Field)) null])
      (define maybe-char : (U EOF Char) (peek-char /dev/csvin))
      (cond [(eof-object? maybe-char) (reverse swor)]
            [(eqv? maybe-char commentchar) (read-line /dev/csvin) (read-row swor)]
            [else (let ([maybe-row (read-csv-row /dev/csvin n delimiter quotechar strict? trim-left? trim-right?)])
                    (cond [(vector? maybe-row) (read-row (cons maybe-row swor))]
                          [else (csv-length-error /dev/csvin strict?)
                                (when (and maybe-row) (read-line /dev/csvin))
                                (read-row swor)]))]))))

(define csv-read* : (-> Input-Port Boolean CSV-Dialect Boolean (Listof (Listof CSV-Field)))
  (lambda [/dev/csvin skipheader? dialect strict?]
    (define delimiter : Char (csv-dialect-delimiter dialect))
    (define quotechar : Char (csv-dialect-quotes dialect))
    (define commentchar : (Option Char) (csv-dialect-comment-char dialect))
    (define escapechar : (Option Char) (csv-dialect-escape-char dialect))
    (define trim-left? : Boolean (csv-dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (csv-dialect-skip-tailing-space? dialect))

    (unless (not skipheader?)
      (read-line /dev/csvin))

    (let read-row ([swor : (Listof (Listof CSV-Field)) null])
      (define maybe-char : (U EOF Char) (peek-char /dev/csvin))
      (cond [(eof-object? maybe-char) (swor->table swor)]
            [(eqv? maybe-char commentchar) (read-line /dev/csvin) (read-row swor)]
            [else (let ([row (read-csv-row* /dev/csvin delimiter quotechar strict? trim-left? trim-right?)])
                    (read-row (cons row swor)))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define read-csv-row : (-> Input-Port Positive-Index Char Char Boolean Boolean Boolean (U (Vectorof CSV-Field) Boolean))
  (lambda [/dev/csvin n delimiter quotechar strict? trim-left? trim-right?]
    (define row : (Vectorof CSV-Field) (make-vector n empty-field))
    (let read-this-row ([idx : Index 0])
      (define-values (field more?) (csv-read-field/trim-left /dev/csvin delimiter quotechar strict? trim-left? trim-right?))
      (define nidx : Positive-Fixnum (+ idx 1))
      (if (and more?)
          (cond [(>= nidx n) #true]
                [else (vector-set! row idx field)
                      (read-this-row nidx)])
          (cond [(< nidx n) #false]
                [else (vector-set! row idx field)
                      row])))))

(define read-csv-row* : (-> Input-Port Char Char Boolean Boolean Boolean (Listof CSV-Field))
  (lambda [/dev/csvin delimiter quotechar strict? trim-left? trim-right?]
    (let read-this-row ([sdleif : (Listof CSV-Field) null])
      (define-values (field more?) (csv-read-field/trim-left /dev/csvin delimiter quotechar strict? trim-left? trim-right?))
      (cond [(and more?) (read-this-row (cons field sdleif))]
            [else (sdleif->row field sdleif)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-read-field/trim-left : (-> Input-Port Char Char Boolean Boolean Boolean (Values CSV-Field Boolean))
  (lambda [/dev/csvin delimiter quotechar strict? trim-left? trim-right?]
    (cond [(not trim-left?) (csv-read-field /dev/csvin #false delimiter quotechar strict? trim-right?)]
          [else (let read-without-whitespace ()
                  (define maybe-char : (U Char EOF) (read-char /dev/csvin))
                  (cond [(eof-object? maybe-char) (values empty-field #false)]
                        [(eqv? maybe-char delimiter) (values empty-field #true)]
                        [(eqv? maybe-char quotechar) (csv-read-quoted-field /dev/csvin delimiter quotechar strict?)]
                        [(csv-newline? maybe-char /dev/csvin) (values empty-field #false)]
                        [(char-whitespace? maybe-char) (read-without-whitespace)]
                        [else (csv-read-field /dev/csvin maybe-char delimiter quotechar strict? trim-right?)]))])))

(define csv-read-field : (-> Input-Port (Option Char) Char Char Boolean Boolean (Values CSV-Field Boolean))
  (lambda [/dev/csvin leading-char delimiter quotechar strict? trim-right?]
    (let read-this-field ([srahc : (Listof Char) (if (not leading-char) null (list leading-char))])
      (define maybe-char : (U Char EOF) (read-char /dev/csvin))
      (cond [(eof-object? maybe-char) (values (srahc->field/trim-right srahc trim-right?) #false)]
            [(eqv? maybe-char delimiter) (values (srahc->field/trim-right srahc trim-right?) #true)]
            [(eqv? maybe-char quotechar) (csv-read-quoted-field/check /dev/csvin srahc delimiter quotechar strict?)]
            [(csv-newline? maybe-char /dev/csvin) (values (srahc->field/trim-right srahc trim-right?) #false)]
            [else (read-this-field (cons maybe-char srahc))]))))

(define csv-read-quoted-field/check : (-> Input-Port (Listof Char) Char Char Boolean (Values CSV-Field Boolean))
  (lambda [/dev/csvin leading-srahc delimiter quotechar strict?]
    ;; NOTE
    ; No matter the leading and trailing whitespaces should be skipped or not,
    ; we tolerate the whitespaces around the quoted field but do not count them as part of the field.
    (unless (for/and : Boolean ([ch : Char (in-list leading-srahc)]) (char-whitespace? ch))
      (csv-out-quotes-error /dev/csvin strict? 'before))
    (csv-read-quoted-field /dev/csvin delimiter quotechar strict?)))

(define csv-read-quoted-field : (-> Input-Port Char Char Boolean (Values CSV-Field Boolean))
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
                   [(csv-newline? next-char /dev/csvin) (values (srahc->field srahc) #false)]
                   [(eqv? next-char quotechar) (read-this-quoted-field (cons maybe-char srahc))]
                   [else (values (srahc->field srahc) (csv-skip-quoted-rest /dev/csvin delimiter next-char strict?))])]
            [else (read-this-quoted-field (cons maybe-char srahc))]))))

(define csv-skip-quoted-rest : (-> Input-Port Char Char Boolean Boolean)
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
            [(csv-newline? maybe-char /dev/csvin) (csv-log-if-invalid /dev/csvin valid? strict?) #false]
            [else (skip-this-quoted-field (cons maybe-char srahc) (and valid? (char-whitespace? maybe-char)))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-newline? : (-> Char Input-Port Boolean)
  (lambda [ch /dev/csvin]
    (and (or (and (eqv? ch #\return)
             (when (eqv? (peek-char /dev/csvin) #\linefeed)
               (read-char /dev/csvin)))
             (and (eqv? ch #\linefeed)
                  (when (eqv? (peek-char /dev/csvin) #\return)
                    (read-char /dev/csvin))))
         #true)))

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

(define swor->table : (-> (Listof (Listof CSV-Field)) (Listof (Listof CSV-Field)))
  (lambda [swor]
    (cond [(null? swor) null]
          [(csv-empty-line? (car swor)) (reverse (cdr swor))]
          [else (reverse swor)])))

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
