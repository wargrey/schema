#lang typed/racket/base

;;; https://tools.ietf.org/html/rfc4180

(provide (all-defined-out))

(define-type CSV-Field String)

(define empty-field : CSV-Field "")
(define empty-row : (Listof CSV-Field) (list empty-field))

(define csv-read : (-> Input-Port Positive-Index Boolean Char Char (Option Char) (Listof (Vectorof CSV-Field)))
  (lambda [/dev/csvin n skipheader? delimiter quotechar commentchar]
    (unless (not skipheader?)
      (read-line /dev/csvin))
    (let read-row ([swor : (Listof (Vectorof CSV-Field)) null])
      (define maybe-char : (U EOF Char) (peek-char /dev/csvin))
      (cond [(eof-object? maybe-char) (reverse swor)]
            [(eqv? maybe-char commentchar) (read-line /dev/csvin) (read-row swor)]
            [else (let ([maybe-row (read-csv-row /dev/csvin n delimiter quotechar)])
                    (cond [(not maybe-row) (csv-log-syntax-error /dev/csvin "field length mismatch" 'warning) (read-row swor)]
                          [else (read-row (cons maybe-row swor))]))]))))

(define csv-read* : (-> Input-Port Boolean Char Char (Option Char) (Listof (Listof CSV-Field)))
  (lambda [/dev/csvin skipheader? delimiter quotechar commentchar]
    (unless (not skipheader?)
      (read-line /dev/csvin))
    (let read-row ([swor : (Listof (Listof CSV-Field)) null])
      (define maybe-char : (U EOF Char) (peek-char /dev/csvin))
      (cond [(eof-object? maybe-char) (reversed-rows->table swor)]
            [(eqv? maybe-char commentchar) (read-line /dev/csvin) (read-row swor)]
            [else (let ([row (read-csv-row* /dev/csvin delimiter quotechar)])
                    (read-row (cons row swor)))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define read-csv-row : (-> Input-Port Positive-Index Char Char (Option (Vectorof CSV-Field)))
  (lambda [/dev/csvin n delimiter quotechar]
    (define row : (Vectorof CSV-Field) (make-vector n empty-field))
    (let read-this-row ([idx : Index 0])
      (define-values (field more?) (csv-read-field /dev/csvin delimiter quotechar))
      (define nidx : Positive-Fixnum (+ idx 1))
      (if (and more?)
          (and (< nidx n)
               (vector-set! row idx field)
               (read-this-row nidx))
          (and (= nidx n)
               (vector-set! row idx field)
               row)))))

(define read-csv-row* : (-> Input-Port Char Char (Listof CSV-Field))
  (lambda [/dev/csvin delimiter quotechar]
    (let read-this-row ([sdleif : (Listof CSV-Field) null])
      (define-values (field more?) (csv-read-field /dev/csvin delimiter quotechar))
      (cond [(and more?) (read-this-row (cons field sdleif))]
            [else (reversed-fields->row field sdleif)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-read-field : (-> Input-Port Char Char (Values CSV-Field Boolean))
  (lambda [/dev/csvin delimiter quotechar]
    (let read-this-field ([srahc : (Listof Char) null])
      (define maybe-char : (U Char EOF) (read-char /dev/csvin))
      (cond [(eof-object? maybe-char) (values (reversed-chars->field srahc) #false)]
            [(eqv? maybe-char delimiter) (values (reversed-chars->field srahc) #true)]
            [(eqv? maybe-char quotechar) (csv-read-quoted-field /dev/csvin srahc delimiter quotechar)]
            [(csv-newline? maybe-char /dev/csvin) (values (reversed-chars->field srahc) #false)]
            [else (read-this-field (cons maybe-char srahc))]))))

(define csv-read-quoted-field : (-> Input-Port (Listof Char) Char Char (Values CSV-Field Boolean))
  (lambda [/dev/csvin leading-srahc delimiter quotechar]
    ;; NOTE
    ; No matter the leading and trailing whitespaces should be skipped or not,
    ; we tolerate the whitespaces around the quoted field but do not count them as part of the field.
    (unless (andmap char-whitespace? leading-srahc)
      (csv-log-syntax-error /dev/csvin "ignored non-whitespace chars before quote char"))
    (let read-this-quoted-field ([srahc : (Listof Char) null])
      (define maybe-char : (U Char EOF) (read-char /dev/csvin))
      (cond [(eof-object? maybe-char)
             (csv-log-syntax-error /dev/csvin "unexpected eof of file")
             (values (reversed-chars->field srahc) #false)]
            [(eqv? maybe-char quotechar)
             (define next-char : (U Char EOF) (read-char /dev/csvin))
             (cond [(eof-object? next-char) (values (reversed-chars->field srahc) #false)]
                   [(eqv? next-char delimiter) (values (reversed-chars->field srahc) #true)]
                   [(csv-newline? next-char /dev/csvin) (values (reversed-chars->field srahc) #false)]
                   [(eqv? next-char quotechar) (read-this-quoted-field (cons maybe-char srahc))]
                   [else (values (reversed-chars->field srahc) (csv-skip-quoted-rest /dev/csvin delimiter next-char))])]
            [else (read-this-quoted-field (cons maybe-char srahc))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-skip-quoted-rest : (-> Input-Port Char Char Boolean)
  (lambda [/dev/csvin delimiter leading-char]
    (let skip-this-field ([srahc : (Listof Char) (list leading-char)]
                    [valid? : Boolean (char-whitespace? leading-char)])
      (define maybe-char : (U Char EOF) (read-char /dev/csvin))
      ;;; TODO: what if the quotechar char shows again?
      (cond [(eof-object? maybe-char) (csv-log-if-invalid /dev/csvin valid?) #false]
            [(eqv? maybe-char delimiter) (csv-log-if-invalid /dev/csvin valid?) #true]
            [(csv-newline? maybe-char /dev/csvin) (csv-log-if-invalid /dev/csvin valid?) #false]
            [else (skip-this-field (cons maybe-char srahc) (and valid? (char-whitespace? maybe-char)))]))))

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

(define reversed-chars->field : (-> (Listof Char) CSV-Field)
  (lambda [srahc]
    (cond [(null? srahc) empty-field]
          [else (list->string (reverse srahc))])))

(define reversed-fields->row : (->  CSV-Field (Listof CSV-Field) (Listof CSV-Field))
  (lambda [field sdleif]
    (cond [(pair? sdleif) (reverse (cons field sdleif))]
          [(eq? field empty-field) empty-row]
          [else (list field)])))

(define reversed-rows->table : (-> (Listof (Listof CSV-Field)) (Listof (Listof CSV-Field)))
  (lambda [swor]
    (cond [(null? swor) null]
          [(csv-empty-line? (car swor)) (reverse (cdr swor))]
          [else (reverse swor)])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-topic : Symbol 'exn:csv:syntax)

(define csv-log-syntax-error : (->* (Input-Port String) (Log-Level) Void)
  (lambda [/dev/csvin brief [level 'error]]
    ;;; NOTE
    ; For the snake of performance when dealing with big CSV file,
    ; it is reasonably to suppose that the reading CSV is well-formed,
    ; thus, client applications do not need any exception handlers.
    ;
    ; After all, if one does have doubts upon the correctness of their data model,
    ; she still has chances to check whether there are syntax errors that relevant in the CSV file. 
    (define-values (line column position) (port-next-location /dev/csvin))
    (define message : String
      (cond [(and line column) (format "~a:~a:~a: ~a" (object-name /dev/csvin) line column brief)]
            [else (format "~a: ~a" (object-name /dev/csvin) brief)]))
    (log-message (current-logger) level csv-topic message #false)))

(define csv-log-if-invalid : (-> Input-Port Boolean Void)
  (lambda [/dev/csvin valid?]
    (when (not valid?)
      (csv-log-syntax-error /dev/csvin "ignored non-whitespace chars after quote char"))))
