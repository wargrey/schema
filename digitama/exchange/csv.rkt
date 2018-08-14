#lang typed/racket/base

(provide (all-defined-out))

;;; https://tools.ietf.org/html/rfc4180

(define-type CSV-Field String)

(define empty-field : CSV-Field "")

(define csv-read : (-> Input-Port Boolean Char Char (Option Char) (Listof (Listof CSV-Field)))
  (lambda [/dev/csvin skipheader? delimiter quotechar commentchar]
    (unless (not skipheader?)
      (read-line /dev/csvin))
    (if (char? commentchar)
        (let read-row ([swor : (Listof (Listof CSV-Field)) null])
          (define maybe-char : (U EOF Char) (peek-char /dev/csvin))
          (cond [(eof-object? maybe-char) (reverse swor)]
                [(eqv? maybe-char commentchar) (read-line /dev/csvin) (read-row swor)]
                [else (let-values ([(row more?) (read-csv-row /dev/csvin delimiter quotechar)])
                        (cond [(not more?) (reverse (cons row swor))]
                              [else (read-row (cons row swor))]))]))
        (let read-row ([swor : (Listof (Listof CSV-Field)) null])
          (define-values (row more?) (read-csv-row /dev/csvin delimiter quotechar))
          (cond [(not more?) (reverse (cons row swor))]
                [else (read-row (cons row swor))])))))

(define read-csv-row : (-> Input-Port Char Char (Values (Listof CSV-Field) Boolean))
  (lambda [/dev/csvin delimiter quotechar]
    (let read-this-row ([sdleif : (Listof String) null])
      (define maybe-char : (U Char EOF) (read-char /dev/csvin))
      (cond [(eof-object? maybe-char) (values (reverse (cons empty-field sdleif)) #false)]
            [else (let-values ([(field more?) (csv-read-field /dev/csvin maybe-char delimiter quotechar)])
                    (cond [(and more?) (read-this-row (cons field sdleif))]
                          [else (values (reverse (cons field sdleif))
                                        (not (eof-object? (peek-char /dev/csvin))))]))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-read-field : (-> Input-Port Char Char Char (Values CSV-Field Boolean))
  (lambda [/dev/csvin leading-char delimiter quotechar]
    (let read-this-field ([srahc : (Listof Char) null]
                          [maybe-char : (U Char EOF) leading-char])
      (cond [(eof-object? maybe-char) (values (list->string (reverse srahc)) #false)]
            [(eqv? maybe-char delimiter) (values (reversed-list->field srahc) #true)]
            [(eqv? maybe-char quotechar) (csv-read-quoted-field /dev/csvin srahc delimiter quotechar)]
            [(csv-newline? maybe-char /dev/csvin) (values (reversed-list->field srahc) #false)]
            [else (read-this-field (cons maybe-char srahc) (read-char /dev/csvin))]))))

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
             (values (reversed-list->field srahc) #false)]
            [(eqv? maybe-char quotechar)
             (define next-char : (U Char EOF) (read-char /dev/csvin))
             (cond [(eof-object? next-char) (values (reversed-list->field srahc) #false)]
                   [(eqv? next-char delimiter) (values (reversed-list->field srahc) #true)]
                   [(csv-newline? next-char /dev/csvin) (values (reversed-list->field srahc) #false)]
                   [(eqv? next-char quotechar) (read-this-quoted-field (cons maybe-char srahc))]
                   [else (values (reversed-list->field srahc) (csv-skip-quoted-rest /dev/csvin delimiter next-char))])]
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
(define reversed-list->field : (-> (Listof Char) CSV-Field)
  (lambda [srahc]
    (cond [(null? srahc) empty-field]
          [else (list->string (reverse srahc))])))

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
