#lang typed/racket/base

(provide (all-defined-out))

;;; https://tools.ietf.org/html/rfc4180

(define-type CSV-Field String)

(define empty-field : CSV-Field "")

(define read-csv-row : (-> Input-Port Char Char (Values (Listof CSV-Field) Boolean))
  (lambda [/dev/csvin separator dquote]
    (let read-this-row ([sdleif : (Listof String) null])
      (define maybe-char : (U Char EOF) (read-char /dev/csvin))
      (cond [(eof-object? maybe-char) (values (reverse (cons empty-field sdleif)) #false)]
            [else (let-values ([(field more?) (csv-read-field /dev/csvin maybe-char separator dquote)])
                    (cond [(and more?) (read-this-row (cons field sdleif))]
                          [else (values (reverse (cons field sdleif))
                                        (not (eof-object? (peek-char /dev/csvin))))]))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-read-field : (-> Input-Port Char Char Char (Values CSV-Field Boolean))
  (lambda [/dev/csvin leading-char separator dquote]
    (let read-this-field ([srahc : (Listof Char) null]
                          [maybe-char : (U Char EOF) leading-char])
      (cond [(eof-object? maybe-char) (values (list->string (reverse srahc)) #false)]
            [(eqv? maybe-char separator) (values (reversed-list->field srahc) #true)]
            [(eqv? maybe-char dquote) (csv-read-dquoted-field /dev/csvin srahc separator dquote)]
            [(csv-newline? maybe-char /dev/csvin) (values (reversed-list->field srahc) #false)]
            [else (read-this-field (cons maybe-char srahc) (read-char /dev/csvin))]))))

(define csv-read-dquoted-field : (-> Input-Port (Listof Char) Char Char (Values CSV-Field Boolean))
  (lambda [/dev/csvin leading-srahc separator dquote]
    ;; NOTE
    ; No matter the leading and trailing whitespaces should be skipped or not,
    ; we tolerate the whitespaces around the quoted field but do not count them as part of the field.
    (unless (andmap char-whitespace? leading-srahc)
      (csv-log-syntax-error /dev/csvin "ignored non-whitespace chars before dquote char"))
    (let read-this-dquoted-field ([srahc : (Listof Char) null])
      (define maybe-char : (U Char EOF) (read-char /dev/csvin))
      (cond [(eof-object? maybe-char)
             (csv-log-syntax-error /dev/csvin "unexpected eof of file")
             (values (reversed-list->field srahc) #false)]
            [(eqv? maybe-char dquote)
             (define next-char : (U Char EOF) (read-char /dev/csvin))
             (cond [(eof-object? next-char) (values (reversed-list->field srahc) #false)]
                   [(eqv? next-char separator) (values (reversed-list->field srahc) #true)]
                   [(csv-newline? next-char /dev/csvin) (values (reversed-list->field srahc) #false)]
                   [(eqv? next-char dquote) (read-this-dquoted-field (cons maybe-char srahc))]
                   [else (values (reversed-list->field srahc) (csv-skip-dquoted-field /dev/csvin separator next-char))])]
            [else (read-this-dquoted-field (cons maybe-char srahc))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-skip-dquoted-field : (-> Input-Port Char Char Boolean)
  (lambda [/dev/csvin separator leading-char]
    (let skip-this-field ([srahc : (Listof Char) (list leading-char)]
                    [valid? : Boolean (char-whitespace? leading-char)])
      (define maybe-char : (U Char EOF) (read-char /dev/csvin))
      ;;; TODO: what if the dquote char shows again?
      (cond [(eof-object? maybe-char) (csv-log-if-invalid /dev/csvin valid?) #false]
            [(eqv? maybe-char separator) (csv-log-if-invalid /dev/csvin valid?) #true]
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
      (csv-log-syntax-error /dev/csvin "ignored non-whitespace chars after dquote char"))))
