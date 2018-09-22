#lang typed/racket/base

;;; https://tools.ietf.org/html/rfc4180

(provide (all-defined-out))

(require "../dialect.rkt")
(require "progress.rkt")
(require "misc.rkt")

(require racket/unsafe/ops)

(define string-newline : String (string #\newline))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define in-csv-line-port : (-> Input-Port Positive-Index CSV-Dialect Boolean Boolean Boolean (Option Symbol) (Sequenceof CSV-Row))
  (lambda [/dev/csvin n dialect skip-header? strict? trim-line? maybe-topic]
    (define maybe-progress-handler : Maybe-CSV-Progress-Handler (default-csv-progress-handler))
    (define topic : Symbol (or maybe-topic ((default-csv-progress-topic-resolver) /dev/csvin)))
    
    (define sentinel : CSV-Row (vector empty-field))
    (define (read-csv [hint : CSV-Row]) : CSV-Row
      (define maybe-line : (U String EOF) (read-line /dev/csvin 'any))
      (cond [(eof-object? maybe-line) (csv-report-final-progress /dev/csvin maybe-progress-handler topic #true) sentinel]
            [else (let* ([eol (string-length maybe-line)]
                         [maybe-row (csv-extract-row /dev/csvin maybe-line eol n dialect strict? trim-line? maybe-progress-handler topic)])
                    (if (not maybe-row) (read-csv sentinel) maybe-row))]))

    (unless (not skip-header?) (read-line /dev/csvin))
    ((inst make-do-sequence CSV-Row CSV-Row)
     (λ [] (values values
                   read-csv
                   (read-csv empty-row)
                   (λ [v] (not (eq? v sentinel)))
                   #false
                   #false)))))

(define in-csv-line-port* : (-> Input-Port CSV-Dialect Boolean Boolean Boolean (Option Symbol) (Sequenceof CSV-Row*))
  (lambda [/dev/csvin dialect skip-header? strict? trim-line? maybe-topic]
    (define maybe-progress-handler : Maybe-CSV-Progress-Handler (default-csv-progress-handler))
    (define topic : Symbol (or maybe-topic ((default-csv-progress-topic-resolver) /dev/csvin)))
    
    (define sentinel : CSV-Row* (list empty-field))
    (define (read-csv [hint : CSV-Row*]) : CSV-Row*
      (define maybe-line : (U String EOF) (read-line /dev/csvin 'any))
      (cond [(eof-object? maybe-line) (csv-report-final-progress /dev/csvin maybe-progress-handler topic #true) sentinel]
            [else (let* ([eol (string-length maybe-line)]
                         [maybe-row (csv-extract-row* /dev/csvin maybe-line eol dialect strict? trim-line? maybe-progress-handler topic)])
                    (if (null? maybe-row) (read-csv sentinel) maybe-row))]))

    (unless (not skip-header?) (read-line /dev/csvin))
    ((inst make-do-sequence CSV-Row* CSV-Row*)
     (λ [] (values values
                   read-csv
                   (read-csv sentinel)
                   (λ [v] (not (eq? v sentinel)))
                   #false
                   #false)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-readline/reverse : (-> Input-Port Positive-Index CSV-Dialect Boolean Boolean Boolean (Option Symbol) (Listof CSV-Row))
  (lambda [/dev/csvin n dialect skip-header? strict? trim-line? maybe-topic]
    (unless (not skip-header?) (read-line /dev/csvin))

    (define maybe-progress-handler : Maybe-CSV-Progress-Handler (default-csv-progress-handler))
    (define topic : Symbol (or maybe-topic ((default-csv-progress-topic-resolver) /dev/csvin)))
    
    (let read-csv ([swor : (Listof CSV-Row) null])
      (define maybe-line : (U String EOF) (read-line /dev/csvin 'any))
      (cond [(eof-object? maybe-line) (csv-report-final-progress /dev/csvin maybe-progress-handler topic #false) swor]
            [else (let* ([eol (string-length maybe-line)]
                         [maybe-row (csv-extract-row /dev/csvin maybe-line eol n dialect strict? trim-line? maybe-progress-handler topic)])
                    (read-csv (if (not maybe-row) swor (cons maybe-row swor))))]))))

(define csv-readline*/reverse : (-> Input-Port CSV-Dialect Boolean Boolean Boolean (Option Symbol) (Listof CSV-Row*))
  (lambda [/dev/csvin dialect skip-header? strict? trim-line? maybe-topic]
    (unless (not skip-header?) (read-line /dev/csvin))
    
    (define maybe-progress-handler : Maybe-CSV-Progress-Handler (default-csv-progress-handler))
    (define topic : Symbol (or maybe-topic ((default-csv-progress-topic-resolver) /dev/csvin)))
    
    (let read-csv ([swor : (Listof CSV-Row*) null])
      (define maybe-line : (U String EOF) (read-line /dev/csvin 'any))
      (cond [(eof-object? maybe-line) (csv-report-final-progress /dev/csvin maybe-progress-handler topic #false) swor]
            [else (let* ([eol (string-length maybe-line)]
                         [maybe-row (csv-extract-row* /dev/csvin maybe-line eol dialect strict? trim-line? maybe-progress-handler topic)])
                    (read-csv (if (null? maybe-row) swor (cons maybe-row swor))))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-extract-row : (-> Input-Port String Index Positive-Index CSV-Dialect Boolean Boolean
                              Maybe-CSV-Progress-Handler Symbol
                              (Option CSV-Row))
  (lambda [/dev/csvin src eol n dialect strict? trim-line? maybe-progress-handler topic]
    (csv-report-progress /dev/csvin maybe-progress-handler topic)

    (define row : CSV-Row (make-vector n empty-field))
    (let extract-row ([src : String src]
                      [eol : Index eol]
                      [pos : Index 0]
                      [count : Index 0])
      (define-values (self this-eol field npos) (csv-extract-field /dev/csvin src eol pos dialect strict?))
      (define ncount : Positive-Fixnum (+ count 1))
      (if (<= npos this-eol) ; has more
          (cond [(>= ncount n) (csv-report-exceeded-fields /dev/csvin src eol npos n ncount dialect strict?)]
                [else (vector-set! row count field) (extract-row self this-eol npos ncount)])
          (cond [(= ncount n) (vector-set! row count field) row]
                [(> ncount 1) (vector-set! row count field) (csv-log-length-error /dev/csvin src pos n ncount row strict?) #false]
                [(not (eq? field empty-field)) (csv-log-length-error /dev/csvin src pos n ncount (vector field) strict?) #false]
                [(or trim-line? (eof-object? (peek-char /dev/csvin))) #false]
                [else (csv-log-length-error /dev/csvin src pos n ncount (vector empty-field) strict?) #false])))))
  
(define csv-extract-row* : (-> Input-Port String Index CSV-Dialect Boolean Boolean
                               Maybe-CSV-Progress-Handler Symbol
                               (Listof CSV-Field))
  (lambda [/dev/csvin src eol dialect strict? trim-line? maybe-progress-handler topic]
    (csv-report-progress /dev/csvin maybe-progress-handler topic)

    (let extract-row ([src : String src]
                      [eol : Index eol]
                      [pos : Index 0]
                      [sdleif : (Listof CSV-Field) null])
      (define-values (self this-eol field npos) (csv-extract-field /dev/csvin src eol pos dialect strict?))
      (cond [(<= npos this-eol) (extract-row self this-eol npos (cons field sdleif))]
            [(pair? sdleif) (reverse (cons field sdleif))]
            [(not (eq? field empty-field)) (list field)]
            [(or trim-line? (eof-object? (peek-char /dev/csvin))) null]
            [else empty-row*]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-extract-field : (-> Input-Port String Index Index CSV-Dialect Boolean (Values String Index String Nonnegative-Fixnum))
  (lambda [/dev/csvin src eol idx dialect strict?]
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define </> : (Option Char) (CSV-Dialect-quote-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    
    (let extract-field ([src : String src]
                        [eol : Index eol]
                        [start : Nonnegative-Fixnum idx]
                        [trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect)]
                        [end : Nonnegative-Fixnum idx]
                        [pos : Nonnegative-Fixnum idx]
                        [escaping? : Boolean #false]
                        [previous : (Option String) #false])
      (cond [(>= pos eol) (values src eol (csv-subfield /dev/csvin previous src start end escaping? </> <\>) (+ eol 1))]
            [else (let ([ch : Char (string-ref src pos)]
                        [next : Nonnegative-Fixnum (+ pos 1)])
                    (cond [(eq? ch <:>) (values src eol (csv-subfield /dev/csvin previous src start end escaping? </> <\>) next)]
                          [(eq? ch </>) (csv-extract-quoted-field /dev/csvin src eol start next </> <\> dialect strict?)]
                          [(eq? ch <#>) (values src eol (csv-subfield /dev/csvin previous src start end escaping? </> <\>) (+ eol 1))]
                          [(eq? ch <\>) ; `#true` => c style escape char has been set
                           (if (< next eol) ; `newline` is not following the escape char
                               (let ([escaped-next (+ pos 2)])
                                 (extract-field src eol start #false escaped-next escaped-next #true previous))
                               (let ([half-field : String (csv-subfield /dev/csvin previous src start end escaping? </> <\>)]
                                     [maybe-src : (U String EOF) (read-line /dev/csvin 'any)])
                                 (cond [(string? maybe-src) (extract-field maybe-src (string-length maybe-src) 0 #false 0 0 #false half-field)]
                                       [else (csv-log-eof-error /dev/csvin src next strict?) (values src eol half-field (+ eol 1))])))]
                          [(char-blank? ch) (extract-field src eol (if trim-left? next start) trim-left?
                                                           (if (CSV-Dialect-skip-trailing-space? dialect) end next)
                                                           next escaping? previous)]
                          [else (extract-field src eol start #false next next escaping? previous)]))]))))

(define csv-extract-quoted-field : (-> Input-Port String Index Nonnegative-Fixnum Nonnegative-Fixnum
                                       (Option Char) (Option Char) CSV-Dialect Boolean (Values String Index String Nonnegative-Fixnum))
  (lambda [/dev/csvin src eol start idx </> <\> dialect strict?]
    (let check ([i : Fixnum (- idx 2)])
      (when (>= i start)
        (cond [(char-blank? (string-ref src i)) (check (- i 1))]
              [else (csv-log-out-quotes-error /dev/csvin src idx strict? 'before)])))

    (let extract-field ([src : String src]
                        [eol : Index eol]
                        [start : Nonnegative-Fixnum idx]
                        [end : Nonnegative-Fixnum idx]
                        [escaping? : Boolean #false]
                        [previous : (Option String) #false])
      (if (>= end eol)
          (let ([half-field : String (csv-subfield /dev/csvin previous src start end escaping? </> <\>)]
                [maybe-src : (U String EOF) (read-line /dev/csvin 'any)])
            (cond [(string? maybe-src) (extract-field maybe-src (string-length maybe-src) 0 0 #false half-field)]
                  [else (csv-log-eof-error /dev/csvin src end strict?) (values src eol half-field (+ eol 1))]))
          (let ([ch : Char (string-ref src end)]
                [next : Nonnegative-Fixnum (+ end 1)])
            (cond [(eq? ch </>)
                   (cond [(>= next eol) (values src eol (csv-subfield /dev/csvin previous src start end escaping? </> <\>) (+ eol 1))]
                         [(and (not <\>) (eq? (string-ref src next) </>)) (extract-field src eol start (+ end 2) #true previous)]
                         [else (values src eol (csv-subfield /dev/csvin previous src start end escaping? </> <\>)
                                       (csv-omit-quoted-rest /dev/csvin src eol next dialect strict?))])]
                  [(eq? ch <\>) ; `#true` => c style escape char has been set
                   (cond [(< next eol) (extract-field src eol start (+ end 2) #true previous)]
                         [else (let ([half-field : String (csv-subfield /dev/csvin previous src start end escaping? </> <\>)]
                                     [maybe-src : (U String EOF) (read-line /dev/csvin 'any)])
                                 (cond [(string? maybe-src) (extract-field maybe-src (string-length maybe-src) 0 0 #false half-field)]
                                       [else (csv-log-eof-error /dev/csvin src next strict?) (values src eol half-field (+ eol 1))]))])]
                  [else (extract-field src eol start next escaping? previous)]))))))

(define csv-omit-quoted-rest : (-> Input-Port String Index Nonnegative-Fixnum CSV-Dialect Boolean Nonnegative-Fixnum)
  (lambda [/dev/csvin src eol idx dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    
    (let skip ([end : Nonnegative-Fixnum idx]
               [valid? : Boolean #true])
      (cond [(>= end eol) (+ eol 1)]
            [else (let ([ch (string-ref src end)])
                    (cond [(eq? ch <:>) (csv-log-if-invalid /dev/csvin src end valid? strict?) (+ end 1)]
                          [(eq? ch <#>) (csv-log-if-invalid /dev/csvin src end valid? strict?) eol]
                          [else (skip (+ end 1) (and valid? (char-blank? ch)))]))]))))

(define csv-report-exceeded-fields : (-> Input-Port String Index Index Positive-Index Positive-Fixnum CSV-Dialect Boolean False)
  (lambda [/dev/csvin src eol pos n count dialect strict?]
    (let skip-row ([src : String src]
                   [eol : Index eol]
                   [pos : Index pos]
                   [extras : (Listof CSV-Field) null]
                   [total : Positive-Fixnum count])
      (define-values (self this-eol field npos) (csv-extract-field /dev/csvin src eol pos dialect #false))
      (cond [(< npos this-eol) (skip-row self this-eol npos (cons field extras) (unsafe-fx+ total 1))]
            [else (csv-log-length-error /dev/csvin self this-eol n (unsafe-fx+ total 1) (reverse (cons field extras)) strict?) #false]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-subfield : (-> CSV-StdIn* (Option String) String Nonnegative-Fixnum Nonnegative-Fixnum Boolean (Option Char) (Option Char) CSV-Field)
  (lambda [/dev/csvin previous src start end escaping? </> <\>]
    (define this-field : String
      (cond [(<= end start) empty-field]
            [(not escaping?) (substring src start end)]
            [else (csv-escfield /dev/csvin src start end </> <\>)]))
    (if (string? previous) (string-append previous string-newline this-field) this-field)))

(define csv-escfield : (-> CSV-StdIn* String Nonnegative-Fixnum Nonnegative-Fixnum (Option Char) (Option Char) CSV-Field)
  (lambda [/dev/csvin src start end </> <\>]
    ;;; NOTE: it cannot produce empty field
    (define dest : String (make-string (- end start)))
    (let escape ([cur : Nonnegative-Fixnum start]
                 [idx : Nonnegative-Fixnum 0])
      (cond [(>= cur end) (substring dest 0 idx)]
            [else (let ([ch : Char (string-ref src cur)]
                        [ncur : Nonnegative-Fixnum (unsafe-fx+ cur 1)])
                    (cond [(eq? ch <\>)
                           (let-values ([(escaped-char span) (csv-extract-escaped-char /dev/csvin src end ncur)])
                             (string-set! dest idx escaped-char) (escape (unsafe-fx+ ncur span) (unsafe-fx+ idx 1)))]
                          [(eq? ch </>) (string-set! dest idx ch) (escape (unsafe-fx+ ncur 1) (unsafe-fx+ idx 1))]
                          [(eq? ch #\return)
                           (let ([span (if (and (< ncur end) (eq? (string-ref src ncur) #\linefeed)) 1 0)])
                             (string-set! dest idx #\newline) (escape (unsafe-fx+ ncur span) (unsafe-fx+ idx 1)))]
                          [else (string-set! dest idx ch) (escape ncur (unsafe-fx+ idx 1))]))]))))

(define csv-extract-escaped-char : (-> CSV-StdIn* String Nonnegative-Fixnum Nonnegative-Fixnum (Values Char Nonnegative-Fixnum))
  ;; https://en.wikipedia.org/wiki/Escape_sequences_in_C#Table_of_escape_sequences
  (lambda [/dev/csvin src end cur]
    (define ch : Char (string-ref src cur))
    (case ch
      [(#\a) (values #\u07 1)]
      [(#\b) (values #\u08 1)]
      [(#\f) (values #\u0C 1)]
      [(#\n) (values #\u0A 1)]
      [(#\r) (values #\u0D 1)]
      [(#\t) (values #\u09 1)]
      [(#\v) (values #\u0B 1)]
      [(#\e) (values #\u1B 1)]
      [(#\x) (csv-extract-hexadecimal-char /dev/csvin src end (unsafe-fx+ cur 1))]
      [(#\u) (csv-extract-unicode-char /dev/csvin src end (unsafe-fx+ cur 1) 4)]
      [(#\U) (csv-extract-unicode-char /dev/csvin src end (unsafe-fx+ cur 1) 8)]
      [(#\0 #\1 #\2 #\3 #\4 #\5 #\6 #\7) (csv-extract-octal-char src end cur ch)]
      [(#\return) (values #\newline ; also #\linefeed
                          (let ([ncur (unsafe-fx+ cur 1)])
                            (cond [(>= ncur end) 1]
                                  [(eq? (string-ref src ncur) #\linefeed) 2]
                                  [else 1])))]
      [else (values ch 1)])))

(define csv-extract-octal-char : (-> String Nonnegative-Fixnum Nonnegative-Fixnum Char (Values Char Nonnegative-Fixnum))
  (lambda [src end cur leading-char]
    (let read-octal ([n : Fixnum (char->decimal leading-char)]
                     [count : Index 1])
      (define idx : Nonnegative-Fixnum (unsafe-fx+ cur count))
      (cond [(or (>= idx end) (>= count 3)) (values (unicode->char n) count)]
            [else (let ([ch (string-ref src idx)])
                    (cond [(char-oct-digit? ch) (read-octal (unsafe-fx+ (unsafe-fxlshift n 3) (char->decimal ch)) (+ count 1))]
                          [else (values (unicode->char n) count)]))]))))

(define csv-extract-hexadecimal-char : (-> CSV-StdIn* String Nonnegative-Fixnum Nonnegative-Fixnum (Values Char Nonnegative-Fixnum))
  (lambda [/dev/csvin src end cur]
    (let read-hexa ([n : Fixnum 0]
                    [count : Nonnegative-Fixnum 0])
      (define idx : Nonnegative-Fixnum (unsafe-fx+ cur count))
      (define ch : Char (if (>= idx end) #\x (string-ref src idx)))
      (if (char-hex-digit? ch)
          (cond [(>= n #x10FFFF) (read-hexa n (unsafe-fx+ count 1))]
                [else (read-hexa (unsafe-fx+ (unsafe-fxlshift n 4) (char->decimal ch)) (unsafe-fx+ count 1))])
          (cond [(= count 0) (csv-log-escape-error /dev/csvin src idx) (values #\uFFFD 1)]
                [else (values (unicode->char n) (unsafe-fx+ count 1))])))))

(define csv-extract-unicode-char : (-> CSV-StdIn* String Nonnegative-Fixnum Nonnegative-Fixnum Positive-Byte
                                       (Values Char Nonnegative-Fixnum))
  (lambda [/dev/csvin src end cur total]
    (let read-unicode ([n : Fixnum 0]
                       [count : Index 0])
      (define idx : Nonnegative-Fixnum (unsafe-fx+ cur count))
      (define ch : Char (if (>= idx end) #\x (string-ref src idx)))
      (cond [(>= count total) (values (unicode->char n) (+ count 1))]
            [(not (char-hex-digit? ch)) (csv-log-escape-error /dev/csvin src idx) (values #\uFFFD (+ count 1))]
            [(< n #x10FFFF) (read-unicode (unsafe-fx+ (unsafe-fxlshift n 4) (char->decimal ch)) (+ count 1))]
            [else (read-unicode n (+ count 1))]))))
