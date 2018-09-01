#lang typed/racket/base

;;; https://tools.ietf.org/html/rfc4180

(provide (all-defined-out))

(require "dialect.rkt")
(require "misc.rkt")

(require racket/unsafe/ops)

(define string-newline : String (string #\newline))

(define csv-readline? : (-> Input-Port Boolean)
  (lambda [/dev/csvin]
    (not (port-counts-lines? /dev/csvin))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define in-csv-line-port : (-> Input-Port Positive-Index CSV-Dialect Boolean (Sequenceof CSV-Row))
  (lambda [/dev/csvin n dialect strict?]
    (define (read-csv [hint : CSV-Row]) : CSV-Row
      (define maybe-line : (U String EOF) (read-line /dev/csvin 'any))
      (cond [(eof-object? maybe-line) (csv-close-input-port /dev/csvin) empty-row]
            [else (let ([maybe-row (csv-extract-row /dev/csvin maybe-line 0 (string-length maybe-line) n dialect strict?)])
                    (if (not maybe-row) (read-csv empty-row) maybe-row))]))

    ((inst make-do-sequence CSV-Row CSV-Row)
     (λ [] (values values
                   read-csv
                   (read-csv empty-row)
                   (λ [v] (not (eq? v empty-row)))
                   #false
                   #false)))))

(define in-csv-line-port* : (-> Input-Port CSV-Dialect Boolean (Sequenceof CSV-Row*))
  (lambda [/dev/csvin dialect strict?]
    (define (read-csv [hint : CSV-Row*]) : CSV-Row*
      (define maybe-line : (U String EOF) (read-line /dev/csvin 'any))
      (cond [(eof-object? maybe-line) (csv-close-input-port /dev/csvin) empty-row*]
            [else (let ([maybe-row (csv-extract-row* /dev/csvin maybe-line 0 (string-length maybe-line) dialect strict?)])
                    (if (pair? maybe-row) maybe-row (read-csv empty-row*)))]))

    ((inst make-do-sequence CSV-Row* CSV-Row*)
     (λ [] (values values
                   read-csv
                   (read-csv empty-row*)
                   (λ [v] (not (eq? v empty-row*)))
                   #false
                   #false)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-readline/reverse : (-> Input-Port Positive-Index CSV-Dialect Boolean (Listof CSV-Row))
  (lambda [/dev/csvin n dialect strict?]
    (let read-csv ([swor : (Listof CSV-Row) null])
      (define maybe-line : (U String EOF) (read-line /dev/csvin 'any))
      (cond [(eof-object? maybe-line) swor]
            [else (let ([maybe-row (csv-extract-row /dev/csvin maybe-line 0 (string-length maybe-line) n dialect strict?)])
                    (read-csv (if (not maybe-row) swor (cons maybe-row swor))))]))))

(define csv-readline*/reverse : (-> Input-Port CSV-Dialect Boolean (Listof CSV-Row*))
  (lambda [/dev/csvin dialect strict?]
    (let read-csv ([swor : (Listof CSV-Row*) null])
      (define maybe-line : (U String EOF) (read-line /dev/csvin 'any))
      (cond [(eof-object? maybe-line) swor]
            [else (let ([maybe-row (csv-extract-row* /dev/csvin maybe-line 0 (string-length maybe-line) dialect strict?)])
                    (read-csv (if (pair? maybe-row) (cons maybe-row swor) swor)))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-extract-row : (-> CSV-StdIn String Index Index Positive-Index CSV-Dialect Boolean (Option CSV-Row))
  (lambda [/dev/stdin src start end n dialect strict?]
    (define row : CSV-Row (make-vector n empty-field))
    (let extract-row ([src : String src]
                      [total : Index end]
                      [pos : Index start]
                      [idx : Index 0])
      (define-values (self this-total field npos) (csv-extract-field /dev/stdin src total pos dialect strict?))
      (define nidx : Positive-Fixnum (+ idx 1))
      (if (<= npos this-total) ; has more
          (cond [(>= nidx n) (csv-skip-exceeded-fields /dev/stdin src total npos n nidx dialect strict?)]
                [else (vector-set! row idx field) (extract-row self this-total npos nidx)])
          (cond [(= nidx n) (vector-set! row idx field) row]
                [(> nidx 1) (vector-set! row idx field) (csv-log-length-error /dev/stdin src pos n idx row strict?) #false]
                [(not (eq? (vector-ref row 0) empty-field)) (csv-log-length-error /dev/stdin src pos n idx row strict?) #false]
                [(CSV-Dialect-skip-empty-line? dialect) #false]
                [else (csv-log-length-error /dev/stdin src pos n idx (vector empty-field) strict?) #false])))))
  
(define csv-extract-row* : (-> CSV-StdIn String Index Index CSV-Dialect Boolean (Listof CSV-Field))
  (lambda [/dev/stdin src start end dialect strict?]
    (let extract-row ([src : String src]
                      [total : Index end]
                      [pos : Index start]
                      [sdleif : (Listof CSV-Field) null])
      (define-values (self this-total field npos) (csv-extract-field /dev/stdin src total pos dialect strict?))
      (cond [(<= npos this-total) (extract-row self this-total npos (cons field sdleif))]
            [(pair? sdleif) (reverse (cons field sdleif))]
            [(not (eq? field empty-field)) (list field)]
            [(CSV-Dialect-skip-empty-line? dialect) null]
            [else empty-row*]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-extract-field : (-> CSV-StdIn String Index Index CSV-Dialect Boolean (Values String Index String Nonnegative-Fixnum))
  (lambda [/dev/stdin src total idx dialect strict?]
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define </> : (Option Char) (CSV-Dialect-quote-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    
    (let extract-field ([src : String src]
                        [total : Index total]
                        [start : Nonnegative-Fixnum idx]
                        [trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect)]
                        [end : Nonnegative-Fixnum idx]
                        [pos : Nonnegative-Fixnum idx]
                        [escaping? : Boolean #false]
                        [previous : (Option String) #false])
      (cond [(>= pos total) (values src total (csv-subfield /dev/stdin previous src start end escaping? </> <\>) (+ total 1))]
            [else (let ([ch : Char (string-ref src pos)]
                        [next : Nonnegative-Fixnum (+ pos 1)])
                    (cond [(eq? ch <:>) (values src total (csv-subfield /dev/stdin previous src start end escaping? </> <\>) next)]
                          [(eq? ch </>) (csv-extract-quoted-field /dev/stdin src total start next </> <\> dialect strict?)]
                          [(eq? ch <#>) (values src total (csv-subfield /dev/stdin previous src start end escaping? </> <\>) (+ total 1))]
                          [(char-blank? ch) (extract-field src total (if trim-left? next start)
                                                           trim-left? (if (CSV-Dialect-skip-trailing-space? dialect) end next)
                                                           next escaping? previous)]
                          [(eq? ch <\>) ; `#true` -> c style escape char has been set'
                           (if (< next total) ; `newline` is not following the escape char
                               (let ([escaped-next (+ pos 2)])
                                 (extract-field src total start #false escaped-next escaped-next #true previous))
                               (let ([half-field : String (csv-subfield /dev/stdin previous src start end escaping? </> <\>)]
                                     [maybe-src : (U String EOF) (csv-read-line /dev/stdin)])
                                 (cond [(string? maybe-src) (extract-field maybe-src (string-length maybe-src) 0 #false 0 0 #false half-field)]
                                       [else (csv-log-eof-error /dev/stdin src pos strict?) (values src total half-field (+ total 1))])))]
                          [else (extract-field src total start #false next next escaping? previous)]))]))))

(define csv-extract-quoted-field : (-> CSV-StdIn String Index Nonnegative-Fixnum Nonnegative-Fixnum
                                       (Option Char) (Option Char) CSV-Dialect Boolean (Values String Index String Nonnegative-Fixnum))
  (lambda [/dev/stdin src total start idx </> <\> dialect strict?]
    (if (CSV-Dialect-skip-leading-space? dialect)
        (unless (= start (- idx 1))
          (csv-log-out-quotes-error /dev/stdin src idx strict? 'before))
        (let check ([i : Fixnum (- idx 2)])
          (when (>= i start)
            (cond [(char-blank? (string-ref src i)) (check (- i 1))]
                  [else (csv-log-out-quotes-error /dev/stdin src idx strict? 'before)]))))

    (let extract-field ([src : String src]
                        [total : Index total]
                        [start : Nonnegative-Fixnum idx]
                        [end : Nonnegative-Fixnum idx]
                        [escaping? : Boolean #false]
                        [previous : (Option String) #false])
      (if (>= end total)
          (let ([half-field : String (csv-subfield /dev/stdin previous src start end escaping? </> <\>)]
                [maybe-src : (U String EOF) (csv-read-line /dev/stdin)])
            (cond [(eof-object? maybe-src) (csv-log-eof-error /dev/stdin src end strict?) (values src total half-field (+ total 1))]
                  [else (extract-field maybe-src (string-length maybe-src) 0 0 #false half-field)]))
          (let ([ch : Char (string-ref src end)]
                [next : Nonnegative-Fixnum (+ end 1)])
            (cond [(eq? ch <\>) ; `#true` -> c style escape char has been set'
                   (cond [(< next total) (extract-field src total start (+ end 2) #true previous)] ; `newline` is not following the escape char
                         [else (let ([half-field : String (csv-subfield /dev/stdin previous src start end escaping? </> <\>)]
                                     [maybe-src : (U String EOF) (csv-read-line /dev/stdin)])
                                 (cond [(string? maybe-src) (extract-field maybe-src (string-length maybe-src) 0 0 #false half-field)]
                                       [else (csv-log-eof-error /dev/stdin src end strict?) (values src total half-field (+ total 1))]))])]
                  [(eq? ch </>)
                   (cond [(>= next total) (values src total (csv-subfield /dev/stdin previous src start end escaping? </> <\>) (+ total 1))]
                         [(eq? (string-ref src next) </>) (extract-field src total start (+ end 2) #true previous)]
                         [else (values src total (csv-subfield /dev/stdin previous src start end escaping? </> <\>)
                                       (csv-skip-quoted-rest /dev/stdin src total next dialect strict?))])]
                   [else (extract-field src total start next escaping? previous)]))))))

(define csv-skip-quoted-rest : (-> CSV-StdIn String Index Nonnegative-Fixnum CSV-Dialect Boolean Nonnegative-Fixnum)
  (lambda [/dev/stdin src total idx dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    
    (let skip ([end : Nonnegative-Fixnum idx]
               [valid? : Boolean #true])
      (cond [(>= end total) (+ total 1)]
            [else (let ([ch (string-ref src end)])
                    (cond [(eq? ch <:>) (csv-log-if-invalid /dev/stdin src end valid? strict?) (+ end 1)]
                          [(eq? ch <#>) (csv-log-if-invalid /dev/stdin src end valid? strict?) total]
                          [else (skip (+ end 1) (and valid? (char-blank? ch)))]))]))))

(define csv-skip-exceeded-fields : (-> CSV-StdIn String Index Index Positive-Index Positive-Fixnum CSV-Dialect Boolean False)
  (lambda [/dev/stdin src total pos n count dialect strict?]
    (let skip-row ([src : String src]
                   [total : Index total]
                   [pos : Index pos]
                   [extras : (Listof CSV-Field) null]
                   [count : Positive-Fixnum count])
      (define-values (self this-total field npos) (csv-extract-field /dev/stdin src total pos dialect #false))
      (cond [(< npos this-total) (skip-row self this-total npos (cons field extras) (unsafe-fx+ count 1))]
            [else (csv-log-length-error /dev/stdin self this-total n (+ count 1) (reverse (cons field extras)) strict?) #false]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-read-line : (-> CSV-StdIn (U String EOF))
  (lambda [/dev/stdin]
    (cond [(input-port? /dev/stdin) (read-line /dev/stdin 'any)]
          [else eof])))

(define csv-subfield : (-> CSV-StdIn (Option String) String Nonnegative-Fixnum Nonnegative-Fixnum Boolean (Option Char) (Option Char) CSV-Field)
  (lambda [/dev/stdin previous src start end escaping? </> <\>]
    (define this-field : String
      (cond [(and escaping?) (csv-escfield /dev/stdin src start end escaping? </> <\>)]
            [(> end start) (substring src start end)]
            [else empty-field]))
    (if (string? previous) (string-append previous string-newline this-field) this-field)))

(define csv-escfield : (-> CSV-StdIn String Nonnegative-Fixnum Nonnegative-Fixnum Boolean (Option Char) (Option Char) CSV-Field)
  (lambda [/dev/stdin src start end escaping? </> <\>]
    ;;; NOTE: it cannot produce empty field
    (define dest : String (make-string (- end start)))
    (let escape ([cur : Nonnegative-Fixnum start]
                 [idx : Nonnegative-Fixnum 0])
      (cond [(>= cur end) (substring dest 0 idx)]
            [else (let ([ch : Char (string-ref src cur)])
                    (cond [(eq? ch <\>)
                           (let-values ([(escaped-char span+1) (csv-extract-escaped-char /dev/stdin src end (unsafe-fx+ cur 1))])
                             (string-set! dest idx escaped-char) (escape (unsafe-fx+ cur span+1) (unsafe-fx+ idx 1)))]
                          [(eq? ch </>) (string-set! dest idx ch) (escape (unsafe-fx+ cur 2) (unsafe-fx+ idx 1))]
                          [else (string-set! dest idx ch) (escape (unsafe-fx+ cur 1) (unsafe-fx+ idx 1))]))]))))

(define csv-extract-escaped-char : (-> CSV-StdIn String Nonnegative-Fixnum Nonnegative-Fixnum (Values Char Nonnegative-Fixnum))
  ;; https://en.wikipedia.org/wiki/Escape_sequences_in_C#Table_of_escape_sequences
  (lambda [/dev/stdin src end cur]
    (define ch : Char (string-ref src cur))
    (case ch
      [(#\a) (values #\u07 2)]
      [(#\b) (values #\u08 2)]
      [(#\f) (values #\u0C 2)]
      [(#\n) (values #\u0A 2)]
      [(#\r) (values #\u0D 2)]
      [(#\t) (values #\u09 2)]
      [(#\v) (values #\u0B 2)]
      [(#\e) (values #\u1B 2)]
      [(#\x) (csv-extract-hexadecimal-char /dev/stdin src end (unsafe-fx+ cur 1))]
      [(#\u) (csv-extract-unicode-char /dev/stdin src end (unsafe-fx+ cur 1) 4)]
      [(#\U) (csv-extract-unicode-char /dev/stdin src end (unsafe-fx+ cur 1) 8)]
      [(#\0 #\1 #\2 #\3 #\4 #\5 #\6 #\7) (csv-extract-octal-char src end cur ch)]
      [else (values ch 2)])))

(define csv-extract-octal-char : (-> String Nonnegative-Fixnum Nonnegative-Fixnum Char (Values Char Nonnegative-Fixnum))
  (lambda [src end cur leading-char]
    (let read-octal ([n : Fixnum (char->decimal leading-char)]
                     [count : Index 1])
      (define idx : Nonnegative-Fixnum (unsafe-fx+ cur count))
      (cond [(or (>= idx end) (>= count 3)) (values (unicode->char n) (+ count 1))]
            [else (let ([ch (string-ref src idx)])
                    (cond [(char-oct-digit? ch) (read-octal (unsafe-fx+ (unsafe-fxlshift n 3) (char->decimal ch)) (+ count 1))]
                          [else (values (unicode->char n) (+ count 1))]))]))))

(define csv-extract-hexadecimal-char : (-> CSV-StdIn String Nonnegative-Fixnum Nonnegative-Fixnum (Values Char Nonnegative-Fixnum))
  (lambda [/dev/stdin src end cur]
    (let read-hexa ([n : Fixnum 0]
                    [count : Nonnegative-Fixnum 0])
      (define idx : Nonnegative-Fixnum (unsafe-fx+ cur count))
      (define ch : Char (if (>= idx end) #\x (string-ref src idx)))
      (if (char-hex-digit? ch)
          (cond [(>= n #x10FFFF) (read-hexa n (unsafe-fx+ count 1))]
                [else (read-hexa (unsafe-fx+ (unsafe-fxlshift n 4) (char->decimal ch)) (unsafe-fx+ count 1))])
          (cond [(= count 0) (csv-log-escape-error /dev/stdin src idx) (values #\uFFFD 2)]
                [else (values (unicode->char n) (unsafe-fx+ count 2))])))))

(define csv-extract-unicode-char : (-> CSV-StdIn String Nonnegative-Fixnum Nonnegative-Fixnum Positive-Byte (Values Char Nonnegative-Fixnum))
  (lambda [/dev/stdin src end cur total]
    (let read-unicode ([n : Fixnum 0]
                       [count : Index 0])
      (define idx : Nonnegative-Fixnum (unsafe-fx+ cur count))
      (define ch : Char (if (>= idx end) #\x (string-ref src idx)))
      (cond [(>= count total) (values (unicode->char n) (+ count 2))]
            [(not (char-hex-digit? ch)) (csv-log-escape-error /dev/stdin src idx) (values #\uFFFD (+ count 2))]
            [(< n #x10FFFF) (read-unicode (unsafe-fx+ (unsafe-fxlshift n 4) (char->decimal ch)) (+ count 1))]
            [else (read-unicode n (+ count 1))]))))
