#lang typed/racket/base

;;; https://tools.ietf.org/html/rfc4180

(provide (all-defined-out))

(require "misc.rkt")

(require racket/unsafe/ops)

(define string-newline : String (string #\newline))

(define csv-readline? : (-> Input-Port Boolean)
  (lambda [/dev/csvin]
    (not (port-counts-lines? /dev/csvin))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define in-csv-line-port : (-> Input-Port Positive-Index CSV-Dialect Boolean (Sequenceof (Vectorof CSV-Field)))
  (lambda [/dev/csvin n dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define </> : (Option Char) (CSV-Dialect-quote-char dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    (define trim-line? : Boolean (CSV-Dialect-skip-empty-line? dialect))
    (define trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (CSV-Dialect-skip-trailing-space? dialect))

    (define (read-csv [hint : (Vectorof CSV-Field)]) : (Vectorof CSV-Field)
      (define maybe-line : (U String EOF) (csv-read-line /dev/csvin <#>))
      (cond [(eof-object? maybe-line) (csv-close-input-port /dev/csvin) empty-row]
            [else (let ([maybe-row (line->csv-row /dev/csvin maybe-line n <#> <:> </> <\> strict? trim-line? trim-left? trim-right?)])
                    (if (not maybe-row) (read-csv empty-row) maybe-row))]))

    ((inst make-do-sequence (Vectorof CSV-Field) (Vectorof CSV-Field))
     (λ [] (values values
                   read-csv
                   (read-csv empty-row)
                   (λ [v] (not (eq? v empty-row)))
                   #false
                   #false)))))

(define in-csv-line-port* : (-> Input-Port CSV-Dialect Boolean (Sequenceof CSV-Row*))
  (lambda [/dev/csvin dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define </> : (Option Char) (CSV-Dialect-quote-char dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    (define trim-line? : Boolean (CSV-Dialect-skip-empty-line? dialect))
    (define trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (CSV-Dialect-skip-trailing-space? dialect))

    (define (read-csv [hint : CSV-Row*]) : CSV-Row*
      (define maybe-line : (U String EOF) (csv-read-line /dev/csvin <#>))
      (cond [(eof-object? maybe-line) (csv-close-input-port /dev/csvin) empty-row*]
            [else (let ([maybe-row (line->csv-row* /dev/csvin maybe-line <#> <:> </> <\> strict? trim-line? trim-left? trim-right?)])
                    (if (pair? maybe-row) maybe-row (read-csv empty-row*)))]))

    ((inst make-do-sequence CSV-Row* CSV-Row*)
     (λ [] (values values
                   read-csv
                   (read-csv empty-row*)
                   (λ [v] (not (eq? v empty-row*)))
                   #false
                   #false)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-readline/reverse : (-> Input-Port Positive-Index CSV-Dialect Boolean (Listof (Vectorof CSV-Field)))
  (lambda [/dev/csvin n dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define </> : (Option Char) (CSV-Dialect-quote-char dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    (define trim-line? : Boolean (CSV-Dialect-skip-empty-line? dialect))
    (define trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (CSV-Dialect-skip-trailing-space? dialect))
    
    (let read-csv ([swor : (Listof (Vectorof CSV-Field)) null])
      (define maybe-line : (U String EOF) (csv-read-line /dev/csvin <#>))
      (cond [(eof-object? maybe-line) swor]
            [else (let ([maybe-row (line->csv-row /dev/csvin maybe-line n <#> <:> </> <\> strict? trim-line? trim-left? trim-right?)])
                    (read-csv (if (not maybe-row) swor (cons maybe-row swor))))]))))

(define csv-readline*/reverse : (-> Input-Port CSV-Dialect Boolean (Listof CSV-Row*))
  (lambda [/dev/csvin dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define </> : (Option Char) (CSV-Dialect-quote-char dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    (define trim-line? : Boolean (CSV-Dialect-skip-empty-line? dialect))
    (define trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect))
    (define trim-right? : Boolean (CSV-Dialect-skip-trailing-space? dialect))

    (let read-csv ([swor : (Listof CSV-Row*) null])
      (define maybe-line : (U String EOF) (csv-read-line /dev/csvin <#>))
      (cond [(eof-object? maybe-line) swor]
            [else (let ([maybe-row (line->csv-row* /dev/csvin maybe-line <#> <:> </> <\> strict? trim-line? trim-left? trim-right?)])
                    (read-csv (if (pair? maybe-row) (cons maybe-row swor) swor)))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define line->csv-row : (-> Input-Port String Positive-Index (Option Char) Char (Option Char) (Option Char) Boolean Boolean Boolean Boolean
                            (Option (Vectorof CSV-Field)))
  (lambda [/dev/csvin raw n <#> <:> </> <\> strict? trim-line? trim-left? trim-right?]
    (define row : (Vectorof CSV-Field) (make-vector n empty-field))
    (let extract-row ([raw : String raw]
                      [total : Index (string-length raw)]
                      [pos : Index 0]
                      [idx : Index 0])
      (define-values (self this-total field npos) (csv-extract-field /dev/csvin raw total pos <#> <:> </> <\> strict? trim-left? trim-right?))
      (define nidx : Nonnegative-Fixnum (+ idx 1))
      (if (<= npos this-total) ; has more
          (cond [(>= nidx n) #false #|(csv-discard-exceeded-fields /dev/csvin n nidx <#> <:> </> <\> strict?)|#]
                [else (vector-set! row idx field) (extract-row self this-total npos nidx)])
          (cond [(= nidx n) row]
                [(> nidx 1) (csv-log-length-error /dev/csvin n nidx row strict?) #false]
                [(not (eq? (vector-ref row 0) empty-field)) (csv-log-length-error /dev/csvin n nidx row strict?) #false]
                [(not trim-line?) (csv-log-length-error /dev/csvin n nidx (vector empty-field) strict?) #false]
                [else #false])))))
  
(define line->csv-row* : (-> Input-Port String (Option Char) Char (Option Char) (Option Char) Boolean Boolean Boolean Boolean (Listof CSV-Field))
  (lambda [/dev/csvin raw <#> <:> </> <\> strict? trim-line? trim-left? trim-right?]
    (let extract-row ([raw : String raw]
                      [total : Index (string-length raw)]
                      [pos : Index 0]
                      [sdleif : (Listof CSV-Field) null])
      (define-values (self this-total field npos) (csv-extract-field /dev/csvin raw total pos <#> <:> </> <\> strict? trim-left? trim-right?))
      (cond [(<= npos this-total) (extract-row self this-total npos (cons field sdleif))]
            [(pair? sdleif) (reverse (cons field sdleif))]
            [(not (eq? field empty-field)) (list field)]
            [(not trim-line?) empty-row*]
            [else null]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-extract-field : (-> Input-Port String Index Index (Option Char) Char (Option Char) (Option Char) Boolean Boolean Boolean
                                (Values String Index String Nonnegative-Fixnum))
  (lambda [/dev/csvin raw total idx <#> <:> </> <\> strict? trim-left? trim-right?]
    (let extract-field ([start : Nonnegative-Fixnum idx]
                        [trim-left? : Boolean trim-left?]
                        [end : Nonnegative-Fixnum idx]
                        [pos : Nonnegative-Fixnum idx]
                        [escaping? : Boolean #false])
      (cond [(>= pos total) (values raw total (csv-subfield raw start end escaping? </> <\>) (+ total 1))]
            [else (let ([ch : Char (string-ref raw pos)]
                        [next : Nonnegative-Fixnum (+ pos 1)])
                    (cond [(eq? ch <:>) (values raw total (csv-subfield raw start end escaping? </> <\>) next)]
                          [(eq? ch </>) (csv-extract-quoted-field /dev/csvin raw total idx start next <#> <:> </> <\> strict?)]
                          [(eq? ch <#>) (values raw total (csv-subfield raw start end escaping? </> <\>) (+ total 1))]
                          [(char-blank? ch) (extract-field (if trim-left? next start) trim-left? (if trim-right? end next) next escaping?)]
                          [(eq? ch <\>) ; `#true` -> c style escape char has been set'
                           (let ([escaped-next (+ pos 2)]) (extract-field start #false escaped-next escaped-next #true))]
                          [else (extract-field start #false next next escaping?)]))]))))

(define csv-extract-quoted-field : (-> Input-Port String Index Index Nonnegative-Fixnum Nonnegative-Fixnum
                                       (Option Char) Char (Option Char) (Option Char) Boolean
                                       (Values String Index String Nonnegative-Fixnum))
  (lambda [/dev/csvin raw total idx0 start idx <#> <:> </> <\> strict?]
    (unless (or (= start idx0) (= start (sub1 idx)))
      (csv-log-out-quotes-error /dev/csvin strict? 'before))
    (let extract-field ([raw : String raw]
                        [total : Index total]
                        [start : Nonnegative-Fixnum idx]
                        [end : Nonnegative-Fixnum idx]
                        [escaping? : Boolean #false]
                        [previous : (Option String) #false])
      (if (>= end total)
          (let ([half-field : String (csv-subfield* previous raw start end escaping? </> <\>)]
                [maybe-raw : (U String EOF) (read-line /dev/csvin 'any)])
            (cond [(eof-object? maybe-raw) (csv-log-eof-error /dev/csvin strict?) (values raw total half-field (+ total 1))]
                  [else (extract-field maybe-raw (string-length maybe-raw) 0 0 #false half-field)]))
          (let ([ch : Char (string-ref raw end)]
                [next : Nonnegative-Fixnum (+ end 1)])
            (cond [(eq? ch <\>) ; `#true` -> c style escape char has been set'
                   (extract-field raw total start (+ end 2) #true previous)]
                  [(eq? ch </>)
                   (cond [(>= next total) (values raw total (csv-subfield* previous raw start end escaping? </> <\>) (+ total 1))]
                         [(eq? (string-ref raw next) </>) (extract-field raw total start (+ end 2) #true previous)]
                         [else (values raw total (csv-subfield* previous raw start end escaping? </> <\>)
                                       (csv-skip-quoted-rest raw total next <:>))])]
                   [else (extract-field raw total start next escaping? previous)]))))))

(define csv-skip-quoted-rest : (-> String Index Nonnegative-Fixnum Char Nonnegative-Fixnum)
  (lambda [raw total idx <:>]
    (let skip ([end : Nonnegative-Fixnum idx])
      (cond [(>= end total) (+ total 1)]
            [(eq? (string-ref raw end) <:>) (+ end 1)]
            [else (skip (+ end 1))]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-read-line : (-> Input-Port (Option Char) (U String EOF))
  (lambda [/dev/csvin <#>]
    (define maybe-line : (U String EOF) (read-line /dev/csvin 'any))
    (if (eq? maybe-line <#>) (csv-read-line /dev/csvin <#>) maybe-line)))
  
(define csv-subfield : (-> String Nonnegative-Fixnum Nonnegative-Fixnum Boolean (Option Char) (Option Char) CSV-Field)
  (lambda [raw start end escaping? </> <\>]
    (cond [(not escaping?) (substring raw start end)]
          [else (let ([dest : String (make-string (- end start))])
                  (let escape ([cur : Nonnegative-Fixnum start]
                               [idx : Nonnegative-Fixnum 0])
                    (cond [(>= cur end) (substring dest 0 idx)]
                          [else (let ([ch : Char (string-ref raw cur)])
                                  (cond [(eq? ch </>) (string-set! dest idx ch) (escape (unsafe-fx+ cur 2) (unsafe-fx+ idx 1))]
                                        [else (string-set! dest idx ch) (escape (unsafe-fx+ cur 1) (unsafe-fx+ idx 1))]))])))])))

(define csv-subfield* : (-> (Option String) String Nonnegative-Fixnum Nonnegative-Fixnum Boolean (Option Char) (Option Char) CSV-Field)
  (lambda [previous raw start end escaping? </> <\>]
    (define this-field : String (csv-subfield raw start end escaping? </> <\>))
    (if (string? previous) (string-append previous string-newline this-field) this-field)))
