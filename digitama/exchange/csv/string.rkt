#lang typed/racket/base

;;; https://tools.ietf.org/html/rfc4180

(provide (all-defined-out))

(require "dialect.rkt")
(require "readline.rkt")
(require "misc.rkt")

(require racket/unsafe/ops)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define in-csv-string : (-> String Positive-Index CSV-Dialect Boolean Boolean Boolean (Sequenceof CSV-Row))
  (lambda [/dev/strin n dialect skip-header? strict? trim-line?]
    (define eos : Index (string-length /dev/strin))
    (define sentinel : (Pairof CSV-Row Index) (cons empty-row (csv-sos /dev/strin eos skip-header?)))
    (define (read-csv [hint : (Pairof CSV-Row Index)]) : (Pairof CSV-Row Index)
      (let read-from ([pos : Index (cdr hint)])
        (define-values (maybe-row npos) (csv-split-row /dev/strin eos pos n dialect strict? trim-line?))
        (cond [(< npos eos) (if (and maybe-row) (cons maybe-row npos) (read-from npos))]
              [(not maybe-row) sentinel]
              [else (cons maybe-row eos)])))

    ((inst make-do-sequence (Pairof CSV-Row Index) CSV-Row)
     (λ [] (values unsafe-car
                   read-csv
                   (read-csv sentinel)
                   (λ [v] (not (eq? v sentinel)))
                   #false
                   #false)))))

(define in-csv-string* : (-> String CSV-Dialect Boolean Boolean Boolean (Sequenceof CSV-Row*))
  (lambda [/dev/strin dialect skip-header? strict? trim-line?]
    (define eos : Index (string-length /dev/strin))
    (define sentinel : (Pairof CSV-Row* Index) (cons empty-row* (csv-sos /dev/strin eos skip-header?)))
    (define (read-csv [hint : (Pairof CSV-Row* Index)]) : (Pairof CSV-Row* Index)
      (let read-from ([pos : Index (cdr hint)])
        (define-values (maybe-row npos) (csv-split-row* /dev/strin eos pos dialect strict? trim-line?))
        (cond [(< npos eos) (if (pair? maybe-row) (cons maybe-row npos) (read-from npos))]
              [(pair? maybe-row) (cons maybe-row eos)]
              [else sentinel])))

    ((inst make-do-sequence (Pairof CSV-Row* Index) CSV-Row*)
     (λ [] (values unsafe-car
                   read-csv
                   (read-csv sentinel)
                   (λ [v] (not (eq? v sentinel)))
                   #false
                   #false)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-split/reverse : (-> String Positive-Index CSV-Dialect Boolean Boolean Boolean (Listof CSV-Row))
  (lambda [/dev/strin n dialect skip-header? strict? trim-line?]
    (define eos : Index (string-length /dev/strin))
    (let read-csv ([swor : (Listof CSV-Row) null]
                   [pos : Index (csv-sos /dev/strin eos skip-header?)])
      (define-values (maybe-row npos) (csv-split-row /dev/strin eos pos n dialect strict? trim-line?))
      (cond [(< npos eos) (read-csv (if (and maybe-row) (cons maybe-row swor) swor) npos)]
            [(and maybe-row) (cons maybe-row swor)]
            [else swor]))))

(define csv-split*/reverse : (-> String CSV-Dialect Boolean Boolean Boolean (Listof CSV-Row*))
  (lambda [/dev/strin dialect skip-header? strict? trim-line?]
   (define eos : Index (string-length /dev/strin))
    (let read-csv ([swor : (Listof CSV-Row*) null]
                   [pos : Index (csv-sos /dev/strin eos skip-header?)])
      (define-values (maybe-row npos) (csv-split-row* /dev/strin eos pos dialect strict? trim-line?))
      (cond [(< npos eos) (read-csv (if (pair? maybe-row) (cons maybe-row swor) swor) npos)]
            [(pair? maybe-row) (cons maybe-row swor)]
            [else swor]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-split-row : (-> String Index Index Positive-Index CSV-Dialect Boolean Boolean (Values (Option CSV-Row) Nonnegative-Fixnum))
  (lambda [/dev/strin eos pos n dialect strict? trim-line?]
    (define row : CSV-Row (make-vector n empty-field))
    (let extract-row ([pos : Nonnegative-Fixnum pos]
                      [idx : Index 0])
      (define-values (field npos has-more?) (csv-split-field /dev/strin eos pos dialect strict?))
      (define nidx : Positive-Fixnum (+ idx 1))
      (if (and has-more?)
          (cond [(>= nidx n) (extract-row (csv-skip-exceeded-fields /dev/strin eos npos n nidx dialect strict?) 0)]
                [else (vector-set! row idx field) (extract-row npos nidx)])
          (cond [(= nidx n) (vector-set! row idx field) (values row npos)]
                [(> nidx 1) (vector-set! row idx field) (csv-log-length-error #false /dev/strin pos n idx row strict?) (values #false npos)]
                [(not (eq? (vector-ref row 0) empty-field)) (csv-log-length-error #false /dev/strin pos n idx row strict?) (values #false npos)]
                [(not trim-line?) (csv-log-length-error #false /dev/strin pos n idx (vector empty-field) strict?) (values #false npos)]
                [else (values #false npos)])))))
  
(define csv-split-row* : (-> String Index Index CSV-Dialect Boolean Boolean (Values (Listof CSV-Field) Nonnegative-Fixnum))
  (lambda [/dev/strin eos pos dialect strict? trim-line?]
    (let extract-row ([pos : Nonnegative-Fixnum pos]
                      [sdleif : (Listof CSV-Field) null])
      (define-values (field npos has-more?) (csv-split-field /dev/strin eos pos dialect strict?))
      (cond [(and has-more?) (extract-row npos (cons field sdleif))]
            [(pair? sdleif) (values (reverse (cons field sdleif)) npos)]
            [(not (eq? field empty-field)) (values (list field) npos)]
            [(not trim-line?) (values empty-row* npos)]
            [else (values null npos)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-split-field : (-> String Index Nonnegative-Fixnum CSV-Dialect Boolean (Values String Nonnegative-Fixnum Boolean))
  (lambda [/dev/strin eos pos dialect strict?]
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define </> : (Option Char) (CSV-Dialect-quote-char dialect))
    (define <\> : (Option Char) (CSV-Dialect-escape-char dialect))
    
    (let extract-field ([start : Nonnegative-Fixnum pos]
                        [trim-left? : Boolean (CSV-Dialect-skip-leading-space? dialect)]
                        [end : Nonnegative-Fixnum pos]
                        [pos : Nonnegative-Fixnum pos]
                        [escaping? : Boolean #false])
      (if (>= pos eos)
          (values (csv-subfield #false #false /dev/strin start eos escaping? </> <\>) eos #false)
          (let ([ch : Char (string-ref /dev/strin pos)]
                [next : Nonnegative-Fixnum (+ pos 1)])
            (cond [(eq? ch <:>) (values (csv-subfield #false #false /dev/strin start end escaping? </> <\>) next #true)]
                  [(eq? ch </>) (csv-split-quoted-field /dev/strin eos start next </> <\> dialect strict?)]
                  [(csv-detect-newline* ch /dev/strin eos next <#>)
                   => (csv-linefeed-values (csv-subfield #false #false /dev/strin start end escaping? </> <\>))]
                  #;[(eq? ch <\>) ; `#true` -> c style escape char has been set'
                   (if (< next total) ; `newline` is not following the escape char
                       (let ([escaped-next (+ pos 2)])
                         (extract-field total start #false escaped-next escaped-next #true previous lgap))
                       (let-values ([(half-field) (csv-subfield /dev/strin previous src start end escaping? </> <\>)]
                                    [(maybe-start end \n-gap) (csv-locate /dev/strin total lgap eos)])
                         (cond [(not maybe-start) (csv-log-eof-error /dev/strin src pos strict?) (values total half-field (+ total 1) \n-gap)]
                               [else (extract-field (CSV-SubRow-end maybe-src) (CSV-SubRow-start maybe-src) #false
                                                    (CSV-SubRow-start maybe-src) (CSV-SubRow-start maybe-src) #false half-field
                                                    (CSV-SubRow-linegap maybe-src))])))]
                  [(char-blank? ch) (extract-field (if trim-left? next start) trim-left?
                                                   (if (CSV-Dialect-skip-trailing-space? dialect) end next)
                                                   next escaping?)]
                  [else (extract-field start #false next next escaping?)]))))))

(define csv-split-quoted-field : (-> String Index Nonnegative-Fixnum Nonnegative-Fixnum
                                     (Option Char) (Option Char) CSV-Dialect Boolean (Values String Nonnegative-Fixnum Boolean))
  (lambda [/dev/strin eos start pos </> <\> dialect strict?]
    (let check ([i : Fixnum (- pos 2)])
      (when (>= i start)
        (cond [(char-blank? (string-ref /dev/strin i)) (check (- i 1))]
              [else (csv-log-out-quotes-error #false /dev/strin i strict? 'before)])))

    (let extract-field ([start : Nonnegative-Fixnum pos]
                        [end : Nonnegative-Fixnum pos]
                        [escaping? : Boolean #false])
      (cond [(>= end eos)
             (csv-log-eof-error #false /dev/strin end strict?)
             (values (csv-subfield #false #false /dev/strin start end escaping? </> <\>) end #false)]
            [else (let ([ch : Char (string-ref /dev/strin end)]
                        [next : Nonnegative-Fixnum (+ end 1)])
                    (cond #;[(eq? ch <\>) ; `#true` -> c style escape char has been set
                           (if (< next total)
                               (extract-field total start (+ end 2) #true previous lgap) ; not followed by `newline`
                               (let ([half-field : String (csv-subfield /dev/strin previous src start end escaping? </> <\>)]
                                     [maybe-src : (Option CSV-SubRow) (csv-locate /dev/strin total lgap total)])
                                 (cond [(not maybe-src) (csv-log-eof-error /dev/strin src end strict?) (values src total half-field (+ total 1))]
                                       [else (extract-field (CSV-SubRow-end maybe-src) (CSV-SubRow-start maybe-src) (CSV-SubRow-start maybe-src)
                                                            #false half-field (CSV-SubRow-linegap maybe-src))])))]
                          [(eq? ch </>)
                           (cond [(>= next eos) (values (csv-subfield #false #false /dev/strin start end escaping? </> <\>) eos #false)]
                                 [(and (not <\>) (eq? (string-ref /dev/strin next) </>)) (extract-field start (+ end 2) #true)]
                                 [else (let-values ([(npos has-more?) (csv-skip-quoted-rest /dev/strin eos next dialect strict?)])
                                         (values (csv-subfield #false #false /dev/strin start end escaping? </> <\>) npos has-more?))])]
                          [else (extract-field start next escaping?)]))]))))

(define csv-skip-quoted-rest : (-> String Index Nonnegative-Fixnum CSV-Dialect Boolean (Values Nonnegative-Fixnum Boolean))
  (lambda [/dev/strin eos idx dialect strict?]
    (define <:> : Char (CSV-Dialect-delimiter dialect))
    (define <#> : (Option Char) (CSV-Dialect-comment-char dialect))
    
    (let skip ([end : Nonnegative-Fixnum idx]
               [valid? : Boolean #true])
      (cond [(>= end eos) (values eos #false)]
            [else (let ([ch (string-ref /dev/strin end)])
                    (cond [(eq? ch <:>) (csv-log-if-invalid #false /dev/strin end valid? strict?) (values (+ end 1) #true)]
                          [(csv-detect-newline* ch /dev/strin eos end <#>)
                           => (csv-linefeed-identity (csv-log-if-invalid #false /dev/strin end valid? strict?))]
                          [else (skip (+ end 1) (and valid? (char-blank? ch)))]))]))))

(define csv-skip-exceeded-fields : (-> String Index Nonnegative-Fixnum Positive-Index Positive-Fixnum CSV-Dialect Boolean Nonnegative-Fixnum)
  (lambda [/dev/strin eos pos n count dialect strict?]
    (let skip-row ([pos : Nonnegative-Fixnum pos]
                   [extras : (Listof CSV-Field) null]
                   [count : Positive-Fixnum count])
      (define-values (field npos has-more?) (csv-split-field /dev/strin eos pos dialect #false))
      (cond [(and has-more?) (skip-row npos (cons field extras) (unsafe-fx+ count 1))]
            [else (csv-log-length-error #false /dev/strin npos n (unsafe-fx+ count 1) (reverse (cons field extras)) strict?) npos]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-detect-newline : (-> Char String Index Nonnegative-Fixnum (Option Nonnegative-Fixnum))
  (lambda [ch /dev/strin eos npos]
    (cond [(eq? ch #\return) (if (and (< npos eos) (eq? (string-ref /dev/strin npos) #\linefeed)) (+ npos 1) npos)]
          [(eq? ch #\linefeed) npos]
          [else #false])))

(define csv-detect-newline* : (-> Char String Index Nonnegative-Fixnum (Option Char) (Option Nonnegative-Fixnum))
  (lambda [ch /dev/strin eos npos <#>]
    (or (csv-detect-newline ch /dev/strin eos npos)
        (and (eq? ch <#>)
             (cond [(>= npos eos) npos]
                   [else (csv-detect-newline* (string-ref /dev/strin npos)
                                               /dev/strin eos (+ npos 1) <#>)])))))

(define csv-linefeed-identity : (All (a) (-> a (-> Nonnegative-Fixnum (Values Nonnegative-Fixnum False))))
  (lambda [v]
    (λ [[npos : Nonnegative-Fixnum]]
      (values npos #false))))

(define csv-linefeed-values : (-> String (-> Nonnegative-Fixnum (Values String Nonnegative-Fixnum False)))
  (lambda [field]
    (λ [[npos : Nonnegative-Fixnum]]
      (values field npos #false))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-sos : (-> String Index Boolean Index)
  (lambda [/dev/strin eos skip-header?]
    (cond [(not skip-header?) 0]
          [else (let skip-header ([idx : Nonnegative-Fixnum 0])
                  (cond [(>= idx eos) eos]
                        [else (let* ([npos (+ idx 1)]
                                     [sos (csv-detect-newline (string-ref /dev/strin idx) /dev/strin eos npos)])
                                (cond [(not sos) (skip-header npos)]
                                      [(< sos eos) sos]
                                      [else eos]))]))])))
