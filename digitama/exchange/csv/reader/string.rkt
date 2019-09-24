#lang typed/racket/base

;;; https://tools.ietf.org/html/rfc4180

(provide (all-defined-out))

(require "../dialect.rkt")
(require "progress.rkt")
(require "line.rkt")
(require "misc.rkt")

(require racket/unsafe/ops)

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define in-csv-string : (-> String Positive-Index CSV-Dialect Boolean Boolean Boolean (Option Symbol) (Sequenceof CSV-Row))
  (lambda [/dev/strin n dialect skip-header? strict? trim-line? maybe-topic]
    (define maybe-progress-handler : Maybe-CSV-Progress-Handler (default-csv-progress-handler))
    (define topic : Symbol (or maybe-topic ((default-csv-progress-topic-resolver) /dev/strin)))
    
    (define eos : Index (string-length /dev/strin))
    (define sentinel : (Pairof CSV-Row Index) (cons empty-row (csv-sos /dev/strin eos skip-header?)))
    (define (read-csv [hint : (Pairof CSV-Row Index)]) : (Pairof CSV-Row Index)
      (let read-from ([pos : Index (cdr hint)])
        (define-values (maybe-row npos) (csv-split-row /dev/strin eos pos n dialect strict? trim-line? maybe-progress-handler topic))
        (cond [(< npos eos) (if (and maybe-row) (cons maybe-row npos) (read-from npos))]
              [(not maybe-row) (csv-report-position* eos maybe-progress-handler topic) sentinel]
              [else (cons maybe-row eos)])))

    ((inst make-do-sequence (Pairof CSV-Row Index) CSV-Row)
     (λ [] (values unsafe-car
                   read-csv
                   (read-csv sentinel)
                   (λ [v] (not (eq? v sentinel)))
                   #false
                   #false)))))

(define in-csv-string* : (-> String CSV-Dialect Boolean Boolean Boolean (Option Symbol) (Sequenceof CSV-Row*))
  (lambda [/dev/strin dialect skip-header? strict? trim-line? maybe-topic]
    (define maybe-progress-handler : Maybe-CSV-Progress-Handler (default-csv-progress-handler))
    (define topic : Symbol (or maybe-topic ((default-csv-progress-topic-resolver) /dev/strin)))
    
    (define eos : Index (string-length /dev/strin))
    (define sentinel : (Pairof CSV-Row* Index) (cons empty-row* (csv-sos /dev/strin eos skip-header?)))
    (define (read-csv [hint : (Pairof CSV-Row* Index)]) : (Pairof CSV-Row* Index)
      (let read-from ([pos : Index (cdr hint)])
        (define-values (maybe-row npos) (csv-split-row* /dev/strin eos pos dialect strict? trim-line? maybe-progress-handler topic))
        (cond [(< npos eos) (if (pair? maybe-row) (cons maybe-row npos) (read-from npos))]
              [(pair? maybe-row) (cons maybe-row eos)]
              [else (csv-report-position* eos maybe-progress-handler topic) sentinel])))

    ((inst make-do-sequence (Pairof CSV-Row* Index) CSV-Row*)
     (λ [] (values unsafe-car
                   read-csv
                   (read-csv sentinel)
                   (λ [v] (not (eq? v sentinel)))
                   #false
                   #false)))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-split/reverse : (-> String Positive-Index CSV-Dialect Boolean Boolean Boolean (Option Symbol) (Listof CSV-Row))
  (lambda [/dev/strin n dialect skip-header? strict? trim-line? maybe-topic]
    (define maybe-progress-handler : Maybe-CSV-Progress-Handler (default-csv-progress-handler))
    (define topic : Symbol (or maybe-topic ((default-csv-progress-topic-resolver) /dev/strin)))
    
    (define eos : Index (string-length /dev/strin))
    (let read-csv ([swor : (Listof CSV-Row) null]
                   [pos : Index (csv-sos /dev/strin eos skip-header?)])
      (define-values (maybe-row npos) (csv-split-row /dev/strin eos pos n dialect strict? trim-line? maybe-progress-handler topic))
      (cond [(< npos eos) (read-csv (if (and maybe-row) (cons maybe-row swor) swor) npos)]
            [(and maybe-row) (csv-report-position* eos maybe-progress-handler topic) (cons maybe-row swor)]
            [else (csv-report-position* eos maybe-progress-handler topic) swor]))))

(define csv-split*/reverse : (-> String CSV-Dialect Boolean Boolean Boolean (Option Symbol) (Listof CSV-Row*))
  (lambda [/dev/strin dialect skip-header? strict? trim-line? maybe-topic]
    (define maybe-progress-handler : Maybe-CSV-Progress-Handler (default-csv-progress-handler))
    (define topic : Symbol (or maybe-topic ((default-csv-progress-topic-resolver) /dev/strin)))
    
    (define eos : Index (string-length /dev/strin))
    (let read-csv ([swor : (Listof CSV-Row*) null]
                   [pos : Index (csv-sos /dev/strin eos skip-header?)])
      (define-values (maybe-row npos) (csv-split-row* /dev/strin eos pos dialect strict? trim-line? maybe-progress-handler topic))
      (cond [(< npos eos) (read-csv (if (pair? maybe-row) (cons maybe-row swor) swor) npos)]
            [(pair? maybe-row) (csv-report-position* eos maybe-progress-handler topic) (cons maybe-row swor)]
            [else (csv-report-position* eos maybe-progress-handler topic) swor]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-split-row : (-> String Index Index Positive-Index CSV-Dialect Boolean Boolean
                            Maybe-CSV-Progress-Handler Symbol
                            (Values (Option CSV-Row) Nonnegative-Fixnum))
  (lambda [/dev/strin eos pos n dialect strict? trim-line? maybe-progress-handler topic]
    (csv-report-position* pos maybe-progress-handler topic)
    
    (define row : CSV-Row (make-vector n empty-field))
    (let split-row ([pos : Nonnegative-Fixnum pos]
                    [count : Index 0])
      (define-values (field npos more?) (csv-split-field /dev/strin eos pos dialect strict?))
      (define ncount : Positive-Fixnum (+ count 1))
      (if (and more?)
          (cond [(>= ncount n) (split-row (csv-skip-exceeded-fields /dev/strin eos npos n ncount dialect strict?) 0)]
                [else (vector-set! row count field) (split-row npos ncount)])
          (cond [(= ncount n) (vector-set! row count field) (values row npos)]
                [(> ncount 1) (vector-set! row count field) (csv-log-length-error #false /dev/strin pos n ncount row strict?) (values #false npos)]
                [(not (eq? field empty-field)) (csv-log-length-error #false /dev/strin pos n ncount (vector field) strict?) (values #false npos)]
                [(or trim-line? (>= npos eos)) (values #false npos)]
                [else (csv-log-length-error #false /dev/strin pos n ncount (vector empty-field) strict?) (values #false npos)])))))
  
(define csv-split-row* : (-> String Index Index CSV-Dialect Boolean Boolean
                             Maybe-CSV-Progress-Handler Symbol
                            (Values (Listof CSV-Field) Nonnegative-Fixnum))
  (lambda [/dev/strin eos pos dialect strict? trim-line? maybe-progress-handler topic]
    (csv-report-position* pos maybe-progress-handler topic)

    (let split-row ([pos : Nonnegative-Fixnum pos]
                    [sdleif : (Listof CSV-Field) null])
      (define-values (field npos more?) (csv-split-field /dev/strin eos pos dialect strict?))
      (cond [(and more?) (split-row npos (cons field sdleif))]
            [(pair? sdleif) (values (reverse (cons field sdleif)) npos)]
            [(not (eq? field empty-field)) (values (list field) npos)]
            [(or trim-line? (>= npos eos)) (values null npos)]
            [else (values empty-row* npos)]))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define csv-split-field : (-> String Index Nonnegative-Fixnum CSV-Dialect Boolean (Values String Nonnegative-Fixnum Boolean))
  (lambda [/dev/strin eos pos dialect strict?]
    (define <#> : (Option Char) (csv-dialect-comment-char dialect))
    (define <:> : Char (csv-dialect-delimiter dialect))
    (define </> : (Option Char) (csv-dialect-quote-char dialect))
    (define <\> : (Option Char) (csv-dialect-escape-char dialect))
    
    (let extract-field ([start : Nonnegative-Fixnum pos]
                        [trim-left? : Boolean (csv-dialect-skip-leading-space? dialect)]
                        [end : Nonnegative-Fixnum pos]
                        [pos : Nonnegative-Fixnum pos]
                        [escaping? : Boolean #false])
      (if (>= pos eos)
          (values (csv-subfield #false #false /dev/strin start end escaping? </> <\>) eos #false)
          (let ([ch : Char (string-ref /dev/strin pos)]
                [next : Nonnegative-Fixnum (+ pos 1)])
            (cond [(eq? ch <:>) (values (csv-subfield #false #false /dev/strin start end escaping? </> <\>) next #true)]
                  [(eq? ch </>) (csv-split-quoted-field /dev/strin eos start next </> <\> dialect strict?)]
                  [(csv-detect-newline* ch /dev/strin eos next <#>)
                   => (csv-linefeed-values (csv-subfield #false #false /dev/strin start end escaping? </> <\>))]
                  [(eq? ch <\>) ; `#true` => c style escape char has been set
                   (let ([escaped-next (+ pos 2)])
                     (cond [(< next eos) (extract-field start #false escaped-next escaped-next #true)]
                           [else (csv-log-eof-error #false /dev/strin eos strict?)
                                 (extract-field start #false end escaped-next escaping?)]))]
                  [(char-blank? ch) (extract-field (if trim-left? next start) trim-left?
                                                   (if (csv-dialect-skip-trailing-space? dialect) end next)
                                                   next escaping?)]
                  [else (extract-field start #false next next escaping?)]))))))

(define csv-split-quoted-field : (-> String Index Nonnegative-Fixnum Nonnegative-Fixnum
                                     (Option Char) (Option Char) CSV-Dialect Boolean (Values String Nonnegative-Fixnum Boolean))
  (lambda [/dev/strin eos start idx </> <\> dialect strict?]
    (let check ([i : Fixnum (- idx 2)])
      (when (>= i start)
        (cond [(char-blank? (string-ref /dev/strin i)) (check (- i 1))]
              [else (csv-log-out-quotes-error #false /dev/strin idx strict? 'before)])))

    (let extract-quoted-field ([start : Nonnegative-Fixnum idx]
                               [end : Nonnegative-Fixnum idx]
                               [escaping? : Boolean #false])
      (cond [(>= end eos)
             (csv-log-eof-error #false /dev/strin eos strict?)
             (values (csv-subfield #false #false /dev/strin start eos escaping? </> <\>) end #false)]
            [else (let ([ch : Char (string-ref /dev/strin end)]
                        [next : Nonnegative-Fixnum (+ end 1)])
                    (cond [(eq? ch </>)
                           (cond [(>= next eos) (values (csv-subfield #false #false /dev/strin start eos escaping? </> <\>) eos #false)]
                                 [(and (not <\>) (eq? (string-ref /dev/strin next) </>)) (extract-quoted-field start (+ end 2) #true)]
                                 [else (let-values ([(npos more?) (csv-skip-quoted-rest /dev/strin eos next dialect strict?)])
                                         (values (csv-subfield #false #false /dev/strin start end escaping? </> <\>) npos more?))])]
                          [(csv-detect-newline ch /dev/strin eos next)
                           => (λ [[npos : Nonnegative-Fixnum]]
                                (extract-quoted-field start npos #true))]
                          [(eq? ch <\>) ; `#true` => c style escape char has been set
                           (cond [(< next eos) (extract-quoted-field start (+ end 2) #true)]
                                 [else (csv-log-eof-error #false /dev/strin eos strict?)
                                       (values (csv-subfield #false #false /dev/strin start end escaping? </> <\>) eos #false)])]
                          [else (extract-quoted-field start next escaping?)]))]))))

(define csv-skip-quoted-rest : (-> String Index Nonnegative-Fixnum CSV-Dialect Boolean (Values Nonnegative-Fixnum Boolean))
  (lambda [/dev/strin eos pos dialect strict?]
    (define <:> : Char (csv-dialect-delimiter dialect))
    (define <#> : (Option Char) (csv-dialect-comment-char dialect))
    
    (let skip ([pos : Nonnegative-Fixnum pos]
               [valid? : Boolean #true])
      (cond [(>= pos eos) (values eos #false)]
            [else (let ([ch : Char (string-ref /dev/strin pos)])
                    (cond [(eq? ch <:>) (csv-log-if-invalid #false /dev/strin pos valid? strict?) (values (+ pos 1) #true)]
                          [(csv-detect-newline* ch /dev/strin eos pos <#>)
                           => (csv-linefeed-identity (csv-log-if-invalid #false /dev/strin pos valid? strict?))]
                          [else (skip (+ pos 1) (and valid? (char-blank? ch)))]))]))))

(define csv-skip-exceeded-fields : (-> String Index Nonnegative-Fixnum Positive-Index Positive-Fixnum CSV-Dialect Boolean Nonnegative-Fixnum)
  (lambda [/dev/strin eos pos n count dialect strict?]
    (let skip-row ([pos : Nonnegative-Fixnum pos]
                   [extras : (Listof CSV-Field) null]
                   [total : Positive-Fixnum count])
      (define-values (field npos more?) (csv-split-field /dev/strin eos pos dialect #false))
      (cond [(and more?) (skip-row npos (cons field extras) (unsafe-fx+ total 1))]
            [else (csv-log-length-error #false /dev/strin npos n (unsafe-fx+ total 1) (reverse (cons field extras)) strict?) npos]))))

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
             (let skip-comment ([pos : Nonnegative-Fixnum npos])
               (cond [(>= pos eos) eos]
                     [else (or (csv-detect-newline (string-ref /dev/strin pos) /dev/strin eos (+ pos 1))
                               (skip-comment (+ pos 1)))]))))))
  
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
