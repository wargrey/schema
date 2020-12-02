#lang typed/racket/base

(provide (all-defined-out))
(provide (all-from-out "../../../csv.rkt"))
(provide (all-from-out racket/logging racket/path racket/file))

(require "../../../csv.rkt")

(require racket/path)
(require racket/file)
(require racket/math)
(require racket/format)
(require racket/logging)

(require (for-syntax racket/base))

(define-syntax (#%csv stx)
  (syntax/loc stx
    (let ([rmp (variable-reference->resolved-module-path (#%variable-reference))])
      (cond [(not rmp) (current-directory)]
            [else (let ([full (resolved-module-path-name rmp)])
                    (if (path? full) (path-replace-extension full #".csv") (current-directory)))]))))

(define-syntax (#%dir stx)
  (syntax/loc stx
    (let ([rmp (variable-reference->resolved-module-path (#%variable-reference))])
      (cond [(not rmp) (current-directory)]
            [else (let ([full (resolved-module-path-name rmp)])
                    (if (path? full) (assert (path-only full) path?) (current-directory)))]))))


(define-type Unit (U 'KB 'MB 'GB 'TB))

(define ~size : (->* (Real) ((U 'Bytes Unit) #:precision (U Integer (List '= Integer))) String)
  (lambda [size [unit 'Bytes] #:precision [prcs '(= 3)]]
    (if (eq? unit 'Bytes)
        (cond [(< -1024.0 size 1024.0) (exact-round size) "Bytes"]
              [else (~size (/ (real->double-flonum size) 1024.0) 'KB #:precision prcs)])
        (let try-next-unit : String ([s : Flonum (real->double-flonum size)] [us : (Option (Listof Unit)) (memq unit '(KB MB GB TB))])
          (cond [(not us) "Typed Racket is buggy if you see this message"]
                [(or (< (abs s) 1024.0) (null? (cdr us))) (string-append (~r s #:precision prcs) (symbol->string (car us)))]
                [else (try-next-unit (/ s 1024.0) (cdr us))])))))

