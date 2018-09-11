#lang typed/racket/base

(provide (all-defined-out))
(provide (all-from-out "../../csv.rkt"))
(provide (all-from-out "../../digitama/exchange/csv/readline.rkt"))
(provide (all-from-out racket/logging racket/path racket/file))

(require "../../csv.rkt")
(require "../../digitama/exchange/csv/readline.rkt")

(require racket/path)
(require racket/file)
(require racket/logging)

(require (for-syntax racket/base))

(define-syntax (#%csv stx)
  #'(let ([rmp (variable-reference->resolved-module-path (#%variable-reference))])
      (cond [(not rmp) (current-directory)]
            [else (let ([full (resolved-module-path-name rmp)])
                    (if (path? full) (path-replace-extension full #".csv") (current-directory)))])))

(define-syntax (#%dir stx)
  #'(let ([rmp (variable-reference->resolved-module-path (#%variable-reference))])
      (cond [(not rmp) (current-directory)]
            [else (let ([full (resolved-module-path-name rmp)])
                    (if (path? full) (assert (path-only full) path?) (current-directory)))])))
