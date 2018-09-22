#lang typed/racket/base

(require "../csv.rkt")

(require racket/file)
(require racket/math)
(require racket/format)

(port-count-lines-enabled #false)

(define StateDepartment.csv : Path (build-path (#%dir) "StateDepartment.csv"))
(define range : Natural (file-size StateDepartment.csv))
(define chars-width : Index 64)

(define update-gauge : (-> Symbol Natural Void)
  (let ([last-count : (Boxof Integer) (box 0)])
    (lambda [topic position]
      (define % : Flonum (real->double-flonum (/ position range)))
      (define count : Integer (exact-floor (* % chars-width)))
      (define lcount : Integer (unbox last-count))
      (when (> count lcount)
        (set-box! last-count count)
        (display "\033[42m")
        (display (make-string (- count lcount) #\space))
        (display "\033[0m")
        (display "\033[s")
        (when (< count chars-width)
          (printf "\033[~aC" (- chars-width count)))
        (printf "] [~a%]" (~r (* % 100.0) #:precision '(= 2)))
        (display "\033[u")
        (flush-output)))))

(parameterize ([default-csv-progress-handler update-gauge])
  (with-input-from-file StateDepartment.csv
    (λ [] (void (time (dynamic-wind
                       (λ [] (display #\[))
                       (λ [] (read-csv (current-input-port) 28 #true))
                       (λ [] (printf "~n~a: ~a " (file-name-from-path StateDepartment.csv) (~size range)))))))))
