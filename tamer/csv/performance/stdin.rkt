#lang typed/racket/base

(require "../csv.rkt")

(require racket/string)
(require racket/port)

(define StateDepartment.csv : Path (build-path (#%dir) "StormEvents.csv"))

(collect-garbage 'major)
(collect-garbage 'major)
(collect-garbage 'major)

(define vectors : (Listof (Vectorof CSV-Field))
  (with-logging-to-port (current-error-port)
    (λ [] (time (read-csv StateDepartment.csv 11 #true)))
    'debug))

(define search : (-> String Fixnum (Values String Fixnum))
  (lambda [src idx]
    (define total : Index (string-length src))
    (let do-search ([i : Fixnum idx])
      (cond [(>= i total) (values "" i)]
            [(eq? (string-ref src i) #\,) (values (substring src idx i) (assert (+ i 1) fixnum?))]
            [else (do-search (assert (+ i 1) fixnum?))]))))

(collect-garbage 'major)
(collect-garbage 'major)
(collect-garbage 'major)

(call-with-input-file* StateDepartment.csv
  (λ [[/dev/csvin : Input-Port]]
    (time (for ([line (in-lines /dev/csvin)])
            (define total : Index (string-length line))
            (define row : (Vectorof CSV-Field) (make-vector 11 ""))
            (let split : Void ([i0 : Fixnum 0]
                               [idx : Index 0])
              (when (and (< i0 total) (< idx 11))
                (let-values ([(field nidx) (search line i0)])
                  (vector-set! row idx field)
                  (split nidx (+ idx 1)))))))))
