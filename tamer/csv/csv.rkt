#lang racket/gui

(provide read-csv)

(require 2htdp/batch-io)

(define (read-csv src.csv)
  (read-csv-file (cond [(not (path? src.csv)) src.csv]
                       [else (path->string src.csv)])))

(define (read-csv* src-dir)
  (define metrics (make-hasheq))

  (for ([sheet.csv (in-list (directory-list src-dir #:build? #true))]
        #:when (equal? (path-get-extension sheet.csv) #".csv"))
    (define sheetname (string->symbol (string-replace (path->string (file-name-from-path sheet.csv)) #px"-Table\\s+\\d+.csv" "")))
    (hash-set! metrics sheetname (read-csv-file (path->string sheet.csv))))

  metrics)

(read-csv "wikipedia-example.csv")
