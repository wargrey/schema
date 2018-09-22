#lang typed/racket/base

(require "xlsx.rkt")

(require racket/file)

(define StateDepartment.xlsx : Path (#%xlsx))

(define StateDepartment.zip (time (read-xlsx-package StateDepartment.xlsx)))

;;StateDepartment.zip
