#lang typed/racket/base

(require "xlsx.rkt")

(require racket/file)

(define StateDepartment.xlsx : Path (#%xlsx))

(define StateDepartment.zip (read-xlsx-package StateDepartment.xlsx))

StateDepartment.zip
