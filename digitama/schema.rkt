#lang digimon

(provide (all-defined-out))

(require typed/db)
(require "virtual-sql.rkt")

(require (for-syntax "normalize.rkt"))

(define-type Schema-Message msg:schema)

(define schema-tables : (HashTable Symbol Struct-TypeTop) (make-hasheq))

(define-type/enum schema-maniplations : Schema-Maniplation read write verify select delete merge index unknown)
(struct msg:schema msg:log ([maniplation : Schema-Maniplation] [table : Symbol] [uuid : String]) #:prefab)

(define make-schema-message : (->* (Schema-Maniplation Struct-TypeTop String Any)
                                   (#:level (Option Log-Level) String) ;;; `#:rest Type` is broken in 6.6
                                   Schema-Message)
  (lambda [maniplation struct:table uuid urgent #:level [level #false] [message ""]]
    (cond [(exn:schema? urgent) (exn:schema->schema-message urgent #:level level)]
          [(exn? urgent) (exn:schema->schema-message (exn->exn:schema urgent maniplation struct:table uuid) #:level level)]
          [else (let ([table : Symbol (value-name struct:table)])
                  (msg:schema (or level 'info) message urgent table maniplation table uuid))])))

(define-type Schema-Record schema-record)
(struct schema-record ()
  #:prefab ;#:type-name Schema ; this break the type checking if it is inherited 
  #:constructor-name abstract-schema-record)

(struct exn:schema exn:fail ([struct:table : Struct-TypeTop] [uuid : String]))
(struct exn:schema:read exn:schema ([reason : (U EOF Schema-Record exn False)]))
(struct exn:schema:write exn:schema ([reason : exn]))
(struct exn:schema:index exn:schema ())
(struct exn:schema:verify exn:schema ())
(struct exn:schema:merge exn:schema ())
(struct exn:schema:select exn:schema ())
(struct exn:schema:delete exn:schema ())

(struct exn:schema:constraint exn:schema:merge ([constraint : (Listof Any)] [given : (HashTable Symbol Any)]))
(struct exn:schema:constraint:unique exn:schema:constraint ([key-type : (U 'Natural 'Surrogate)]))

(define raise-schema-constraint-error : (-> Symbol Struct-TypeTop String (Listof Any) (Listof (Pairof Symbol Any)) Nothing)
  (lambda [source struct:table uuid constraints given]
    (throw [exn:schema:constraint struct:table uuid (reverse constraints) (make-hash given)]
           "~a: constraint violation~n  table: ~a~n  constraint: @~a~n  given: ~a~n" source struct:table
           (string-join ((inst map String Any) ~s (reverse constraints))
                        (format "~n~a@" (make-string 14 #\space)))
           (string-join ((inst map String (Pairof Symbol Any)) (λ [kv] (format "(~a . ~s)" (car kv) (cdr kv))) (reverse given))
                        (format "~n~a " (make-string 8 #\space))))))

(define raise-schema-unique-constraint-error : (-> Symbol Struct-TypeTop String (Listof (Pairof Symbol Any))
                                                   [#:type (U 'Natural 'Surrogate)]
                                                   Nothing)
  (lambda [source struct:table uuid given #:type [key-type 'Natural]]
    (define entry : (HashTable Symbol Any) (make-hash given))
    (throw [exn:schema:constraint:unique struct:table uuid `(UNIQUE ,(hash-keys entry)) entry key-type]
           "~a: constraint violation~n  table: ~a~n  constraint: @Unique{~a}~n  given: {~a}~n"
           source (object-name struct:table)
           (string-join (map symbol->string (hash-keys entry)) ", ")
           (string-join (hash-map entry (λ [k v] (format "~a: ~s" k v)))
                        (format "~n~a " (make-string 9 #\space))))))

(define exn->exn:schema : (-> exn Schema-Maniplation Struct-TypeTop String exn:schema)
  (lambda [e maniplation struct:table uuid]
    (define brief : String (exn-message e))
    (define cmarks : Continuation-Mark-Set (exn-continuation-marks e))
    (case maniplation
      [(read) (exn:schema:read brief cmarks struct:table uuid e)]
      [(write) (exn:schema:write brief cmarks struct:table uuid e)]
      [(select) (exn:schema:select brief cmarks struct:table uuid)]
      [(delete) (exn:schema:delete brief cmarks struct:table uuid)]
      [(merge) (exn:schema:merge brief cmarks struct:table uuid)]
      [(verify) (exn:schema:verify brief cmarks struct:table uuid)]
      [(index) (exn:schema:index brief cmarks struct:table uuid)]
      [else (exn:schema brief cmarks struct:table uuid)])))

(define exn:schema->schema-message : (-> exn:schema [#:level (Option Log-Level)] Schema-Message)
  (lambda [e #:level [level #false]]
    (msg:schema (or level (match e [(struct* exn:schema:constraint:unique ([key-type 'Natural])) 'warning] [_ 'error]))
                (exn-message e)
                (continuation-mark->stacks (exn-continuation-marks e))
                (value-name e)
                (cond [(exn:schema:read? e) 'read]
                      [(exn:schema:write? e) 'write]
                      [(exn:schema:select? e) 'select]
                      [(exn:schema:delete? e) 'delete]
                      [(exn:schema:merge? e) 'merge]
                      [(exn:schema:verify? e) 'verify]
                      [(exn:schema:index? e) 'index]
                      [else 'unknown])
                (value-name (exn:schema-struct:table e))
                (exn:schema-uuid e))))

(define schema-name->struct:schema : (-> Symbol Struct-TypeTop)
  (let ([unknown-schema-tables : (HashTable Symbol Struct-TypeTop) (make-hasheq)])
    (lambda [record-name]
      (hash-ref schema-tables record-name
                (thunk (hash-ref! unknown-schema-tables record-name
                                  (thunk (let-values ([(struct:unknown make-unknown unknown? unknown-ref unknown-set!)
                                                       (make-struct-type record-name struct:schema-record 0 0
                                                                         'manually-serialization null 'prefab
                                                                         #false null (and 'prefab-has-not-guard #false)
                                                                         #false)])
                                           struct:unknown))))))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define-syntax (define-table stx)
  (define (parse-field-definition tablename rowid racket? stx)
    (syntax-parse stx
      [(field DataType (~or (~between (~seq #:check contract:expr) 0 +inf.0)
                            (~optional (~or (~seq #:default defval) (~seq #:auto generate)) #:name "#:default or #:auto")
                            (~optional (~seq #:guard guard) #:name "#:guard")
                            (~optional (~seq (~and #:not-null not-null)) #:name "#:not-null")
                            (~optional (~seq (~and #:unique unique)) #:name "#:unique")
                            (~optional (~seq (~and #:hide hide)) #:name "#:hide")) ...)
       (define table-field (format-id #'field "~a-~a" tablename (syntax-e #'field)))
       (values (and (eq? (syntax-e #'field) rowid) table-field)
               (list (datum->syntax #'field (string->keyword (symbol->string (syntax-e #'field))))
                     table-field (if (attribute contract) #'(and contract ...) #'#true)
                     (if (attribute not-null) #'DataType #'(Option DataType))
                     (if (attribute generate) #'generate #'(void))
                     (cond [(attribute defval) #'(defval)]
                           [(attribute generate) #'(generate)]
                           [(attribute not-null) #'()]
                           [else #'(#false)]))
               (unless (and racket? (attribute hide))
                 (list #'field (id->sql #'field) table-field #'DataType
                       (or (attribute guard) #'racket->sql)
                       (and (attribute not-null) #'#true)
                       (and (attribute unique) #'#true))))]))
  (syntax-parse stx #:datum-literals [:]
    [(_ id #:as Table #:with primary-key (~optional prefab)
        ([field : DataType constraints ...] ...)
        (~optional (~seq #:contract record-contract:expr) #:defaults ([record-contract #'#true])))
     (with-syntax* ([(rowid ___) (list (id->sql #'primary-key) (format-id #'id "..."))]
                    [(table dbtable) (syntax-parse #'id [(id db) (list #'id (id->sql #'db))] [id (list #'id (id->sql #'id))])]
                    [(racket :opt) (if (attribute prefab) (list (id->sql #'prefab) #'#:prefab) (list #'#false #'#:transparent))]
                    [([table-rowid (:field table-field field-contract FieldType on-update [defval ...]) ...]
                      [(column-id column table-column ColumnType column-guard column-not-null column-unique) ...]
                      [struct:table table? table-row?] [check-fields table.sql]
                      [unsafe-table make-table create-table insert-table in-table select-table update-table delete-table])
                     (let ([tablename (syntax-e #'table)]
                           [pkname (syntax-e #'primary-key)]
                           [racket? (and (syntax-e #'racket) #true)])
                       (define-values (sdleif snmuloc table-rowid)
                         (for/fold ([sdleif null] [snmuloc null] [table-rowid #false])
                                   ([stx (in-syntax #'([field DataType constraints ...] ...))])
                           (define-values (pk-info field-info column-info) (parse-field-definition tablename pkname racket? stx))
                           (cond [(void? column-info) (values (cons field-info sdleif) snmuloc table-rowid)]
                                 [else (values (cons field-info sdleif) (cons column-info snmuloc) (or table-rowid pk-info))])))
                       (list (cons table-rowid (reverse sdleif)) (reverse snmuloc)
                             (for/list ([fmt (in-list (list "struct:~a" "~a?" "~a-row?"))]) (format-id #'table fmt tablename))
                             (for/list ([idx (in-range 2)]) (datum->syntax #'table (gensym tablename)))
                             (for/list ([prefix (in-list (list 'unsafe 'make 'create 'insert 'in 'select 'update 'delete))])
                               (format-id #'table "~a-~a" prefix tablename))))]
                    [([args ...] [args! ...])
                     (for/fold ([syns (list null null)])
                               ([:fld (in-syntax #'(:field ...))]
                                [arg (in-syntax #'([field : FieldType defval ...] ...))]
                                [arg! (in-syntax #'([field : (U FieldType Void) on-update] ...))])
                       (list (cons :fld (cons arg (car syns)))
                             (cons :fld (cons arg! (cadr syns)))))])
       #'(begin (define-type Table table)
                (struct table ([field : FieldType] ...) :opt #:constructor-name unsafe-table)
                (define-predicate table-row? (List FieldType ...))
                (hash-set! schema-tables (value-name struct:table) struct:table)
                (define table.sql : (HashTable Symbol Statement) (make-hasheq))
                
                (define-syntax (check-fields stx)
                  (syntax-case stx []
                    [(_ src field ...)
                     #'(let ([failures (list (false? field-contract) ...)]
                             [table-failure (false? record-contract)])
                         (when (or (memq #true failures) table-failure)
                           (define maybe-fields (remove-duplicates (filter symbol? (flatten 'record-contract))))
                           (define-values (givens checks)
                             (for/fold ([fields : (Listof (Pairof Symbol Any)) null] [checks : (Listof Any) null])
                                       ([target (in-list (list 'field ...))]
                                        [given (in-list (list field ...))]
                                        [check (in-list (list 'field-contract ...))]
                                        [failure (in-list failures)])
                               (cond [failure (values (cons (cons target given) fields) (cons check checks))]
                                     [(and table-failure (memq target maybe-fields)) (values (cons (cons target given) fields) checks)]
                                     [else (values fields checks)])))
                           (raise-schema-constraint-error src struct:table "uuid"
                                                          (if table-failure (cons 'constraint checks) checks)
                                                          givens)))]))
                
                (define (make-table #:unsafe? [unsafe? : Boolean #false] args ...) : Table
                  (when (not unsafe?) (check-fields 'create-table field ...))
                  (unsafe-table field ...))

                (define (create-table [dbc : Connection] #:if-not-exists? [silent? : Boolean #false]) : Void
                  (when (false? table-rowid) (raise-unsupported-error 'create-table "no need to create a temporary view"))
                  (define (virtual.sql) : Virtual-Statement
                    (create-table.sql silent? dbtable rowid racket '(column ...) '(column-not-null ...) '(column-unique ...)))
                  (query-exec dbc (hash-ref! table.sql (if silent? 'create-if-not-exists 'create) virtual.sql)))

                (define insert-table : (-> Connection [#:or-replace? Boolean] (U Table (Listof Table)) * Void)
                  (lambda [dbc #:or-replace? [replace? #false] . occurrences]
                    (define (virtual.sql) : Virtual-Statement (insert-into.sql replace? dbtable racket '(column ...)))
                    (when (false? table-rowid) (raise-unsupported-error 'create-table "cannot insert records into a temporary view"))
                    (define dbsys : Symbol (dbsystem-name (connection-dbsystem dbc)))
                    (define insert.sql : Statement (hash-ref! table.sql (if replace? 'replace 'insert) virtual.sql))
                    (for ([occurrence (in-list occurrences)])
                      (for ([row (if (list? occurrence) (in-list occurrence) (in-value occurrence))])
                        (define column-id : SQL-Datum (column-guard 'column-id (table-column row) dbsys)) ...
                        (cond [(false? racket) (query-exec dbc insert.sql column-id ...)]
                              [else (query-exec dbc insert.sql column-id ... (call-with-output-string (λ [db] (write row db))))])))))

                (define-syntax (select-table stx) (syntax-case stx [] [(_ argl ___) #'(sequence->list (in-table argl ___))]))
                (define (in-table [dbc : Connection] #:fetch [size : (U Positive-Integer +inf.0) 1]) : (Sequenceof (U Table exn))
                  (define (virtual.sql [which : Symbol]) : (-> Virtual-Statement)
                    (thunk (simple-select.sql which dbtable rowid racket '(column ...))))
                  (define (read-table sexp) : (U Table exn)
                    (with-handlers ([exn? (λ [[e : exn]] e)])
                      (read/assert sexp table?)))
                  (define (read-by-pk [pk : SQL-Datum]) : (U Table exn)
                    (with-handlers ([exn? (λ [[e : exn]] e)])
                      (define record : (Listof SQL-Datum)
                        (for/list ([row (in-vector (query-row dbc (hash-ref table.sql 'select-row (virtual.sql 'row)) pk))])
                          (if (sql-null? row) #false row)))
                      (apply unsafe-table (assert record table-row?))))
                  (define-values (key fmap) (if (and racket) (values 'select-racket read-table) (values 'select-rowid read-by-pk)))
                  (sequence-map fmap (in-query dbc #:fetch size (hash-ref table.sql key (virtual.sql 'nowhere)))))

                (define (update-table [occurrence : Table] #:dbconnection [dbc : (Option Connection) #false] args! ...) : Table
                  (define (virtual.sql) : Virtual-Statement (update.sql dbtable rowid racket '(column ...)))
                  (cond [(false? table-rowid) (raise-unsupported-error 'create-table "cannot update records of a temporary view")]
                        [else (let ([field : FieldType (if (void? field) (table-field occurrence) field)] ...)
                                (check-fields 'update-table field ...)
                                (define updated-occurrence : Table (unsafe-table field ...))
                                (when (and dbc (equal? (table-rowid occurrence) (table-rowid updated-occurrence)))
                                  (define dbsys : Symbol (dbsystem-name (connection-dbsystem dbc)))
                                  (define update.sql : Statement (hash-ref! table.sql 'update virtual.sql))
                                  (define column-id : SQL-Datum (column-guard 'column-id (table-column updated-occurrence) dbsys)) ...
                                  (cond [(false? racket) (query-exec dbc update.sql column-id ... (table-rowid updated-occurrence))]
                                        [else (query-exec dbc update.sql column-id ...
                                                          (call-with-output-string (λ [db] (write updated-occurrence db)))
                                                          (table-rowid updated-occurrence))]))
                                updated-occurrence)]))
                
                (define (delete-table [dbc : Connection] . [occurrences : (U Table (Listof Table)) *]) : Void
                  (define (virtual.sql) : Virtual-Statement (delete.sql dbtable rowid))
                  (cond [(false? table-rowid) (raise-unsupported-error 'create-table "cannot delete records from a temporary view")]
                        [else (for ([occurrence (in-list occurrences)])
                                (for ([row (if (list? occurrence) (in-list occurrence) (in-value occurrence))])
                                  (query-exec dbc (hash-ref! table.sql 'delete-table-by-rowid virtual.sql) (table-rowid row))))]))))]))

(define-syntax (define-schema stx)
  (syntax-parse stx
    [(_ Table-Datum (define-table id #:as ID rest ...) ...)
     #'(begin (define-type Table-Datum (U ID ...))
              (define-table id #:as ID rest ...) ...)]))
