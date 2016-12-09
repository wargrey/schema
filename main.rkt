#lang digimon

(provide (all-defined-out))
(provide (all-from-out typed/db))

(require typed/db)

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
(define-predicate sql-datum? SQL-Datum)

(define schema-create-table.sql : (-> Boolean Symbol (Listof Symbol) (Listof Boolean) (Listof Boolean) Virtual-Statement)
  (lambda [silent? table columns not-nulls uniques]
    (virtual-statement
     (λ [[dbms : DBSystem]]
       (define cols : (Listof String) (map schema-name-normalize '(column ...)))
       (define (+++ [name : String] !null uniq) : String
         (cond [(and !null uniq) (string-append name " UNIQUE NOT NULL")]
               [(and !null) (string-append name " NOT NULL")]
               [(and uniq) (string-append name " UNIQUE")]
               [else name]))
       (case (dbsystem-name dbms)
         [(sqlite3)
          (format "CREATE ~a ~a (~a, ~a~a) WITHOUT ROWID;" (if silent? "TABLE IF NOT EXISTS" "TABLE")
                  (schema-name-normalize 'dbname) (string-append (schema-name-normalize 'uuid) " Text PRIMARY KEY")
                  (string-join (map +++ cols '(column-not-null ...) '(column-unique ...)) ", ")
                  (if (eq? 'rawfield '||) "" (format ", ~a NOT NULL" (schema-name-normalize 'rawfield))))]
         [else (raise-unsupported-error 'create-table "unknown database system: ~a" (dbsystem-name dbms))])))))


(define schema-name-normalize : (-> Symbol String)
  (let ([dictionaries : (HashTable Symbol String) (make-hasheq)])
    (lambda [name]
      (hash-ref! dictionaries name
                 (thunk (cond [(eq? name '||) (symbol->string (gensym '_))]
                              [else (let* ([patterns '([#rx"(.*)\\?$" "is_\\1"] [#rx"[^A-Za-z0-9_]" "_"])]
                                           [name (regexp-replaces (symbol->string name) patterns)])
                                      (if (string? name) name (bytes->string/utf-8 name)))]))))))

(define schema-value-normalize : (-> Symbol Any Symbol SQL-Datum)
  (lambda [field v dbsystem]
    (cond [(false? v) sql-null]
          [(boolean? v) (if (memq dbsystem '(mysql sqlite3)) 1 v)]
          [(sql-datum? v) v]
          [else (~a v)])))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define-syntax (define-raw-table stx)
  (syntax-parse stx #:datum-literals [:]
    [(_ id #:as Table (~optional (~seq #:with rawfield) #:defaults ([rawfield #'||]))
        ([uuid : UUID #:primary uuid-gen] [field : TypeHint extra-info ...] ...)
        (~optional (~seq #:contract record-contract:expr) #:defaults ([record-contract #'true])))
     (with-syntax* ([(table dbname) (syntax-case #'id [] [(table dbname) (list #'table #'dbname)] [id (list #'id #'id)])]
                    [struct:table (format-id #'table "struct:~a" (syntax-e #'table))]
                    [table? (format-id #'table "~a?" (syntax-e #'table))]
                    [check-fields (datum->syntax #'table (gensym (syntax-e #'table)))]
                    [table.sql (datum->syntax #'table (gensym (syntax-e #'table)))]
                    [([unsafe-table make-table create-table insert-table in-table select-table update-table delete-table]
                      [(:uuid table-uuid) (:field table-field) ...])
                     (let ([tablename (syntax-e #'table)])
                       (list (for/list ([prefix (in-list (list 'unsafe 'make 'create 'insert 'in 'select 'update 'delete))])
                               (format-id #'table "~a-~a" prefix tablename))
                             (for/list ([<field> (in-syntax #'(uuid field ...))])
                               (list (datum->syntax <field> (string->keyword (symbol->string (syntax-e <field>))))
                                     (datum->syntax <field> (format-id #'table "~a-~a" tablename (syntax-e <field>)))))))]
                    [([FieldType field-contract [defval ...] field-guard field-not-null field-unique column?] ...)
                     (for/list ([field-info (in-syntax #'([TypeHint extra-info ...] ...))])
                       (syntax-parse field-info
                         [(Type (~or (~between (~seq #:check contract:expr) 0 +inf.0)
                                     (~optional (~seq #:default defval:expr) #:name "#:default")
                                     (~optional (~seq #:guard racket->sql) #:name "#:guard")
                                     (~optional (~seq (~and #:unique uniqueness)) #:name "#:unique")
                                     (~optional (~seq (~and #:optional optional)) #:name "#:optional")
                                     (~optional (~seq (~and #:hide hide)) #:name "#:hide")) ...)
                          (define optional? (and (attribute optional) #true))
                          (list (if optional? #'(Option Type) #'Type)
                                (if (attribute contract) #'(and contract ...) #'#true) ; it won't be translated
                                (cond [(attribute defval) #'(defval)] [optional? #'(#false)] [else #'()]) ; it won't be translated
                                (or (attribute racket->sql) #'schema-value-normalize)
                                (if (not optional?) #'#true #'#false) ; NOT NULL
                                (if (attribute uniqueness) #'#true #'#false) ; UNIQUE
                                (not (attribute hide)) #|used to determine whether this field should be defined in database |#)]))]
                    [([column ColumnType table-column column-not-null column-unique column-guard] ...)
                     (for/list ([<column?> (in-syntax #'(column? ...))]
                                [column (in-syntax #'(field ...))]
                                [Type (in-syntax #'(FieldType ...))]
                                [table-column (in-syntax #'(table-field ...))]
                                [not-null (in-syntax #'(field-not-null ...))]
                                [unique (in-syntax #'(field-unique ...))]
                                [guard (in-syntax #'(field-guard ...))]
                                #:when (syntax-e <column?>))
                       (list column Type table-column not-null unique guard))]
                    [([args ...] [args! ...])
                     (for/fold ([syns (list null null)])
                               ([:fld (in-syntax #'(:field ...))]
                                [arg (in-syntax #'([field : FieldType defval ...] ...))]
                                [arg! (in-syntax #'([field : (U FieldType Void) (void)] ...))])
                       (list (cons :fld (cons arg (car syns)))
                             (cons :fld (cons arg! (cadr syns)))))])
       #'(begin (define-type Table table)
                (struct table schema-record ([uuid : UUID] [field : FieldType] ...) #:prefab #:constructor-name unsafe-table)
                (hash-set! schema-tables (value-name struct:table) struct:table)
                (define table.sql : (HashTable Symbol Statement) (make-hasheq))
                
                (define-syntax (check-fields stx)
                  (syntax-case stx []
                    [(_ src uuid field ...)
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
                           (raise-schema-constraint-error src struct:table (~a uuid)
                                                          (if table-failure (cons 'constraint checks) checks)
                                                          givens)))]))
                
                (define (make-table #:unsafe? [unsafe? : Boolean #false] :uuid [uuid : (-> UUID) uuid-gen] args ...) : Table
                  (when (not unsafe?) (check-fields 'create-table "" field ...))
                  (unsafe-table (uuid) field ...))

                (define (create-table [dbc : Connection] #:if-not-exists? [silent? : Boolean #false]) : Void
                  (define (sql [silent? : Boolean]) : Virtual-Statement
                    (virtual-statement
                     (λ [[dbms : DBSystem]]
                       (define cols : (Listof String) (map schema-name-normalize '(column ...)))
                       (define (+++ [name : String] !null uniq) : String
                         (cond [(and !null uniq) (string-append name " UNIQUE NOT NULL")]
                               [(and !null) (string-append name " NOT NULL")]
                               [(and uniq) (string-append name " UNIQUE")]
                               [else name]))
                       (case (dbsystem-name dbms)
                         [(sqlite3)
                          (format "CREATE ~a ~a (~a, ~a~a) WITHOUT ROWID;" (if silent? "TABLE IF NOT EXISTS" "TABLE")
                                  (schema-name-normalize 'dbname) (string-append (schema-name-normalize 'uuid) " Text PRIMARY KEY")
                                  (string-join (map +++ cols '(column-not-null ...) '(column-unique ...)) ", ")
                                  (if (eq? 'rawfield '||) "" (format ", ~a NOT NULL" (schema-name-normalize 'rawfield))))]
                         [else (raise-unsupported-error 'create-table "unknown database system: ~a" (dbsystem-name dbms))]))))
                  (query-exec dbc (hash-ref! table.sql (if silent? 'create-if-not-exists 'create) (thunk (sql silent?)))))

                (define (insert-table [dbc : Connection] [occurrence : Table] #:or-replace? [replace? : Boolean #false]) : Void
                  (define (sql [replace? : Boolean]) : Virtual-Statement
                    (virtual-statement
                     (λ [[dbms : DBSystem]]
                       (case (dbsystem-name dbms)
                         [(sqlite3)
                          (define-values (snmuloc seulva$)
                            (for/fold ([snmuloc : (Listof String) null] [seulva$ : (Listof String) null])
                                      ([col (in-list (append '(column ...) (if (eq? 'rawfield '||) null (list 'rawfield))))]
                                       [idx (in-naturals 2)])
                              (values (cons (schema-name-normalize col) (cons ", " snmuloc))
                                      (cons (number->string idx) (cons ", $" seulva$)))))
                          (format "INSERT ~a ~a (~a~a) VALUES ($1~a);" (if replace? "OR REPLACE INTO" "INTO")
                                  (schema-name-normalize 'dbname) (schema-name-normalize 'uuid)
                                  (apply string-append (reverse snmuloc))
                                  (apply string-append (reverse seulva$)))]
                         [else (raise-unsupported-error 'create-table "unknown database system: ~a" (dbsystem-name dbms))]))))
                  (define dbsys : Symbol (dbsystem-name (connection-dbsystem dbc)))
                  (define insert.sql : Statement (hash-ref! table.sql (if replace? 'replace 'insert) (thunk (sql replace?))))
                  (define column : SQL-Datum (column-guard 'column (table-column occurrence) dbsys)) ...
                  (cond [(eq? 'rawfield '||) (query-exec dbc insert.sql (table-uuid occurrence) column ...)]
                        [else (query-exec dbc insert.sql (table-uuid occurrence) column ...
                                          (call-with-output-string (λ [db] (write occurrence db))))]))

                (define (in-table [dbc : Connection] #:fetch [size : (U Positive-Integer +inf.0) +inf.0]) : (Sequenceof (U Table exn))
                  (define (sql [hint : Symbol]) : Virtual-Statement
                    (define the-one : (Listof String) (map schema-name-normalize (if (eq? 'rawfield '||) '(column ...) '(rawfield))))
                    (define SELECT : String (format "SELECT ~a FROM ~a" (string-join the-one ", ") (schema-name-normalize 'dbname)))
                    (define ~uuid : String (string-append "~a WHERE " (schema-name-normalize 'uuid) " = ~a"))
                    (virtual-statement
                     (case hint
                       [(select-uuid) (λ [[_ : DBSystem]] (format ~uuid SELECT (if (eq? (dbsystem-name _) 'postgresql) "$1" "?")))]
                       [else SELECT])))
                  (define select.sql : Statement (hash-ref! table.sql 'select* (thunk (sql 'select*))))
                  (sequence-map (λ [sexp] (with-handlers ([exn? (λ [[e : exn]] e)]) (read/assert sexp table?)))
                                (in-query dbc select.sql #:fetch size)))
                
                (define (update-table [occurrence : Table] :uuid [uuid : (Option (-> UUID)) #false] args! ...) : Table
                  ; Maybe: (void) will never be an valid value that will be inserted into database.
                  (let ([field : FieldType (if (void? field) (table-field occurrence) field)] ...)
                    (check-fields 'update-table (table-uuid occurrence) field ...)
                    (unsafe-table (if uuid (uuid) (table-uuid occurrence)) field ...)))
                
                (define (delete-table [occurrence : Table]) : Table
                  (unsafe-table (table-uuid occurrence) (table-field occurrence) ...))))]))

(define-syntax (define-schema stx)
  (syntax-parse stx
    [(_ subdefinition ...)
     #'(begin subdefinition ...)]))



;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(define-schema 
  (define-raw-table master #:as Master #:with racket
    ([uuid     : String        #:primary uuid:timestamp]
     [type     : Symbol        #:default 'table]
     [name     : String        #:default "བོད་ཀྱི་བཅུ་ཕྲག་རིག་མཛོད་ཆེན་མོ" #:unique]
     [tbl-name : String        #:default "藏族十明文化传世经典丛书" #:unique #:optional]
     [rootpage : Natural       #:default 0 #:optional]
     [sql      : String        #:optional #:hide]
     [ctime    : Fixnum        #:default (current-macroseconds)]
     [mtime    : Fixnum        #:default (current-macroseconds)]
     [deleted? : Boolean       #:default #false #:optional]))

  (define-raw-table sqlite-master #:as Sqlite-Master
    ([rowid    : Integer       #:primary current-macroseconds]
     [type     : Symbol        #:default 'table]
     [name     : String        #:default "བོད་ཀྱི་བཅུ་ཕྲག་རིག་མཛོད་ཆེན་མོ" #:unique]
     [tbl-name : String        #:default "藏族十明文化传世经典丛书" #:unique #:optional]
     [rootpage : Natural       #:default 0 #:optional]
     [sql      : String        #:optional #:hide])))

#|(define sqlite : Connection (sqlite3-connect #:database 'memory))
(create-master sqlite)
(define sqls : (Listof SQL-Datum) (query-list sqlite "SELECT sql FROM sqlite_master WHERE name = 'master';"))
(define record : Master (make-master #:sql (if (pair? sqls) (~a (car sqls)) "")))
(insert-master sqlite record)
(for ([record : (U Master exn) (in-master sqlite #:fetch 16)])
  (pretty-display record))
(for ([record : (U Sqlite-Master exn) (in-sqlite-master sqlite #:fetch 16)])
  (pretty-display record))
(disconnect sqlite)
|#