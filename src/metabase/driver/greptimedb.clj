(ns metabase.driver.greptimedb
  "GreptimeDB driver. Builds off of the SQL-JDBC driver."
  (:require
   [clojure.java.io :as jio]
   [clojure.java.jdbc :as jdbc]
   [clojure.set :as set]
   [clojure.string :as str]
   [clojure.walk :as walk]
   [honey.sql :as sql]
   [java-time :as t]
   [metabase.config :as config]
   [metabase.db.spec :as mdb.spec]
   [metabase.driver :as driver]
   [metabase.driver.common :as driver.common]
   [metabase.driver.greptimedb.actions :as greptimedb.actions]
   [metabase.driver.greptimedb.ddl :as greptimedb.ddl]
   [medley.core :as m]
   [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.driver.sql.query-processor.util :as sql.qp.u]
   [metabase.driver.sql.util :as sql.u]
   [metabase.driver.sql.util.unprepare :as unprepare]
   [metabase.models.field :as field]
   [metabase.query-processor.store :as qp.store]
   [metabase.query-processor.timezone :as qp.timezone]
   [metabase.query-processor.util.add-alias-info :as add]
   [metabase.query-processor.writeback :as qp.writeback]
   [metabase.upload :as upload]
   [metabase.util :as u]
   [metabase.util.honey-sql-2 :as h2x]
   [metabase.util.i18n :refer [deferred-tru trs]]
   [metabase.util.log :as log])
  (:import
   (java.io File)
   (java.sql ResultSet ResultSetMetaData Types)
   (java.time LocalDateTime OffsetDateTime OffsetTime ZonedDateTime)))

(set! *warn-on-reflection* true)

(comment
  ;; method impls live in these namespaces.
  greptimedb.actions/keep-me
  greptimedb.ddl/keep-me)

(driver/register! :greptimedb, :parent :sql-jdbc)

(defmethod driver/display-name :greptimedb [_] "GreptimeDB")

(doseq [[feature supported?] {:persist-models          true
                              :convert-timezone        true
                              :datetime-diff           true
                              :now                     true
                              :regex                   false
                              :percentile-aggregations false
                              :full-join               false
                              :uploads                 true
                              :schemas                 false
                              ;; MySQL LIKE clauses are case-sensitive or not based on whether the collation of the server and the columns
                              ;; themselves. Since this isn't something we can really change in the query itself don't present the option to the
                              ;; users in the UI
                              :case-sensitivity-string-filter-options false
                              :nested-field-columns false}]
  (defmethod driver/database-supports? [:greptimedb feature] [_driver _feature _db] supported?))

;; This is a bit of a lie since the JSON type was introduced for MySQL since 5.7.8.
;; And MariaDB doesn't have the JSON type at all, though `JSON` was introduced as an alias for LONGTEXT in 10.2.7.
;; But since JSON unfolding will only apply columns with JSON types, this won't cause any problems during sync.
;; (defmethod driver/database-supports? [:greptimedb :nested-field-columns] [_driver _feat db]
;;   (driver.common/json-unfolding-default db))

(doseq [feature [:actions :actions/custom]]
  (defmethod driver/database-supports? [:greptimedb feature]
    [driver _feat _db]
    ;; Only supported for MySQL right now. Revise when a child driver is added.
    (= driver :greptimedb)))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                             metabase.driver impls                                              |
;;; +----------------------------------------------------------------------------------------------------------------+



(defmethod driver/can-connect? :greptimedb
  [driver details]
  ;; delegate to parent method to check whether we can connect; if so, check if it's an unsupported version and issue
  ;; a warning if it is 
  (when ((get-method driver/can-connect? :sql-jdbc) driver details)
    true))

(def default-ssl-cert-details
  "Server SSL certificate chain, in PEM format."
  {:name         "ssl-cert"
   :display-name (deferred-tru "Server SSL certificate chain")
   :placeholder  ""
   :visible-if   {"ssl" true}})

(defmethod driver/connection-properties :greptimedb
  [_]
  (->>
   [driver.common/default-host-details
    (assoc driver.common/default-port-details :placeholder 4002)
    driver.common/default-dbname-details
    (assoc driver.common/default-user-details :require false)
    driver.common/default-password-details
    driver.common/default-ssl-details
    default-ssl-cert-details
    driver.common/advanced-options-start
    (assoc driver.common/additional-options
           :placeholder  "tinyInt1isBit=false")
    driver.common/default-advanced-options]
   (map u/one-or-many)
   (apply concat)))

(defn- format-interval
  "Generate a Postgres 'INTERVAL' literal.

    (sql/format-expr [::interval 2 :day])
    =>
    [\"INTERVAL '2 day'\"]"
  ;; I tried to write this with Malli but couldn't figure out how to make it work. See
  ;; https://metaboat.slack.com/archives/CKZEMT1MJ/p1676076592468909
  [_fn [amount unit]]
  {:pre [(number? amount)
         (#{:millisecond :second :minute :hour :day :week :month :year} unit)]}
  [(format "INTERVAL '%s %s'" (num amount) (name unit))])

(sql/register-fn! ::interval #'format-interval)

(defn- interval [amount unit]
  (h2x/with-database-type-info [::interval amount unit] "interval"))

(defn- ->timestamp [honeysql-form]
  (h2x/cast-unless-type-in "timestamp" #{"timestamp" "timestamptz" "date"} honeysql-form))

(defmethod sql.qp/add-interval-honeysql-form :greptimedb
  [driver hsql-form amount unit]
  ;; Postgres doesn't support quarter in intervals (#20683)
  (if (= unit :quarter)
    (recur driver hsql-form (* 3 amount) :month)
    (let [hsql-form (->timestamp hsql-form)]
      (-> (h2x/+ hsql-form (interval amount unit))
          (h2x/with-type-info (h2x/type-info hsql-form))))))


(defmethod sql.qp/current-datetime-honeysql-form :greptimedb
  [_driver]
  (h2x/with-database-type-info :%now "timestamp"))

(defmethod driver/humanize-connection-error-message :greptimedb
  [_ message]
  (condp re-matches message
    #"^Communications link failure\s+The last packet sent successfully to the server was 0 milliseconds ago. The driver has not received any packets from the server.$"
    :cannot-connect-check-host-and-port

    #"^Unknown database .*$"
    :database-name-incorrect

    #"Access denied for user.*$"
    :username-or-password-incorrect

    #"Must specify port after ':' in connection string"
    :invalid-hostname

    ;; else
    message))

(defmethod sql-jdbc.sync/db-default-timezone :greptimedb
  [_ _]
  "+00:00")

(defmethod driver/db-start-of-week :greptimedb
  [_]
  :sunday)

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                           metabase.driver.sql impls                                            |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql.qp/honey-sql-version :greptimedb
  [_driver]
  2)

(defmethod sql.qp/unix-timestamp->honeysql [:greptimedb :seconds] [_ _ expr]
  [:from_unixtime expr])

(defmethod sql.qp/cast-temporal-string [:greptimedb :Coercion/ISO8601->DateTime]
  [_driver _coercion-strategy expr]
  (h2x/->datetime expr))

(defmethod sql.qp/cast-temporal-string [:greptimedb :Coercion/YYYYMMDDHHMMSSString->Temporal]
  [_driver _coercion-strategy expr]
  [:convert expr [:raw "DATETIME"]])

(defmethod sql.qp/cast-temporal-byte [:greptimedb :Coercion/YYYYMMDDHHMMSSBytes->Temporal]
  [driver _coercion-strategy expr]
  (sql.qp/cast-temporal-string driver :Coercion/YYYYMMDDHHMMSSString->Temporal expr))

(defn- date-format [format-str expr] [:date_format expr (h2x/literal format-str)])
(defn- str-to-date [format-str expr] [:str_to_date expr (h2x/literal format-str)])

(defmethod sql.qp/->float :greptimedb
  [_ value]
  ;; no-op as MySQL doesn't support cast to float
  value)

(defmethod sql.qp/->integer :greptimedb
  [_ value]
  (h2x/maybe-cast :signed value))

(defmethod sql.qp/->honeysql [:greptimedb :regex-match-first]
  [driver [_ arg pattern]]
  [:regexp_substr (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern)])

(defmethod sql.qp/->honeysql [:greptimedb :length]
  [driver [_ arg]]
  [:char_length (sql.qp/->honeysql driver arg)])

(defmethod sql.qp/->honeysql [:greptimedb :field]
  [driver [_ :as mbql-clause]]
  (let [parent-method (get-method sql.qp/->honeysql [:sql :field])
        honeysql-expr    (parent-method driver mbql-clause)]
    honeysql-expr))

(defmethod sql.qp/->honeysql [:greptimedb :asc]
  [driver clause]
  ((get-method sql.qp/->honeysql [:sql :asc])
   driver
   (sql.qp/rewrite-fields-to-force-using-column-aliases clause)))

(defmethod sql.qp/->honeysql [:greptimedb :desc]
  [driver clause]
  ((get-method sql.qp/->honeysql [:sql :desc])
   driver
   (sql.qp/rewrite-fields-to-force-using-column-aliases clause)))
;; Since MySQL doesn't have date_trunc() we fake it by formatting a date to an appropriate string and then converting
;; back to a date. See http://dev.mysql.com/doc/refman/5.6/en/date-and-time-functions.html#function_date-format for an
;; explanation of format specifiers
;; this will generate a SQL statement casting the TIME to a DATETIME so date_format doesn't fail:
;; date_format(CAST(mytime AS DATETIME), '%Y-%m-%d %H') AS mytime
(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str (h2x/->datetime expr))))

(defn- ->date [expr]
  (if (h2x/is-of-type? expr "date")
    expr
    (-> [:arrow_cast expr [:raw "'Date32'"]]
        (h2x/with-database-type-info "date"))))

(defn make-date
  "Create and return a date based on  a year and a number of days value."
  [year-expr number-of-days]
  (-> [:makedate year-expr (sql.qp/inline-num number-of-days)]
      (h2x/with-database-type-info "date")))

(defmethod sql.qp/date [:greptimedb :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:greptimedb :minute]          [_ _ expr] (trunc-with-format "%Y-%m-%d %H:%i" expr))
(defmethod sql.qp/date [:greptimedb :minute-of-hour]  [_ _ expr] (h2x/minute expr))
(defmethod sql.qp/date [:greptimedb :hour]            [_ _ expr] (trunc-with-format "%Y-%m-%d %H" expr))
(defmethod sql.qp/date [:greptimedb :hour-of-day]     [_ _ expr] (h2x/hour expr))
(defmethod sql.qp/date [:greptimedb :day]             [_ _ expr] (->date expr))
(defmethod sql.qp/date [:greptimedb :day-of-month]    [_ _ expr] [:dayofmonth expr])
(defmethod sql.qp/date [:greptimedb :day-of-year]     [_ _ expr] [:dayofyear expr])
(defmethod sql.qp/date [:greptimedb :month-of-year]   [_ _ expr] (h2x/month expr))
(defmethod sql.qp/date [:greptimedb :quarter-of-year] [_ _ expr] (h2x/quarter expr))
(defmethod sql.qp/date [:greptimedb :year]            [_ _ expr] (make-date (h2x/year expr) 1))

(defmethod sql.qp/date [:greptimedb :day-of-week]
  [driver _unit expr]
  (sql.qp/adjust-day-of-week driver [:dayofweek expr]))

;; To convert a YEARWEEK (e.g. 201530) back to a date you need tell MySQL which day of the week to use,
;; because otherwise as far as MySQL is concerned you could be talking about any of the days in that week
(defmethod sql.qp/date [:greptimedb :week] [_ _ expr]
  (let [extract-week-fn (fn [expr]
                          (str-to-date "%X%V %W"
                                       (h2x/concat [:yearweek expr]
                                                   (h2x/literal " Sunday"))))]
    (sql.qp/adjust-start-of-week :greptimedb extract-week-fn expr)))

(defmethod sql.qp/date [:greptimedb :week-of-year-iso] [_ _ expr] (h2x/week expr 3))

(defmethod sql.qp/date [:greptimedb :month] [_ _ expr]
  (str-to-date "%Y-%m-%d"
               (h2x/concat (date-format "%Y-%m" expr)
                           (h2x/literal "-01"))))

;; Truncating to a quarter is trickier since there aren't any format strings.
;; See the explanation in the H2 driver, which does the same thing but with slightly different syntax.
(defmethod sql.qp/date [:greptimedb :quarter] [_ _ expr]
  (str-to-date "%Y-%m-%d"
               (h2x/concat (h2x/year expr)
                           (h2x/literal "-")
                           (h2x/- (h2x/* (h2x/quarter expr)
                                         3)
                                  2)
                           (h2x/literal "-01"))))

(defmethod sql.qp/->honeysql [:greptimedb :convert-timezone]
  [driver [_ arg target-timezone source-timezone]]
  (let [expr       (sql.qp/->honeysql driver arg)
        timestamp? (h2x/is-of-type? expr "timestamp")]
    (sql.u/validate-convert-timezone-args timestamp? target-timezone source-timezone)
    (h2x/with-database-type-info
      [:convert_tz expr (or source-timezone (qp.timezone/results-timezone-id)) target-timezone]
      "datetime")))

(defn- timestampdiff-dates [unit x y]
  [:timestampdiff [:raw (name unit)] (h2x/->date x) (h2x/->date y)])

(defn- timestampdiff [unit x y]
  [:timestampdiff [:raw (name unit)] x y])

(defmethod sql.qp/datetime-diff [:greptimedb :year]    [_driver _unit x y] (timestampdiff-dates :year x y))
(defmethod sql.qp/datetime-diff [:greptimedb :quarter] [_driver _unit x y] (timestampdiff-dates :quarter x y))
(defmethod sql.qp/datetime-diff [:greptimedb :month]   [_driver _unit x y] (timestampdiff-dates :month x y))
(defmethod sql.qp/datetime-diff [:greptimedb :week]    [_driver _unit x y] (timestampdiff-dates :week x y))
(defmethod sql.qp/datetime-diff [:greptimedb :day]     [_driver _unit x y] [:datediff y x])
(defmethod sql.qp/datetime-diff [:greptimedb :hour]    [_driver _unit x y] (timestampdiff :hour x y))
(defmethod sql.qp/datetime-diff [:greptimedb :minute]  [_driver _unit x y] (timestampdiff :minute x y))
(defmethod sql.qp/datetime-diff [:greptimedb :second]  [_driver _unit x y] (timestampdiff :second x y))

(defmethod driver/mbql->native :greptimedb [driver query]
  (let [result ((get-method driver/mbql->native :sql) driver query)]
    (log/info "xxxxxxxx" query result)
    result))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                         metabase.driver.sql-jdbc impls                                         |
;;; +----------------------------------------------------------------------------------------------------------------+

(defmethod sql-jdbc.sync/database-type->base-type :greptimedb
  [_ database-type]
  ({:TimestampSecond        :type/DateTime
    :TimestampMillisecond   :type/DateTime
    :TimestampMicrosecond   :type/DateTime
    :TimestampNanosecond    :type/DateTime
    :DateTime               :type/DateTime
    :Date                   :type/Date
    :Binary                 :type/*
    :Boolean                :type/Boolean
    :Int8                   :type/Integer
    :Int16                  :type/Integer
    :Int32                  :type/Integer
    :Int64                  :type/BigInteger
    :UInt8                  :type/Integer
    :UInt16                 :type/Integer
    :UInt32                 :type/Integer
    :UInt64                 :type/BigInteger
    :Float32                :type/Float
    :Float64                :type/Float
    :String                 :type/Text}
   ;; strip off " UNSIGNED" from end if present
   (keyword (str/replace (name database-type) #"\sUNSIGNED$" ""))))

(def ^:private default-connection-args
  "Map of args for the MySQL/MariaDB JDBC connection string."
  {;; 0000-00-00 dates are valid in MySQL; convert these to `null` when they come back because they're illegal in Java
   :zeroDateTimeBehavior "convertToNull"
   ;; Force UTF-8 encoding of results
   :useUnicode           true
   :characterEncoding    "UTF8"
   :characterSetResults  "UTF8"
   ;; GZIP compress packets sent between Metabase server and MySQL/MariaDB database
   :useCompression       true})

(defn- maybe-add-program-name-option [jdbc-spec additional-options-map]
  ;; connectionAttributes (if multiple) are separated by commas, so values that contain spaces are OK, so long as they
  ;; don't contain a comma; our mb-version-and-process-identifier shouldn't contain one, but just to be on the safe side
  (let [set-prog-nm-fn (fn []
                         (let [prog-name (str/replace config/mb-version-and-process-identifier "," "_")]
                           (assoc jdbc-spec :connectionAttributes (str "program_name:" prog-name))))]
    (if-let [conn-attrs (get additional-options-map "connectionAttributes")]
      (if (str/includes? conn-attrs "program_name")
        jdbc-spec ; additional-options already includes the program_name; don't set it here
        (set-prog-nm-fn))
      (set-prog-nm-fn)))) ; additional-options did not contain connectionAttributes at all; set it


(defmethod mdb.spec/spec :greptimedb
  [_ {:keys [host port db]
      :or   {host "localhost", port 4002, db ""}
      :as   opts}]
  (merge
   {:classname   "org.mariadb.jdbc.Driver"
    :subprotocol "mysql"
    :subname     (mdb.spec/make-subname host port db)}
   (dissoc opts :host :port :db)))

(defmethod sql-jdbc.conn/connection-details->spec :greptimedb
  [_ {ssl? :ssl, :keys [additional-options ssl-cert], :as details}]
  ;; In versions older than 0.32.0 the MySQL driver did not correctly save `ssl?` connection status. Users worked
  ;; around this by including `useSSL=true`. Check if that's there, and if it is, assume SSL status. See #9629
  ;;
  ;; TODO - should this be fixed by a data migration instead? 
  (let [addl-opts-map (sql-jdbc.common/additional-options->map additional-options :url "=" false)
        ssl?          (or ssl? (= "true" (get addl-opts-map "useSSL")))
        ssl-cert?     (and ssl? (some? ssl-cert))]
    (when (and ssl? (not (contains? addl-opts-map "trustServerCertificate")))
      (log/info (trs "You may need to add 'trustServerCertificate=true' to the additional connection options to connect with SSL.")))
    (merge
     default-connection-args
     ;; newer versions of MySQL will complain if you don't specify this when not using SSL
     {:useSSL (boolean ssl?)}
     (let [details (-> (if ssl-cert? (set/rename-keys details {:ssl-cert :serverSslCert}) details)
                       (set/rename-keys {:dbname :db})
                       (dissoc :ssl))]
       (-> (mdb.spec/spec :greptimedb details)
           (maybe-add-program-name-option addl-opts-map)
           (sql-jdbc.common/handle-additional-options details))))))

(defmethod driver/describe-database :greptimedb
  [_ database]
  {:tables
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (set
      (for [{:keys [table_schema table_name]}
            (jdbc/query {:connection conn}
                        ["select * from information_schema.tables where table_type = 'BASE TABLE';"])]
        {:name table_name :schema table_schema})))})

(defmethod driver/describe-table :greptimedb
  [_ database {table_name :name, schema :schema}]
  {:name   table_name
   :schema schema
   :fields
   (with-open [conn (jdbc/get-connection (sql-jdbc.conn/db->pooled-connection-spec database))]
     (let [results (jdbc/query
                    {:connection conn}
                    [(format "select * from information_schema.columns where table_name = '%s'" table_name)])]
       (set
        (for [[idx {column_name :column_name, data_type :data_type}] (m/indexed results)]
          {:name              column_name
           :database-type     data_type
           :base-type         (sql-jdbc.sync/database-type->base-type :greptimedb (keyword data_type))
           :database-position idx}))))})

(defmethod driver/describe-table-fks :greptimedb
  [_ _ _]
  (set #{}))

(defmethod sql-jdbc.sync/active-tables :greptimed
  ;;"will be overwrite by desc database"
  [& args]
  (apply sql-jdbc.sync/post-filtered-active-tables args))

(defmethod sql-jdbc.sync/excluded-schemas :greptimedb
  [_]
  #{"INFORMATION_SCHEMA"})

(defmethod sql.qp/quote-style :greptimedb [_] :mysql)

;; If this fails you need to load the timezone definitions from your system into MySQL; run the command
;;
;;    `mysql_tzinfo_to_sql /usr/share/zoneinfo | mysql -u root mysql`
;;
;; See https://dev.mysql.com/doc/refman/5.7/en/time-zone-support.html for details
;;
(defmethod sql-jdbc.execute/set-timezone-sql :greptimedb
  [_]
  "SET @@session.time_zone = %s;")

(defmethod sql-jdbc.execute/set-parameter [:greptimedb OffsetTime]
  [driver ps i t]
  ;; convert to a LocalTime so MySQL doesn't get F U S S Y
  (sql-jdbc.execute/set-parameter driver ps i (t/local-time (t/with-offset-same-instant t (t/zone-offset 0)))))

;; Regardless of session timezone it seems to be the case that OffsetDateTimes get normalized to UTC inside MySQL
;;
;; Since MySQL TIMESTAMPs aren't timezone-aware this means comparisons are done between timestamps in the report
;; timezone and the local datetime portion of the parameter, in UTC. Bad!
;;
;; Convert it to a LocalDateTime, in the report timezone, so comparisions will work correctly.
;;
;; See also — https://dev.mysql.com/doc/refman/5.5/en/datetime.html
;;
;; TIMEZONE FIXME — not 100% sure this behavior makes sense
(defmethod sql-jdbc.execute/set-parameter [:greptimedb OffsetDateTime]
  [driver ^java.sql.PreparedStatement ps ^Integer i t]
  (let [zone   (t/zone-id (qp.timezone/results-timezone-id))
        offset (.. zone getRules (getOffset (t/instant t)))
        t      (t/local-date-time (t/with-offset-same-instant t offset))]
    (sql-jdbc.execute/set-parameter driver ps i t)))

;; MySQL TIMESTAMPS are actually TIMESTAMP WITH LOCAL TIME ZONE, i.e. they are stored normalized to UTC when stored.
;; However, MySQL returns them in the report time zone in an effort to make our lives horrible.
(defmethod sql-jdbc.execute/read-column-thunk [:greptimedb Types/TIMESTAMP]
  [_ ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
  ;; Check and see if the column type is `TIMESTAMP` (as opposed to `DATETIME`, which is the equivalent of
  ;; LocalDateTime), and normalize it to a UTC timestamp if so.
  (if (= (.getColumnTypeName rsmeta i) "TIMESTAMP")
    (fn read-timestamp-thunk []
      (when-let [t (.getObject rs i LocalDateTime)]
        (t/with-offset-same-instant (t/offset-date-time t (t/zone-id (qp.timezone/results-timezone-id))) (t/zone-offset 0))))
    (fn read-datetime-thunk []
      (.getObject rs i LocalDateTime))))

;; Results of `timediff()` might come back as negative values, or might come back as values that aren't valid
;; `LocalTime`s e.g. `-01:00:00` or `25:00:00`.
;;
;; There is currently no way to tell whether the column is the result of a `timediff()` call (i.e., a duration) or a
;; normal `LocalTime` -- JDBC doesn't have interval/duration type enums. `java.time.LocalTime`only accepts values of
;; hour between 0 and 23 (inclusive). The MariaDB JDBC driver's implementations of `(.getObject rs i
;; java.time.LocalTime)` will throw Exceptions theses cases.
;;
;; Thus we should attempt to fetch temporal results the normal way and fall back to string representations for cases
;; where the values are unparseable.
(defmethod sql-jdbc.execute/read-column-thunk [:greptimedb Types/TIME]
  [driver ^ResultSet rs rsmeta ^Integer i]
  (let [parent-thunk ((get-method sql-jdbc.execute/read-column-thunk [:sql-jdbc Types/TIME]) driver rs rsmeta i)]
    (fn read-time-thunk []
      (try
        (parent-thunk)
        (catch Throwable _
          (.getString rs i))))))

(defmethod sql-jdbc.execute/read-column-thunk [:greptimedb Types/DATE]
  [driver ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i]
  (if (= "YEAR" (.getColumnTypeName rsmeta i))
    (fn read-time-thunk []
      (when-let [x (.getObject rs i)]
        (.toLocalDate ^java.sql.Date x)))
    (let [parent-thunk ((get-method sql-jdbc.execute/read-column-thunk [:sql-jdbc Types/DATE]) driver rs rsmeta i)]
      parent-thunk)))

(defn- format-offset [t]
  (let [offset (t/format "ZZZZZ" (t/zone-offset t))]
    (if (= offset "Z")
      "UTC"
      offset)))

(defmethod unprepare/unprepare-value [:greptimedb OffsetTime]
  [_ t]
  ;; MySQL doesn't support timezone offsets in literals so pass in a local time literal wrapped in a call to convert
  ;; it to the appropriate timezone
  (format "convert_tz('%s', '%s', @@session.time_zone)"
          (t/format "HH:mm:ss.SSS" t)
          (format-offset t)))

(defmethod unprepare/unprepare-value [:greptimedb OffsetDateTime]
  [_ t]
  (format "convert_tz('%s', '%s', @@session.time_zone)"
          (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)
          (format-offset t)))

(defmethod unprepare/unprepare-value [:greptimedb ZonedDateTime]
  [_ t]
  (format "convert_tz('%s', '%s', @@session.time_zone)"
          (t/format "yyyy-MM-dd HH:mm:ss.SSS" t)
          (str (t/zone-id t))))

(defmethod driver/upload-type->database-type :greptimedb
  [_driver upload-type]
  (case upload-type
    ::upload/varchar_255 "VARCHAR(255)"
    ::upload/text        "TEXT"
    ::upload/int         "INTEGER"
    ::upload/float       "DOUBLE"
    ::upload/boolean     "BOOLEAN"
    ::upload/date        "DATE"
    ::upload/datetime    "TIMESTAMP"))

(defmethod driver/table-name-length-limit :greptimedb
  [_driver]
  ;; https://dev.mysql.com/doc/refman/8.0/en/identifier-length.html
  64)

(defn- format-load
  [_clause [file-path table-name]]
  [(format "LOAD DATA LOCAL INFILE '%s' INTO TABLE %s" file-path (sql/format-entity table-name))])

(sql/register-clause! ::load format-load :insert-into)

(defn- sanitize-value
  ;; Per https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-field-line-handling
  ;; Backslash is the MySQL escape character within strings in SQL statements. Thus, to specify a literal backslash,
  ;; you must specify two backslashes for the value to be interpreted as a single backslash. The escape sequences
  ;; '\t' and '\n' specify tab and newline characters, respectively.
  [v]
  (cond
    (string? v)
    (str/replace v #"\\|\n|\r|\t" {"\\" "\\\\"
                                   "\n" "\\n"
                                   "\r" "\\r"
                                   "\t" "\\t"})
    (boolean? v)
    (if v 1 0)
    :else
    v))

(defn- row->tsv
  [column-count row]
  (when (not= column-count (count row))
    (throw (Exception. (format "ERROR: missing data in row \"%s\"" (str/join "," row)))))
  (->> row
       (map sanitize-value)
       (str/join "\t")))

(defn- get-global-variable
  "The value of the given global variable in the DB. Does not do any type coercion, so, e.g., booleans come back as
  \"ON\" and \"OFF\"."
  [db-id var-name]
  (:value
   (first
    (jdbc/query (sql-jdbc.conn/db->pooled-connection-spec db-id)
                ["show global variables like ?" var-name]))))

(defmethod driver/insert-into! :greptimedb
  [driver db-id ^String table-name column-names values]
  ;; `local_infile` must be turned on per
  ;; https://dev.mysql.com/doc/refman/8.0/en/load-data.html#load-data-local
  (if (not= (get-global-variable db-id "local_infile") "ON")
    ;; If it isn't turned on, fall back to the generic "INSERT INTO ..." way
    ((get-method driver/insert-into! :sql-jdbc) driver db-id table-name column-names values)
    (let [temp-file (File/createTempFile table-name ".tsv")
          file-path (.getAbsolutePath temp-file)]
      (try
        (let [tsvs (map (partial row->tsv (count column-names)) values)
              sql  (sql/format {::load   [file-path (keyword table-name)]
                                :columns (map keyword column-names)}
                               :quoted true
                               :dialect (sql.qp/quote-style driver))]
          (with-open [^java.io.Writer writer (jio/writer file-path)]
            (doseq [value (interpose \newline tsvs)]
              (.write writer (str value))))
          (qp.writeback/execute-write-sql! db-id sql))
        (finally
          (.delete temp-file))))))