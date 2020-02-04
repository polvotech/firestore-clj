(ns firestore-clj.core
  (:refer-clojure :exclude [set set! merge merge! assoc assoc! dissoc dissoc! take inc update! count])
  (:require [clojure.core :as core]
            [clojure.core.match :refer [match]]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [manifold.stream :as st])
  (:import (clojure.lang IAtom)
           (com.google.api.core ApiFuture)
           (com.google.auth.oauth2 GoogleCredentials)
           (com.google.cloud Timestamp)
           (com.google.cloud.firestore Firestore QuerySnapshot CollectionReference EventListener DocumentReference DocumentSnapshot Query$Direction FieldValue Query ListenerRegistration WriteBatch Transaction UpdateBuilder Transaction$Function TransactionOptions QueryDocumentSnapshot DocumentChange$Type DocumentChange)
           (com.google.firebase FirebaseApp FirebaseOptions FirebaseOptions)
           (com.google.firebase.cloud FirestoreClient)
           (java.io Writer)
           (java.util HashMap List)
           (java.util.concurrent Executor)))

(defn- build-hash-map
  "Helper for build java.util.HashMap without reflection."
  [m]
  (reduce (fn [^HashMap h [k v]]
            (do
              (.put h k v)
              h))
          (HashMap.)
          m))

; AUTH

(defn client-with-creds
  "Creates a client from a credentials JSON file."
  ^Firestore [creds-path]
  (with-open [sa (io/input-stream creds-path)]
    (let [options (-> (FirebaseOptions/builder)
                      (.setCredentials (GoogleCredentials/fromStream sa))
                      (.build))]
      (FirebaseApp/initializeApp options)
      (FirestoreClient/getFirestore))))

(defn default-client
  "Gets default client i.e. using a service account."
  ^Firestore [project-id]
  (let [options (-> (FirebaseOptions/builder)
                    (.setCredentials (GoogleCredentials/getApplicationDefault))
                    (.setProjectId project-id)
                    (.build))]
    (FirebaseApp/initializeApp options)
    (FirestoreClient/getFirestore)))

; BASICS AND REPRESENTATIONS

(defn id
  "Returns the id of a CollectionReference, DocumentReference or DocumentSnapshot."
  [d]
  (condp instance? d
    DocumentReference (.getId ^DocumentReference d)
    DocumentSnapshot (.getId ^DocumentSnapshot d)
    CollectionReference (.getId ^CollectionReference d)))

(defn parent
  "A reference to the collection to which this document belongs to."
  [^DocumentReference dr]
  (.getParent dr))

(defn path
  "A string representing the path of the referenced document or collection."
  [r]
  (condp instance? r
    DocumentReference (.getPath ^DocumentReference r)
    CollectionReference (.getPath ^CollectionReference r)))

(defn update-time
  "The time at which this document was last updated."
  [^DocumentSnapshot ds]
  (.getUpdateTime ds))

(defn read-time
  "The time at which this snapshot was read."
  [q]
  (condp instance? q
    DocumentSnapshot (.getReadTime ^DocumentSnapshot q)
    QuerySnapshot (.getReadTime ^QuerySnapshot q)))

(defn create-time
  "The time at which this document was created."
  [^DocumentSnapshot ds]
  (.getCreateTime ds))

(defn count
  "Number of documents in a QuerySnapshot"
  [^QuerySnapshot qs]
  (.size qs))

(defn ds->plain
  "Represents a DocumentSnapshot as plain map."
  [^DocumentSnapshot s]
  (->> (.getData s)
       (map (fn [[k v]]
              (if (instance? Timestamp v)
                [k (.toDate ^Timestamp v)]
                [k v])))
       (into {})))

(defn ds->plain-with-id
  "Represents a DocumentSnapshot as a [id plain-doc] pair"
  [^DocumentSnapshot s]
  [(id s) (ds->plain s)])

(defn qs->plainv
  "Represents a QuerySnapshot as a plain vector of document data."
  [^QuerySnapshot s]
  (mapv ds->plain s))

(defn qs->plainv-with-ids
  "Represents a QuerySnapshot as a plain vector of [id doc] pairs."
  [^QuerySnapshot s]
  (mapv ds->plain-with-id s))

(defn qs->plain-map
  "Represents a QuerySnapshot as a plain map whose keys are the document ids."
  [^QuerySnapshot s]
  (into {} (qs->plainv-with-ids s)))

(defn snapshot->data
  "Gets a DocumentSnapshot/CollectionSnapshot/QuerySnapshot's underlying data."
  [s]
  (condp instance? s
    DocumentSnapshot (ds->plain s)
    QuerySnapshot (qs->plain-map s)))

(defn doc-snapshot
  "Gets a QueryDocumentSnapshot given a DocumentReference and possibly a Transaction."
  ([^DocumentReference dr]
   (.get ^ApiFuture (.get dr)))
  ([^DocumentReference dr ^Transaction t]
   (.get ^ApiFuture (.get t dr))))

(defn query-snapshot
  "Gets a QuerySnapshot given a Query and possibly a Transaction."
  ([^Query q]
   (.get ^ApiFuture (.get q)))
  ([^Query q ^Transaction t]
   (.get ^ApiFuture (.get t q))))

(def ^{:doc "Pulls clojure data from a DocumentReference."}
  pull-doc (comp ds->plain doc-snapshot))

(def ^{:doc "Pulls results from a Query as clojure data."}
  pull-query (comp qs->plain-map query-snapshot))

(def ^{:doc "Pulls query results as a vector of document data."}
  pullv (comp qs->plainv query-snapshot))

(def ^{:doc "Pulls query results as a vector of pairs. Each pair has an id and document data"}
  pullv-with-ids (comp qs->plainv-with-ids query-snapshot))

(defn pull-docs
  "Pulls clojure data from a sequence of `DocumentReference`s, possibly inside a transaction context."
  ([drs]
   (mapv pull-doc drs))
  ([drs ^Transaction t]
   (->> drs
        (into-array DocumentReference)
        (.getAll t)
        (.get)
        (mapv ds->plain))))

(defn pull
  "Pulls data from a DocumentReference or Query, possibly inside a transaction context."
  ([q]
   (condp instance? q
     Query (pull-query q)
     DocumentReference (pull-doc q)))
  ([q ^Transaction t]
   (condp instance? q
     Query (pull-query t q)
     DocumentReference (pull-doc t q))))

(defn coll
  "Returns a CollectionReference for the collection of given name."
  ^CollectionReference [^Firestore db ^String coll-name]
  (.collection db coll-name))

(defn list-colls
  "Lists all collections"
  [^Firestore db]
  (into [] (.listCollections db)))

(defn doc
  "Gets a DocumentReference given CollectionReference and an id or Firestore and path. If id is not given, it will point
  to a new document with an auto-generated-id"
  (^DocumentReference [^CollectionReference cr]
   (.document cr))
  (^DocumentReference [cr ^String id]
   (condp instance? cr
     CollectionReference (.document ^CollectionReference cr id)
     Firestore (.document ^Firestore cr id))))

(defn docs
  "Gets a vector of `DocumentReference`s, given a vector of ids"
  [c ds]
  (mapv (partial doc c) ds))

; PRINT METHODS

(defmethod print-method DocumentReference [^DocumentReference dr ^Writer w]
  (let []
    (.write w
            (str "DocumentReference for \"" (path dr) "\"\n"
                 "Contents:\n" (pull-doc dr)))))

(defmethod print-method DocumentSnapshot [^DocumentSnapshot ds ^Writer w]
  (let [exists? (.exists ds)]
    (.write w
            (str "DocumentSnapshot for \"" (path (.getReference ds)) "\"\n"
                 (if exists?
                   (str
                     "Create time: " (create-time ds) "\n"
                     "Update time: " (update-time ds) "\n")
                   "")
                 "Read time:   " (read-time ds) "\n"
                 (if exists?
                   (str "Contents:\n" (ds->plain ds))
                   (str "Document does not exist."))))))

(defmethod print-method CollectionReference [^CollectionReference cr ^Writer w]
  (let [l (.listDocuments cr)
        [t d] (split-at 3 (seq l))]
    (.write w
            (str "CollectionReference for \"" (id cr) "\"\n"
                 (str
                   "Contents:\n("
                   (-> (string/join " \n\n" (for [dr t]
                                              (with-out-str (print dr))))
                       (string/replace #"\n" "\n "))
                   (if (empty? d)
                     ")"
                     "\n\n ...)"))))))

(defmethod print-method QuerySnapshot [^QuerySnapshot qs ^Writer w]
  (let [l (.getDocuments qs)
        [t d] (split-at 3 (seq l))]
    (.write w
            (str "QuerySnapshot instance\n"
                 "Read time: " (read-time qs) "\n"
                 "Count:     " (count qs) "\n"
                 (str
                   "Contents:\n("
                   (-> (string/join " \n\n" (for [dr t]
                                              (with-out-str (print dr))))
                       (string/replace #"\n" "\n "))
                   (if (empty? d)
                     ")"
                     "\n\n ...)"))))))

; REAL-TIME DATA

(defn add-listener
  "Adds a snapshot listener to a DocumentReference or Query.

  Listener is a fn of arity 2. First arg is the QuerySnapshot, second arg is a FirestoreException.
  Returns an ListenerRegistration object."
  [q f]
  (let [listener (reify
                   EventListener
                   (onEvent [_ s e]
                     (f s e)))]
    (condp instance? q
      Query (.addSnapshotListener ^Query q listener)
      DocumentReference (.addSnapshotListener ^DocumentReference q listener))))

(defn detach
  "Detaches an atom built with `->atom` or a listener returned from `add-listener`."
  [a]
  (condp instance? a
    IAtom (.remove ^ListenerRegistration (:registration (meta a)))
    ListenerRegistration (.remove ^ListenerRegistration a)))

(defn ->atom
  "Returns an atom holding the latest value of a DocumentReference or Query."
  ([q]
   (->atom q {}))
  ([q {:keys [error-handler plain-fn] :or {error-handler identity plain-fn snapshot->data}}]
   (let [a            (atom nil)
         registration (add-listener q (fn [s e]
                                        (if (some? e)
                                          (error-handler e)
                                          (reset! a (plain-fn s)))))
         p            (promise)]
     (alter-meta! a core/assoc :registration registration)
     (add-watch a :waiting-first-val (fn [_ _ _ _]
                                       (do
                                         (remove-watch a :waiting-first-val)
                                         (deliver p a))))
     @p)))

(defn ->stream
  "Returns a manifold stream pushing the latest values of a DocumentReference or Query."
  ([q]
   (->stream q {}))
  ([q {:keys [error-handler plain-fn] :or {error-handler identity plain-fn snapshot->data}}]
   (let [stream       (st/stream)
         registration (add-listener q (fn [s e]
                                        (if (some? e)
                                          (do
                                            (error-handler e)
                                            (st/close! stream))

                                          (if-not (st/closed? stream)
                                            (st/put! stream (plain-fn s))))))]
     (alter-meta! stream core/assoc :registration registration)
     (st/on-closed stream (fn []
                            (.remove ^ListenerRegistration (:registration (meta stream)))))
     stream)))

(def ^:private change-enum->change-kw {DocumentChange$Type/ADDED    :added
                                       DocumentChange$Type/REMOVED  :removed
                                       DocumentChange$Type/MODIFIED :modified})

(defn changes
  "Returns a vector of changes. Each change is a map with keys `:type`, `:reference`, `:new-index`,
  and `:old-index`. Type is one of `#{:added :removed :modified}`."
  [^QuerySnapshot s]
  (mapv (fn [^DocumentChange dc]
          {:type      (change-enum->change-kw (.getType dc))
           :reference (some-> (.getDocument dc)
                              (.getReference))
           :new-index (.getNewIndex dc)
           :old-index (.getOldIndex dc)})
        (.getDocumentChanges s)))

;; WRITE OPERATIONS

(defn array-union
  "Used with `set!` and `merge!`. Adds unique values to an array field."
  [& vs]
  (FieldValue/arrayUnion (into-array vs)))

(defn array-remove
  "Used with `set!` and `merge!`. Removes values from an array field."
  [& vs]
  (FieldValue/arrayRemove (into-array vs)))

(defn server-timestamp
  "Used with `set!` and `merge!`. Timestamp for when the update operation is performed on server."
  []
  (FieldValue/serverTimestamp))

(defn inc
  "Used with `set!` and `merge!`. Increments a numeric field."
  [v]
  (if (int? v)
    (FieldValue/increment ^long v)
    (FieldValue/increment ^double v)))

(defn mark-for-deletion
  "Used with `set!` and `merge!`. A sentinel value that marks a field for deletion."
  []
  (FieldValue/delete))

(defn add!
  "Adds a document to a collection. Its id will be automatically generated."
  [^CollectionReference cr m]
  (-> cr (.add (build-hash-map m)) (.get)))

(defn create!
  "Creates a new document at the DocumentReference's location. Fails if the document exists."
  [^DocumentReference dr m]
  (-> dr (.create (build-hash-map m)) (.get)))

(defn set!
  "Creates or overwrites a document."
  [^DocumentReference dr m]
  (-> dr (.set (build-hash-map m)) (.get)))

(defn set
  "Creates or overwrites a document in a batched write/transaction context."
  [^UpdateBuilder context ^DocumentReference dr m]
  (.set context dr (build-hash-map m)))

(defn delete!
  "Deletes a document."
  [^DocumentReference dr]
  (-> (.delete dr) (.get)))

(defn delete
  "Deletes a document in a batched write/transaction context."
  [^UpdateBuilder context ^DocumentReference dr]
  (-> (.delete context dr)))

(defn merge!
  "Updates fields of a document."
  [^DocumentReference dr m]
  (-> dr (.update (build-hash-map m)) (.get)))

(defn merge
  "Updates field of a document in a batched write/transaction context."
  [^UpdateBuilder context ^DocumentReference dr m]
  (.update context dr (build-hash-map m)))

(defn assoc!
  "Associates new fields and values."
  [^DocumentReference dr & fvs]
  (merge! dr (apply hash-map fvs)))

(defn assoc
  "Associates new fields and values in a batched write/transaction context."
  [context ^DocumentReference dr & fvs]
  (merge context dr (apply hash-map fvs)))

(defn dissoc!
  "Deletes fields."
  [^DocumentReference dr & fields]
  (->> fields
       (map (fn [field] [field (mark-for-deletion)]))
       (into {})
       (merge! dr)))

(defn dissoc
  "Deletes keys in a batched write/transaction context."
  [context ^DocumentReference dr & ks]
  (->> ks
       (map (fn [field] [field (mark-for-deletion)]))
       (into {})
       (merge context dr)))

; BATCHES AND TRANSACTIONS

(defn batch
  "Get a new write batch"
  [^Firestore db]
  (.batch db))

(defn commit!
  "Commits a write batch."
  [^WriteBatch b]
  (-> (.commit b) (.get)))

(defn- tx-option
  "Creates a Transaction$Function."
  [f]
  (reify
    Transaction$Function
    (updateCallback [_ t]
      (f t))))

(defn transact!
  "Performs a transaction. Optionally, you can specify an executor and the maximum number of attemps."
  ([^Firestore db f]
   (.get (.runTransaction db (tx-option f))))
  ([^Firestore db f {:keys [attempts executor] :as options}]
   (.get (.runTransaction db
                          (tx-option f)
                          (match [attempts executor]
                                 [nil nil] (TransactionOptions/create)
                                 [nil _] (TransactionOptions/create ^int attempts)
                                 [_ nil] (TransactionOptions/create ^Executor executor)
                                 [_ _] (TransactionOptions/create executor attempts))))))

(defn update!
  "Updates a document by applying a function to it."
  ([^Firestore db ^DocumentReference dr f & args]
   (transact! db (fn [tx]
                   (let [data (pull-doc dr tx)]
                     (set tx dr (apply f data args)))))))

(defn update-field!
  "Updates a single field of a document by applying a function to it."
  [^Firestore db ^DocumentReference dr field f & args]
  (update! db dr (fn [data]
                   (apply update data field f args))))

(defn map!
  "Updates all docs in a vector or query by applying a function to them"
  [^Firestore db q f & args]
  (transact! db (fn [tx]
                  (let [drs      (if (instance? Query q)
                                   (let [doclist (.getDocuments ^QuerySnapshot (.get ^ApiFuture (.get ^Transaction tx ^Query q)))]
                                     (mapv #(.getReference ^QueryDocumentSnapshot %) doclist))
                                   q)
                        all-data (pull-docs drs tx)]
                    (mapv #(set tx %1 (apply f %2 args)) drs all-data)))))

; QUERIES

(defn filter=
  "Filters where field = value. A map may be used for checking multiple equalities."
  ([^Query q m]
   (reduce (fn [q' [field value]]
             (filter= q' field value))
           q
           m))
  ([^Query q ^String field value]
   (.whereEqualTo q field value)))

(defn filter<
  "Filters where field < value."
  [^Query q ^String field value]
  (.whereLessThan q field value))

(defn filter<=
  "Filters where field <= value."
  [^Query q ^String field value]
  (.whereLessThanOrEqualTo q field value))

(defn filter>
  "Filters where field > value."
  [^Query q ^String field value]
  (.whereGreaterThan q field value))

(defn filter>=
  "Filters where field >= value."
  [^Query q ^String field value]
  (.whereGreaterThanOrEqualTo q field value))

(defn take
  "Limits results to a certain number."
  [^Query q n]
  (.limit q n))

(defn filter-in
  "Filters where field is one of the values in arr."
  [^Query q ^String field ^List arr]
  (.whereIn q field arr))

(defn filter-contains
  "Filters where field contains value."
  [^Query q ^String field value]
  (.whereArrayContains q field value))

(defn filter-contains-any
  "Filters where field contains one of the values in arr."
  [^Query q ^String field ^List arr]
  (.whereArrayContainsAny q field arr))

(s/def ::direction #{:asc :desc})
(s/def ::ordering (s/+ (s/alt
                         :field-direction (s/cat :field string? :direction ::direction)
                         :field string?)))

(defn order-by
  "Orders by a sequence of fields with optional directions. Notice that ordering by multiple fields
  requires creation of a composite index."
  [^Query q & ordering]
  (let [conformed (s/conform ::ordering ordering)]
    (if (= conformed ::s/invalid)
      (throw (ex-info "Invalid ordering: " (s/explain-str ::ordering ordering)))
      (reduce (fn [^Query q' [spec value]]
                (case spec
                  :field (.orderBy q' ^String value)
                  :field-direction (let [{:keys [field direction]} value]
                                     (if (= direction :desc)
                                       (.orderBy q' ^String field Query$Direction/DESCENDING)
                                       (.orderBy q' ^String field)))))
              q
              conformed))))