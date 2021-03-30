(ns firestore-clj.core
  (:refer-clojure :exclude [set set! merge merge! assoc assoc! dissoc dissoc! range inc update! count ref])
  (:require [clojure.core :as core]
            [clojure.core.match :refer [match]]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [manifold.stream :as st]
            [firestore-clj.google :as google])
  (:import (clojure.lang IAtom)
           (com.google.api.core ApiFuture)
           (com.google.auth.oauth2 GoogleCredentials)
           (com.google.cloud Timestamp)
           (com.google.cloud.firestore Firestore QuerySnapshot CollectionReference EventListener DocumentReference
                                       DocumentSnapshot Query$Direction FieldValue Query ListenerRegistration WriteBatch
                                       Transaction UpdateBuilder Transaction$Function TransactionOptions
                                       QueryDocumentSnapshot DocumentChange$Type DocumentChange GeoPoint Precondition WriteResult
                                       SetOptions FirestoreOptions FirestoreOptions$Builder)
           (com.google.firebase FirebaseApp FirebaseOptions FirebaseOptions)
           (com.google.firebase.cloud FirestoreClient)
           (java.io Writer)
           (java.util HashMap List Date)
           (java.util.concurrent Executor)
           (firestore_clj VariadicHelper)))

(defn- build-hash-map
  "Helper for build java.util.HashMap without reflection."
  ^HashMap [m]
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

(defn emulator-client
  "Gets a client using the emulator."
  ^Firestore [project-id emulator-host]
  (let [credentials               (google/mk-fake-credentials)
        channel-provider          (google/mk-emulator-grpc-channel-provider emulator-host)
        ^FirestoreOptions options (-> (FirestoreOptions/getDefaultInstance)
                                      (.toBuilder)
                                      ^FirestoreOptions$Builder
                                      (.setProjectId project-id)
                                      (.setChannelProvider channel-provider)
                                      ^FirestoreOptions$Builder
                                      (.setCredentials credentials)
                                      ^FirestoreOptions (.build))]
    (.getService options)))

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

(defn ref
  "Gets a reference to the document this snapshot refers to."
  [^QueryDocumentSnapshot ds]
  (.getReference ds))

(defn exists?
  "Checks whether the document snapshot exists."
  [^DocumentSnapshot ds]
  (.exists ds))

(defn path
  "A string representing the path of the referenced document or collection."
  [r]
  (condp instance? r
    DocumentReference (.getPath ^DocumentReference r)
    CollectionReference (.getPath ^CollectionReference r)))

(defn firestore
  "Gets the Firestore instance associated with this query."
  [q]
  (condp instance? q
    Query (.getFirestore ^Query q)
    DocumentReference (.getFirestore ^DocumentReference q)))

(defn update-time
  "The time at which this document was last updated."
  [^DocumentSnapshot ds]
  (.toDate (.getUpdateTime ds)))

(defn read-time
  "The time at which this snapshot was read."
  [q]
  (condp instance? q
    DocumentSnapshot (.toDate ^Timestamp (.getReadTime ^DocumentSnapshot q))
    QuerySnapshot (.toDate ^Timestamp (.getReadTime ^QuerySnapshot q))))

(defn create-time
  "The time at which this document was created."
  [^DocumentSnapshot ds]
  (.toDate (.getCreateTime ds)))

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

(defn snap->plain
  "Gets a DocumentSnapshot/CollectionSnapshot/QuerySnapshot's underlying data."
  [s]
  (condp instance? s
    DocumentSnapshot (ds->plain s)
    QuerySnapshot (qs->plain-map s)))

(defn doc-snap
  "Gets a QueryDocumentSnapshot given a DocumentReference and possibly a Transaction."
  ([^DocumentReference dr]
   (.get ^ApiFuture (.get dr)))
  ([^DocumentReference dr ^Transaction t]
   (.get ^ApiFuture (.get t dr))))

(defn query-snap
  "Gets a QuerySnapshot given a Query and possibly a Transaction."
  ([^Query q]
   (.get ^ApiFuture (.get q)))
  ([^Query q ^Transaction t]
   (.get ^ApiFuture (.get t q))))

(defn snap
  "Gets a DocumentSnapshot or QuerySnapshot given a DocumentReference or Query."
  ([q]
   (condp instance? q
     Query (query-snap q)
     DocumentReference (doc-snap q)))
  ([q ^Transaction t]
   (condp instance? q
     Query (query-snap q t)
     DocumentReference (doc-snap q t))))

(def ^{:doc "Pulls clojure data from a DocumentReference."}
  pull-doc (comp ds->plain doc-snap))

(def ^{:doc "Pulls results from a Query as clojure data."}
  pull-query (comp qs->plain-map query-snap))

(def ^{:doc "Pulls query results as a vector of document data."}
  pullv (comp qs->plainv query-snap))

(def ^{:doc "Pulls query results as a vector of pairs. Each pair has an id and document data"}
  pullv-with-ids (comp qs->plainv-with-ids query-snap))

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
  ^CollectionReference [r ^String coll-name]
  (condp instance? r
    Firestore (.collection ^Firestore r coll-name)
    DocumentReference (.collection ^DocumentReference r coll-name)))

(defn colls
  "Returns collections or subcollections as a vector of CollectionReference."
  ([d]
   (into [] (condp instance? d
              Firestore (.listCollections ^Firestore d)
              DocumentReference (.listCollections ^DocumentReference d))))
  ([d cs]
   (mapv (partial coll d) cs)))

(defn coll-group
  "Returns a query that includes all documents contained in a collection or subcollection with the given id."
  [^Firestore db coll-id]
  (.collectionGroup db coll-id))

(defn doc
  "Gets a DocumentReference given CollectionReference or Firestore and a path. If path is not given, it will point
  to a new document with an auto-generated-id"
  (^DocumentReference [^CollectionReference cr]
   (.document cr))
  (^DocumentReference [cr ^String id]
   (condp instance? cr
     CollectionReference (.document ^CollectionReference cr id)
     Firestore (.document ^Firestore cr id))))

(defn docs
  "Gets a vector of `DocumentReference`s."
  ([^CollectionReference c]
   (into [] (.listDocuments c)))
  ([c ds]
   (mapv (partial doc c) ds)))

(defn query-snap->doc-snaps
  "Gets DocumentSnapshots from a QuerySnapshot"
  [^QuerySnapshot qs]
  (into [] (.getDocuments qs)))

; PRINT METHODS

(defmethod print-method Firestore [^Firestore db ^Writer w]
  (let [subcolls (colls db)]
    (.write w
            (str "Firestore instance. \n"
                 "Collections: " (mapv id subcolls)))))

(defmethod print-method DocumentReference [^DocumentReference dr ^Writer w]
  (let [subcolls (colls dr)]
    (.write w
            (str "DocumentReference for \"" (path dr) "\"\n"
                 "Contents:\n" (pull-doc dr)
                 (if-not (empty? subcolls)
                   (str "\nSubcollections: " (mapv id subcolls)))))))

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
  (let [l (docs cr)
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
  (let [l (query-snap->doc-snaps qs)
        [t d] (split-at 3 l)]
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
  ([q {:keys [error-handler plain-fn] :or {error-handler identity plain-fn snap->plain}}]
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
  ([q {:keys [error-handler plain-fn] :or {error-handler identity plain-fn snap->plain}}]
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
  "Returns a vector of changes. Each change is a map with keys `:type`, `:ref`, `:new-index`,
  and `:old-index`. Type is one of `#{:added :removed :modified}`."
  [^QuerySnapshot s]
  (mapv (fn [^DocumentChange dc]
          {:type      (change-enum->change-kw (.getType dc))
           :reference (some-> (.getDocument dc)
                              ref)
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
  ([^DocumentReference dr m]
   (-> (.set dr (build-hash-map m))
       (.get)))
  ([^DocumentReference dr m kw]
   (let [options (case kw
                   :merge (SetOptions/merge))]
     (-> (.set dr (build-hash-map m) ^SetOptions options)
         (.get))))
  ([^DocumentReference dr m kw args]
   (let [options (case kw
                   :merge-fields (VariadicHelper/mergeFields (into-array String args)))]
     (-> (.set dr (build-hash-map m) ^SetOptions options)
         (.get)))))

(defn set
  "Creates or overwrites a document in a batched write/transaction context."
  ([^UpdateBuilder context ^DocumentReference dr m]
   (.set context dr (build-hash-map m)))
  ([^UpdateBuilder context ^DocumentReference dr m kw]
   (let [options (case kw
                   :merge (SetOptions/merge))]
     (.set context dr (build-hash-map m) ^SetOptions options)))
  ([^UpdateBuilder context ^DocumentReference dr m kw args]
   (let [options (case kw
                   :merge-fields (VariadicHelper/mergeFields (into-array String args)))]
     (.set context dr (build-hash-map m) ^SetOptions options))))

(defn delete!
  "Deletes a document."
  [^DocumentReference dr]
  (-> (.delete dr) (.get)))

(defn delete
  "Deletes a document in a batched write/transaction context."
  [^UpdateBuilder context ^DocumentReference dr]
  (-> (.delete context dr)))

(defn merge!
  "Updates fields of a document. Accepts an updated-time as precondition."
  ([^DocumentReference dr m]
   (-> dr (.update (build-hash-map m)) (.get)))
  ([^DocumentReference dr m updated-time]
   (-> dr (.update (build-hash-map m) (Precondition/updatedAt (Timestamp/of ^Date updated-time))) (.get))))

(defn merge
  "Updates field of a document in a batched write/transaction context."
  ([^UpdateBuilder context ^DocumentReference dr m]
   (.update context dr (build-hash-map m)))
  ([^UpdateBuilder context ^DocumentReference dr m updated-time]
   (.update context dr (build-hash-map m) (Precondition/updatedAt (Timestamp/of ^Date updated-time)))))

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
  (^Transaction [^Firestore db f]
   (.get (.runTransaction db (tx-option f))))
  (^Transaction [^Firestore db f {:keys [attempts executor] :as options}]
   (.get (.runTransaction db
                          (tx-option f)
                          (match [attempts executor]
                                 [nil nil] (TransactionOptions/create)
                                 [nil _] (TransactionOptions/create ^int attempts)
                                 [_ nil] (TransactionOptions/create ^Executor executor)
                                 [_ _] (TransactionOptions/create executor attempts))))))

(defn update!
  "Updates a document by applying a function to it."
  ([^DocumentReference dr f & args]
   (transact! (firestore dr) (fn [tx]
                               (let [data (pull-doc dr tx)]
                                 (set tx dr (apply f data args)))))))

(defn update-field!
  "Updates a single field of a document by applying a function to it."
  [^DocumentReference dr field f & args]
  (update! (firestore dr) dr (fn [data]
                               (apply update data field f args))))

(defn map!
  "Updates all docs in a vector or query by applying a function to them"
  [q f & args]
  (let [db (if (vector? q) (firestore (first q)) (firestore q))]
    (transact! db (fn [tx]
                    (let [drs (if (instance? Query q)
                                (->> (query-snap q tx)
                                     (query-snap->doc-snaps)
                                     (mapv ref))
                                q)
                          all-data (pull-docs drs tx)]
                      (mapv #(set tx %1 (apply f %2 args)) drs all-data))))))

(declare limit)
(declare offset)

(defn delete-all!
  "Deletes all documents from a query. Batches for efficiency. Query must not contain limit or offset."
  ([^Query q]
   (delete-all! q 500))
  ([^Query q batch-size]
   (loop []
     (let [snaps (->> (limit q batch-size)
                      (query-snap)
                      (query-snap->doc-snaps))
           len   (core/count snaps)
           b     (batch (firestore q))]

       (doseq [snap snaps]
         (delete b (ref snap)))

       (commit! b)

       (when (>= len batch-size)
         (recur))))))

(defn batch-delete!
  "Deletes all documents in a seq of DocumentReference."
  [doc-seq batch-size]
  (when-let [d (first doc-seq)]
    (let [fstore (firestore d)]

      (loop [[delete-now remaining] (split-at batch-size doc-seq)]

        (let [b (batch fstore)]
          (doseq [r delete-now]
            (delete b r))

          (commit! b)

          (when-not (empty? remaining)
            (recur (split-at batch-size remaining))))))))

(defn delete-all!*
  "Deletes all documents resulting from a query. Batches for efficiency. Fetches all results in memory."
  ([^Query q]
   (delete-all!* q 500))
  ([^Query q batch-size]
   (let [snaps (query-snap->doc-snaps (query-snap q))]
     (batch-delete! (mapv ref snaps) batch-size))))

(defn subdocs
  "A lazy sequence including given document and all documents in its subcollections."
  [^DocumentReference dr]
  (lazy-seq
    (cons dr
          (let [children (mapcat docs (colls dr))]
            (when (seq? children)
              (mapcat subdocs children))))))

(defn purge!
  "If a document, deletes it and all its subcollections recursively. If a collection or query,
  purges all its documents. Operates in batches."
  ([root]
   (purge! root 500))
  ([root batch-size]
   (let [doclist (condp instance? root
                   DocumentReference (subdocs root)
                   CollectionReference (mapcat subdocs (docs root))
                   Query (->> (query-snap root)
                              query-snap->doc-snaps
                              (map ref)
                              (mapcat subdocs)))]
     (batch-delete! doclist batch-size))))

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

(defn limit
  "Limits results to a certain number."
  [^Query q n]
  (.limit q n))

(defn offset
  "Skips the first n results."
  [^Query q n]
  (.offset q n))

(defn select
  "Selects given fields"
  [^Query q & fields]
  (VariadicHelper/select q (into-array String fields)))

(defn range
  "Gets a range"
  [^Query q start end]
  (-> (offset q start)
      (limit (- end start))))

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

(defn start-at
  "Starts results at the provided document, or at the provided fields relative to the order of the query."
  [^Query q & args]
  (let [fst (first args)]
    (condp instance? fst
      DocumentSnapshot (.startAt q ^DocumentSnapshot fst)
      Object (VariadicHelper/startAt q (into-array Object args)))))

(defn start-after
  "Starts results after the provided document, or at the provided fields relative to the order of the query."
  [^Query q & args]
  (let [fst (first args)]
    (condp instance? fst
      DocumentSnapshot (.startAfter q ^DocumentSnapshot fst)
      Object (VariadicHelper/startAfter q (into-array Object args)))))

(defn end-at
  "Ends results at the provided document, or at the provided fields relative to the order of the query."
  [^Query q & args]
  (let [fst (first args)]
    (condp instance? fst
      DocumentSnapshot (.endAt q ^DocumentSnapshot fst)
      Object (VariadicHelper/endAt q (into-array Object args)))))

(defn end-before
  "Ends results before the provided document, or at the provided fields relative to the order of the query."
  [^Query q & args]
  (let [fst (first args)]
    (condp instance? fst
      DocumentSnapshot (.endBefore q ^DocumentSnapshot fst)
      Object (VariadicHelper/endBefore q (into-array Object args)))))

(defn geo-point
  "Creates a geocode object from a lat lon pair."
  [[lat lon]]
  (GeoPoint. lat lon))

(defn lat-lon
  "Returns a lat lon pair from a GeoPoint."
  [^GeoPoint g]
  [(.getLatitude g) (.getLongitude g)])
