(defproject lurodrigo/firestore-clj "1.2.1"
  :description "A Firestore API for Clojure. Provides tools for doing single pulls and writes, streaming real-time data, batched writes and transactions."
  :url "https://github.com/polvotech/firestore-clj"
  :license {:name "MIT LICENSE"
            :url  "https://github.com/lurodrigo/firestore-clj/blob/master/LICENSE"}
  :scm {:name "git" :url "https://github.com/lurodrigo/firestore-clj"}
  :plugins [[lein-codox "0.10.7"]]
  :dependencies [[org.clojure/clojure "1.10.1" :scope "provided"]
                 [org.clojure/core.match "1.0.0"]
                 [com.google.firebase/firebase-admin "7.1.0"]
                 [com.google.cloud/google-cloud-firestore "2.2.4"]
                 [manifold "0.1.8"]]
  :aot [firestore-clj.google.fake-credentials
        firestore-clj.google.emulator-channel-configurator]
  :profiles {:dev {:dependencies [[com.taoensso/timbre "4.10.0"]
                                  [com.fzakaria/slf4j-timbre "0.3.19"]
                                  [com.google.cloud/google-cloud-logging-logback "0.116.0-alpha"]]
                   :source-paths ["test/clj"]
                   :java-source-paths ["test/java"]}}
  :global-vars {*warn-on-reflection* true}
  :jar-exclusions [#"user\.clj"]
  :uberjar-exclusions [#"user\.clj"]
  :source-paths ["src/clj"]
  :java-source-paths ["src/java"])
