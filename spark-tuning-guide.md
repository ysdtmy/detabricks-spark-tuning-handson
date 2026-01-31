# **Databricks Spark パフォーマンスチューニングおよび最適化に関する包括的技術レポート**

## **エグゼクティブサマリー**

本レポートは、Databricks プラットフォーム上での Apache Spark ワークロードのパフォーマンス最適化に関する包括的な技術ガイドです。従来のオンプレミス Hadoop 環境や素の IaaS 上での Spark 運用とは異なり、Databricks は「レイクハウス（Lakehouse）」アーキテクチャを基盤とした独自の最適化レイヤー（Photon エンジン、Delta Lake、Predictive Optimization）を備えています 1。本稿では、これらの独自機能の内部動作原理から、Spark Core の低レベルチューニング、そして Spark UI や Ganglia を用いた科学的なボトルネック特定手法に至るまで、網羅的かつ詳細に解説します。特に、近年導入された Liquid Clustering や Photon のベクトル化実行モデルが、従来のパーティショニングや JVM ベースの実行とどのように異なるかを対比させながら、最新のベストプラクティスを提示することを目的としています。読者は本レポートを通じて、SLA の遵守とクラウドコスト（DBU）の削減を両立させるための、実践的かつ高度なエンジニアリング手法を習得することができます。

## ---

**第1章 Databricks ランタイムアーキテクチャとパフォーマンスの基礎**

Databricks Runtime (DBR) は、Apache Spark の単なるディストリビューションではなく、ストレージ I/O、ネットワーク通信、およびクエリ実行エンジンの全レイヤーにおいて大幅な改良が施された統合プラットフォームです。パフォーマンスエンジニアリングを行う上で、まず OSS (Open Source Software) Spark と DBR の構造的な差異を理解することは不可欠です。

### **1.1 DBR と OSS Spark の決定的な差異**

多くのデータエンジニアが直面する最初の課題は、ローカル環境や他社クラウドのマネージド Spark サービスで培った知識が、Databricks 上でそのまま通用しない場合があるという点です。これは、DBR がデフォルトで多数の最適化設定を適用しており、かつ独自のプロプライエタリなコンポーネントを含んでいるためです 3。

最も顕著な違いは I/O レイヤーにあります。OSS Spark が Hadoop File System (HDFS) API を通じて S3 や ADLS にアクセスする際、リスティング操作や整合性確保のために多大なオーバーヘッドが発生することが一般的でした。対して DBR は、クラウドオブジェクトストレージに特化した「DBIO (Databricks I/O)」モジュールをカーネルレベルで統合しています。これにより、S3 の LIST 操作の遅延を隠蔽し、並列アップロードを最適化することで、OSS Spark と比較して数倍から数十倍の I/O スループットを実現します。

また、設定管理の哲学も異なります。OSS Spark では spark-defaults.conf を通じて spark.executor.memory や spark.sql.shuffle.partitions を手動で細かく調整することが「チューニング」と見なされがちですが、DBR においては、これらの多くがワークロードのサイズに応じて自動調整されるか、あるいは Adaptive Query Execution (AQE) によって実行時に動的に決定されます。したがって、レガシーな設定値を無批判に適用することは、逆に DBR の自動最適化機構を阻害する「アンチパターン」となり得ます 3。

### **1.2 Lakehouse アーキテクチャがもたらすパフォーマンスの利点**

Databricks が提唱する Lakehouse アーキテクチャは、データウェアハウス（DWH）のパフォーマンスと信頼性を、データレイクの柔軟性と低コストなストレージ上で実現するものです。このアーキテクチャの中核を担うのが Delta Lake です。Delta Lake は、Parquet ファイルとトランザクションログ（\_delta\_log）を組み合わせることで、以下のパフォーマンス上の利点を提供します 1。

1. **メタデータ操作のスケーラビリティ:** 従来の Hive Metastore 形式では、数万〜数百万のファイルを持つテーブルに対するパーティション探索がボトルネックとなっていました。Delta Lake はメタデータを分散処理可能なログとして保持するため、Spark 自体を使ってメタデータを処理できます。これにより、ペタバイト規模のテーブルであっても、ファイルリスティングにかかる時間を大幅に短縮できます。  
2. **ACID トランザクションによる読み書きの分離:** スナップショット分離（Snapshot Isolation）により、書き込みジョブが実行中であっても、読み取りクエリは一貫したスナップショットに対してロックフリーでアクセスできます。これにより、DWH で頻発するロック待ちによるパフォーマンス低下を回避できます。

### **1.3 コンピュートの分離とサーバーレス化**

近年のトレンドとして、コンピュートリソースとストレージの完全な分離が進んでいます。Databricks SQL (Serverless) などの環境では、ユーザーはクラスターの起動時間やインスタンスタイプを意識することなくクエリを投げることができます。この裏側では、マルチテナントの強力なコンピュートプールが待機しており、クエリが到着した瞬間にリソースが割り当てられます。これにより、「クラスターの起動待ち時間（Cold Start）」という古典的なボトルネックが解消され、BI ダッシュボードなどのインタラクティブなワークロードの応答速度が劇的に向上します。

## ---

**第2章 Photon エンジン：次世代の実行モデル**

Databricks におけるパフォーマンス最適化の最大の柱の一つが、Photon エンジンです。これは従来の Spark 実行エンジンを根本から再設計したものであり、現代のハードウェア性能を極限まで引き出すために開発されました。

### **2.1 JVM ベース実行の限界と Photon の登場**

従来の Spark SQL エンジンは、Java Virtual Machine (JVM) 上で動作します。Spark 2.0 で導入された Whole-Stage Code Generation (Codegen) は、クエリプランを実行時に Java バイトコードにコンパイルすることで、仮想関数呼び出しのオーバーヘッドを削減し、パフォーマンスを向上させました 5。しかし、Codegen には以下の限界がありました。

* **JVM のオーバーヘッド:** オブジェクトの生成とガベージコレクション（GC）による停止時間が、大規模データ処理において無視できないコストとなります。  
* **命令レベルの並列性の欠如:** Java の JIT コンパイラは進化していますが、最新の CPU が備える SIMD (Single Instruction, Multiple Data) 命令を常に最適に活用できるわけではありません。  
* **行指向処理の残存:** メモリ上でのデータ表現が行指向である場合、キャッシュミスが発生しやすく、メモリ帯域幅を浪費します。

Photon は、C++ で実装されたベクトル化クエリエンジンであり、これらの課題を解決します 6。Photon はデータをカラムナ形式（列指向）のままメモリ上で保持し、SIMD 命令を使用して複数のデータポイントを単一の CPU 命令で処理します。これにより、従来の JVM ベースの実行と比較して、データのスキャン、フィルタリング、集計、ハッシュ結合などの操作が劇的に高速化されます。

### **2.2 ベクトル化実行のメカニズム**

Photon の核心は「ベクトル化（Vectorization）」にあります。従来のモデルでは「1行読み込み、解釈し、処理する」というサイクルを繰り返していましたが、Photon では「データのバッチ（ベクトル）を読み込み、CPU のベクトル演算器で一括処理」します。

例えば、WHERE price \> 100 というフィルタ処理を行う場合、Photon は price 列のメモリブロックをロードし、SIMD 命令を用いて一度に（例えば）4つや8つの数値を比較します。これにより、分岐予測ミス（Branch Misprediction）のリスクを減らし、CPU パイプラインを最大限に稼働させることができます。また、C++ ネイティブでメモリを管理するため、Java ヒープを使用せず、GC の影響を受けません。これは、メモリ集中型のワークロードにおいて安定性を大きく向上させます 6。

### **2.3 Photon の適用範囲とフォールバック**

Photon は Spark と完全に互換性があるように設計されていますが、すべての操作をサポートしているわけではありません。クエリプランの中に Photon がサポートしていない演算子（例えば、複雑な UDF や特定のデータ型操作）が含まれている場合、Photon は透過的かつ自動的に従来の Spark SQL エンジン（JVM）に処理を委譲（フォールバック）します 7。

**Photon が特に有効なシナリオ:**

* 大規模なファクトテーブルに対する結合（Join）と集計（Aggregation）。  
* Parquet や Delta 形式のデータの読み書き（特にエンコーディングのデコード処理）。  
* MERGE 文などの書き込み集中型ワークロード。

**Photon の制限事項と注意点:**

* **UDF (User Defined Functions):** Python UDF や、Java/Scala で書かれた複雑な UDF は Photon 内部では実行できません。これらが使用されると、データは Photon のネイティブメモリから JVM ヒープへ転送（Row-to-Column 変換の逆）される必要があり、このシリアライゼーションコストがパフォーマンス向上の恩恵を相殺してしまう可能性があります 6。  
* **短時間クエリ:** 数秒で終わるような小規模なクエリでは、Photon エンジンの初期化やプランニングのオーバーヘッドが支配的になり、速度向上が体感できない場合があります 9。

クエリが実際に Photon で実行されているかどうかを確認するには、クエリプロファイル（後述）の「Time in Photon」などのメトリクスを参照する必要があります 7。

## ---

**第3章 Delta Lake ストレージレイヤのチューニング**

コンピュートエンジンの性能がいかに高くても、読み込むデータ構造が非効率であれば、パフォーマンスは頭打ちになります。「Garbage In, Garbage Out」の原則はパフォーマンスにも当てはまります。Databricks におけるチューニングの第一歩は、常にストレージレイヤの最適化（データレイアウト設計）から始めるべきです。

### **3.1 ファイルサイズ管理：Small File Problem の克服**

Spark のような分散処理システムにおいて、最も一般的かつ致命的な問題の一つが「Small File Problem（小規模ファイル問題）」です。数キロバイト〜数メガバイトの小さなファイルが大量に存在すると、以下の弊害が発生します。

1. **メタデータ負荷:** Driver ノードが大量のファイルメタデータを管理するためにメモリを消費し、OOM の原因となります。  
2. **I/O スループット低下:** 各ファイルのオープン/クローズ処理や、S3 へのリクエストオーバーヘッドが実データ転送時間を上回ります。

**推奨ファイルサイズ:** Databricks は一般的に、1つのファイルを 128MB 〜 1GB 程度のサイズにすることを推奨しています 11。このサイズ感は、I/O の効率性と並列度（コア数に対するタスク分割）のバランスが取れているためです。

**対策:**

* **OPTIMIZE コマンド:** 定期的に OPTIMIZE table\_name を実行することで、既存の小さなファイルを読み込み、より大きなファイルに書き直します（Bin-packing）。これは、ストリーミング取込などで断片化したテーブルに対して必須のメンテナンスです 12。  
* **Auto Compaction:** 書き込みトランザクションの一部として自動的に小規模ファイルをマージする機能です。レイテンシよりも読み取り性能を重視する場合に有効です 14。  
* **Optimized Writes:** 書き込みを行う前にシャッフルを挟むことで、各 Executor が十分な大きさのデータを書き込めるようにします。

### **3.2 データスキッピング：不要な I/O を排除する技術**

クエリの高速化において最も効果的なのは「データを読まないこと」です。Delta Lake は、各データファイルに含まれる列の最小値（Min）と最大値（Max）の統計情報を \_delta\_log に保持しています。クエリの WHERE 句がこの範囲外であれば、Spark はそのファイルの読み込みを完全にスキップします（Data Skipping）1。この効果を最大化するためには、関連するデータが物理的に近くに配置されている必要があります。

### **3.3 データレイアウト戦略の比較：Partitioning vs Z-Order vs Liquid Clustering**

データをどのように物理的に配置するかは、パフォーマンスエンジニアリングの最も重要な意思決定の一つです。

#### **3.3.1 パーティショニング (Partitioning)**

従来の Hive スタイルのディレクトリ分割（例: /data/year=2023/month=01/）です。

* **長所:** パーティション列に対するフィルタリングが明確に機能し、不要なディレクトリを完全に無視できます。  
* **短所:** カーディナリティ（値の種類）が高い列でパーティションを切ると、大量のディレクトリと小規模ファイルが生成され、パフォーマンスが劇的に悪化します。また、一度決めたパーティションキーを変更するには全データの書き直しが必要です 16。  
* **適用基準:** テーブルサイズが数 TB 以上で、かつフィルタリング条件が固定されている場合のみ推奨されます 1。

#### **3.3.2 Z-Order インデックス (Z-Ordering)**

Z-Order は、多次元データを1次元にマッピングする空間充填曲線（Z-curve）アルゴリズムを用いて、複数の列（例: customer\_id と timestamp）の相関関係を保ったままデータをソート・配置します 12。

* **長所:** 高カーディナリティの列に対しても有効であり、複数の列を組み合わせたフィルタリングで極めて高いスキッピング性能を発揮します。  
* **短所:** データの追加（Append）時には適用されず、定期的に OPTIMIZE ZORDER BY... を実行する必要があります。この再構成処理（Clustering）は計算コストが高い操作です。

#### **3.3.3 Liquid Clustering（リキッドクラスタリング）：新時代の標準**

2024年以降、Databricks が最も強く推奨しているのが Liquid Clustering です。これは従来のパーティショニングと Z-Order の欠点を解消するために開発された動的なデータレイアウト技術です 18。

* **仕組み:** 固定された物理ディレクトリ構造を持たず、Delta Lake の内部管理によってデータを動的にクラスタリングします。書き込み時（Write-time）に軽量なクラスタリングを行い、非同期の OPTIMIZE ジョブでより深い最適化を行います。  
* **Z-Order との違い:** Z-Order が全ファイルの書き直しを伴う高コストな操作になりがちなのに対し、Liquid Clustering は「増分的な（Incremental）」最適化が可能です。変更があった部分のみを効率的に再配置するため、メンテナンスコストが低く抑えられます。  
* **スケーラビリティ:** データの増加に合わせてファイルサイズやクラスタリングの粒度を自動調整するため、小規模テーブルからペタバイト級のテーブルまでシームレスに対応します。  
* **推奨:** 新規に作成するほぼすべての Delta テーブルにおいては、パーティショニングではなく Liquid Clustering (CLUSTER BY 句) の使用が推奨されます 19。

以下の表に、3つの戦略の比較をまとめます。

| 特徴 | Partitioning | Z-Order | Liquid Clustering |
| :---- | :---- | :---- | :---- |
| **設定の柔軟性** | 低 (作成時に固定、変更困難) | 中 (コマンドで指定、変更可) | **高** (いつでも変更可能) |
| **書き込みコスト** | 低 (ただし小ファイル問題のリスク) | 高 (全量シャッフルに近い負荷) | **中〜低** (増分処理により効率的) |
| **データスキュー耐性** | 低 (特定のパーティションが肥大化) | 中 | **高** (動的に分割・統合) |
| **メンテナンス** | 手動 (ファイルサイズ管理が必要) | 定期的なフル実行が必要 | **自動化** (Predictive Optimizationと連携) |
| **推奨ユースケース** | レガシー移行、明確な日付分割 | 頻繁に更新されない静的データ | **現在のデフォルト推奨** 18 |

### **3.4 Predictive Optimization による自律運用**

Databricks の最新の進化の一つが Predictive Optimization（予測的最適化）です。これは Unity Catalog の管理テーブル（Managed Table）に対して、AI がテーブルの使用状況や変更履歴を分析し、最適なタイミングで OPTIMIZE（Compaction/Clustering）や VACUUM（不要ファイルの削除）を自動実行する機能です 20。

従来、エンジニアは「毎晩深夜2時に OPTIMIZE を走らせる」といった固定スケジュールを組んでいましたが、これはデータ更新がない日には無駄なコストとなり、逆に更新が頻繁な日には最適化が追いつかないという問題がありました。Predictive Optimization はこのジレンマを解消し、コスト対効果がプラスになると判断された場合のみメンテナンスジョブを実行します。これにより、ストレージレイヤの健康状態が自律的に保たれ、エンジニアは手動チューニングから解放されます。

## ---

**第4章 Spark Core チューニング：メモリ管理とシャッフル**

コンピュートエンジンの設定、特にメモリ管理とシャッフル動作の調整は、ジョブの安定性と速度に直結します。

### **4.1 メモリ管理の内部構造とチューニング**

Spark の Executor メモリは、主に以下の領域に分割されています 22。

1. **Spark Memory (Heap):** spark.memory.fraction (デフォルト 0.6) で割り当てられる領域。  
   * **Storage Memory:** キャッシュデータ (.persist()) のための領域。  
   * **Execution Memory:** シャッフル、結合、ソート、集計などの中間計算に使われる領域。これら2つは「Unified Memory Manager」によって動的に融通されます。  
2. **User Memory:** UDF やユーザー定義のデータ構造、Spark 内部メタデータに使われる領域。  
3. **Reserved Memory:** システム予約領域（通常 300MB）。

**Off-Heap Memory の活用:** Databricks の一部のランタイムや Photon 環境では、JVM のガベージコレクション（GC）の影響を避けるために、Off-Heap メモリ（ネイティブメモリ）を積極的に活用します 22。これにより、巨大なヒープを持つ Executor で頻発する「Stop-the-World」GC ポーズを軽減できます。

**OOM (Out Of Memory) への対処:**

OOM エラーが発生した場合、まず「Driver OOM」か「Executor OOM」かを区別する必要があります。

* **Driver OOM:** collect() の多用や、Broadcast 結合の閾値超過が主な原因です 24。  
* **Executor OOM:** パーティションサイズが大きすぎて Execution Memory に収まらず、かつディスクへのスピル（Spill）も間に合わない場合に発生します。対策としては、並列度（spark.sql.shuffle.partitions）を上げる、コアあたりのメモリ比率が高いインスタンスタイプに変更する、などが挙げられます 25。

### **4.2 シャッフル（Shuffle）のメカニズムと最適化**

シャッフルは、ステージ間でデータを再分配する（例えば GROUP BY や JOIN のために同じキーを持つデータを同じノードに集める）操作であり、ネットワーク I/O とディスク I/O を伴う最もコストの高い処理です。

#### **4.2.1 Sort Shuffle vs Hash Shuffle**

現代の Spark はデフォルトで Sort Shuffle を使用します。これは、Map 側でデータをソートしてから書き出し、Reduce 側でマージソートしながら読み込む方式で、メモリ効率が良い反面、CPU 負荷がかかります。

#### **4.2.2 パーティション数の適正化**

spark.sql.shuffle.partitions のデフォルト値「200」は、ビッグデータ処理においては少なすぎる（パーティションが巨大になりすぎる）か、あるいは小規模データにおいては多すぎる（タスクのオーバーヘッドが増える）ことがほとんどです。

**目安:** 1つのパーティション（タスク）が処理するデータ量を 100MB 〜 200MB 程度に収めるのが理想的です。例えば、シャッフル対象のデータが 1TB ある場合、1,000,000 MB / 200 MB \= 5,000 なので、パーティション数は 5,000 程度に設定すべきです。

### **4.3 Adaptive Query Execution (AQE) の深層**

Spark 3.0 以降、特に Databricks 環境では AQE がゲームチェンジャーとなっています。AQE はクエリ実行中に統計情報を収集し、後続のステージの実行計画を動的に修正します 26。

1. **Dynamically Coalescing Shuffle Partitions:** シャッフル後のパーティションサイズを見て、小さすぎるパーティションを自動的に結合（Coalesce）します。これにより、ユーザーは spark.sql.shuffle.partitions を「安全側に振って大きめ（例: 4000）」に設定しておけば、AQE が実行時に適切な数（例: 500）に減らしてくれます 1。これは「多すぎるパーティション」問題に対する特効薬です。  
2. **Dynamically Switching Join Strategies:** 当初は Sort-Merge Join が計画されていても、片方のテーブルがフィルタリングの結果として十分に小さくなった（spark.sql.adaptive.autoBroadcastJoinThreshold 以下になった）場合、AQE は自動的に Broadcast Hash Join に切り替えます。これにより、巨大なシャッフルを回避し、劇的な高速化を実現します 29。  
3. **Optimizing Skew Joins:** データスキュー（偏り）を検知すると、AQE は肥大化したパーティションを複数の小さなタスクに分割（Split）し、相手側のテーブルを複製して結合します。これにより、一部のタスクだけが終わらない「ストラグラー」問題を自動的に緩和します 30。

## ---

**第5章 キャッシング戦略と一時データ管理**

「キャッシュ」は諸刃の剣です。適切に使えば I/O を削減できますが、不適切に使えばメモリを枯渇させ、GC 地獄を引き起こします。Databricks には2種類のキャッシュが存在し、これらを明確に使い分ける必要があります。

### **5.1 Spark Cache (.cache(), .persist()) vs Disk Cache**

| 機能 | Spark Cache | Disk Cache (旧 Delta Cache) |
| :---- | :---- | :---- |
| **保存場所** | Executor JVM ヒープ (MEMORY\_AND\_DISK) | ワーカーノードのローカル SSD (NVMe) |
| **フォーマット** | 行指向 (Java オブジェクト) またはシリアライズ形式 | カラムナ形式 (Parquet/Delta のコピー) |
| **永続性** | アプリケーション終了まで | クラスター再起動まで (場合による) |
| **メモリ影響** | **大** (Execution Memory を圧迫) | **なし** (メインメモリを使用しない) |
| **推奨度** | 低 (反復計算が必要な特定DAGのみ) | **高** (デフォルトで活用すべき) |

31

### **5.2 ベストプラクティス：Disk Cache ファースト**

Databricks 環境では、.cache() を呼ぶ前に、まず **Disk Cache** の活用を検討すべきです。Disk Cache はリモートストレージ（S3/ADLS）のデータをローカルの高速な SSD に透過的にコピーします。これは JVM ヒープを消費しないため、Spark の計算リソースを圧迫しません。特に、同じテーブルに対して異なるクエリを何度も投げるような BI ワークロードや、探索的データ分析において絶大な効果を発揮します。

一方、Spark Cache (df.cache()) は、複雑な計算結果（例えば、多数の結合や集計を行った後の DataFrame）を再利用する場合にのみ使用します。単に S3 から読み込んだ直後のデータを .cache() するのは、Disk Cache の役割と重複し、かつメモリを無駄にするため推奨されません。

## ---

**第6章 結合戦略 (Join Strategies) と SQL チューニング**

結合（Join）はデータ処理の中で最もリソースを消費する操作の一つです。適切な結合戦略を選択することで、パフォーマンスは桁違いに変わります。

### **6.1 主要な結合戦略**

1. **Broadcast Hash Join (BHJ):**  
   * **仕組み:** 小さい方のテーブル（Build 側）を全 Executor にコピー（ブロードキャスト）し、大きい方のテーブル（Probe 側）はシャッフルせずにローカルで結合します。  
   * **適用:** 片方のテーブルがメモリに乗るほど小さい場合。最速です。  
   * **チューニング:** spark.sql.autoBroadcastJoinThreshold で閾値を設定します。ただし、メモリ不足のリスクがあるため、無闇に閾値を上げすぎないよう注意が必要です 1。  
2. **Sort-Merge Join (SMJ):**  
   * **仕組み:** 両方のテーブルを結合キーでシャッフル（ソート）し、同じキーを持つ行をマージします。  
   * **適用:** 両方のテーブルが大きい場合の標準的な戦略。メモリ効率が良く堅牢です。  
   * **チューニング:** spark.sql.shuffle.partitions の設定が重要です。  
3. **Shuffle Hash Join (SHJ):**  
   * **仕組み:** シャッフルした後、Executor 上でハッシュテーブルを作成して結合します。  
   * **適用:** ソートコストを省きたいが、片方のパーティションがメモリに収まる場合。Photon エンジンでは SHJ が好まれる傾向があります。

### **6.2 データスキュー (Data Skew) への対処**

特定のキー（例えば NULL キーや特定の「顧客ID」）にデータが集中すると、そのキーを担当するタスクだけが極端に遅くなり、全体の足を引っ張ります。

**対処法:**

1. **AQE Skew Optimization:** 前述の通り、AQE は自動的にスキューを分割しようとします。まずはこれに任せます。  
2. **Salting (ソルティング):** AQE でも解決しない場合、結合キーに乱数（Salt）を付与してデータを強制的に分散させるテクニックを使います。例えば、キーに \_0 〜 \_9 のサフィックスを付け、相手テーブルを10倍に複製して結合します。  
3. **Skew Hint:** Databricks では SQL ヒントを使って、明示的にスキューがある列と値を指定できます。  
   SQL  
   SELECT /\*+ SKEW('orders', 'customer\_id') \*/ \* FROM orders JOIN customers...

   これにより、Spark は指定された列のスキューをより積極的に分割処理します 34。

## ---

**第7章 モニタリングとトラブルシューティング：ボトルネックの科学的特定**

パフォーマンスチューニングにおいて最も重要なのは、「推測」ではなく「計測」です。Databricks が提供する Spark UI と Ganglia（または Cluster Metrics）を駆使して、ボトルネックの所在を突き止めます。

### **7.1 Spark UI を用いたデバッグフロー**

Spark UI はジョブの実行状況をミクロな視点で可視化します。以下のステップで解析を行います 36。

1. **Jobs / Stages タブ:**  
   * 全体のタイムラインを確認し、異常に時間がかかっている Stage を特定します。  
   * **DAG Visualization:** どの処理（スキャン、シャッフル、書き込み）が含まれているかを把握します。  
2. **Summary Metrics (Stage 詳細画面):**  
   * ここが最も重要です。タスクの実行時間（Duration）の分布（Min, 25%, Median, 75%, Max）を見ます。  
   * **Skew の判定:** もし Max が Median の数倍〜数十倍であれば、深刻なデータスキューが発生しています。一部のタスクだけが長時間稼働し、他は終わって待機している状態です 38。  
3. **Shuffle Read / Write:**  
   * 処理データ量（Input Size / Shuffle Read Size）を確認します。特定のタスクだけデータ量が巨大になっていないか確認します。  
4. **Spill (Memory / Disk):**  
   * **絶対に見逃してはならない指標です。** テーブルに "Shuffle Spill (Disk)" という列があり、値が 0 より大きい場合、メモリ不足によりデータがディスクに退避されています。ディスク I/O はメモリアクセスの数千倍遅いため、これが遅延の直接原因です 40。  
   * **対策:** パーティション数を増やす、インスタンスメモリを増やす、AQE の調整。

### **7.2 Ganglia / Cluster Metrics によるインフラ監視**

Spark UI が「論理的な実行」を示すのに対し、Ganglia は「物理的なリソース消費」を示します 1。

1. **CPU 使用率:**  
   * **健全:** 全ノードの CPU が均等に高く（User/System で 80%以上）、各色の分布が揃っている。  
   * **不健全:** 全体の CPU 使用率が低い。これは I/O 待ち（ストレージが遅い）、Driver ボトルネック（単一ノード処理）、またはロック待ちを示唆します。  
   * **不均衡:** 一部のノードだけ CPU が高く、他が遊んでいる。これはデータスキューの典型的な症状です。  
2. **メモリ使用率:**  
   * Databricks ではキャッシュを多用するため、メモリ使用率が高いこと自体は問題ではありません。しかし、Swap が発生している場合は危険です。  
3. **ネットワーク:**  
   * シャッフルフェーズでトラフィックがスパイクするのは正常です。しかし、ジョブ全体を通じて常にネットワークが高い場合、不必要なデータ転送（Broadcast の失敗など）が疑われます。

### **7.3 Driver ボトルネックの特定**

Executor ではなく、司令塔である Driver が詰まっているケースも多々あります。

* **症状:** Spark UI 上ではタスクが実行されていない（アイドル状態）のに、ジョブが終わらない。Ganglia で Driver ノードの CPU/メモリだけが高い。  
* **原因:** collect() による大量データのフェッチ、大量のファイルメタデータ処理、シングルスレッドの Python コード実行。  
* **ログ:** Driver ログ (log4j) に "Total size of serialized results... is bigger than..." や GC 関連の警告が出ていないか確認します 24。

### **7.4 Query Profile (Databricks SQL / Photon)**

Photon エンジンを使用している場合、Query Profile 機能を使うことで、演算子レベルの詳細なパフォーマンスを確認できます 10。

* **Time in Photon:** クエリ全体の時間のうち、どれだけが Photon で実行されたか。この割合が低い場合、非対応の演算子や UDF がボトルネックになっている可能性があります。  
* **Rows Processed:** 各オペレータを通過する行数。予想外に行数が増えている（Exploding Join）箇所がないか視覚的に確認できます。

## ---

**第8章 インフラストラクチャとコスト最適化**

最後に、コードや設定以外のインフラ選定による最適化について触れます。

### **8.1 オートスケーリング (Autoscaling)**

Databricks のオートスケーリングには「Standard」と「Optimized」の2種類があります 45。

* **Optimized Autoscaling:** 分散処理の特性を考慮し、アグレッシブにノードを追加し、不要になれば即座に縮小します。コスト効率とパフォーマンスのバランスが良いため、通常はこちらを推奨します。  
* **注意点:** ストリーミングなどの定常負荷においては、頻繁なスケーリングが逆効果になる場合があるため、固定サイズの方が安定することもあります。

### **8.2 Spot インスタンスの活用**

コスト削減の切り札です。AWS の Spot Instance や Azure の Spot VM を使用することで、コンピュートコストを最大 70-90% 削減できます。

* **戦略:** Driver ノードは安定性のため「オンデマンド」にし、Executor ノードのみ「スポット」にする構成がベストプラクティスです。Spark は耐障害性があるため、Executor が途中で失われてもリトライが可能だからです。Databricks の "Fleet" 機能を使うことで、複数のインスタンスタイプを組み合わせて可用性を高めることができます。

## ---

**結論**

Databricks Spark のチューニングは、単なるパラメータ調整の技術ではありません。それは、レイクハウスアーキテクチャの特性を理解し、データレイアウト（Liquid Clustering）、実行エンジン（Photon）、そして自律化機能（Predictive Optimization, AQE）を調和させる包括的なエンジニアリングです。

本レポートで解説したように、まずはストレージレイヤの最適化から着手し、次にコンピュートエンジンの特性に合わせたクエリ設計を行い、最後に Spark UI と Ganglia を用いた科学的なデバッグで残存するボトルネックを解消するという手順を踏むことで、パフォーマンスとコストの双方において最適なデータパイプラインを構築することが可能となります。

---

**参考文献一覧:** 1 Databricks Spark performance tuning guide 2 Databricks optimization techniques 6 Photon engine optimization guide 18 Liquid Clustering vs Partitioning 19 Databricks Liquid Clustering documentation 24 Databricks driver bottlenecks 36 Debugging Spark UI 7 Photon engine architecture 26 Adaptive Query Execution details 19 Liquid Clustering recommendations 20 Predictive Optimization insights 12 Delta Lake Compaction strategies 30 Data Skew Identification 40 Shuffle Spill analysis 1 Ganglia metrics interpretation 43 Driver unavailability troubleshooting 34 Skew hint usage 35 Skew join optimization

#### **引用文献**

1. Comprehensive Guide to Optimize Databricks, Spark and Delta Lake Workloads, 1月 29, 2026にアクセス、 [https://www.databricks.com/discover/pages/optimize-data-workloads-guide](https://www.databricks.com/discover/pages/optimize-data-workloads-guide)  
2. 9 Powerful Spark Optimization Techniques in Databricks (With Real Examples), 1月 29, 2026にアクセス、 [https://community.databricks.com/t5/community-articles/9-powerful-spark-optimization-techniques-in-databricks-with-real/td-p/132925](https://community.databricks.com/t5/community-articles/9-powerful-spark-optimization-techniques-in-databricks-with-real/td-p/132925)  
3. Set Spark configuration properties on Databricks, 1月 29, 2026にアクセス、 [https://docs.databricks.com/aws/en/spark/conf](https://docs.databricks.com/aws/en/spark/conf)  
4. Comparing Databricks to Apache Spark, 1月 29, 2026にアクセス、 [https://www.databricks.com/spark/comparing-databricks-to-apache-spark](https://www.databricks.com/spark/comparing-databricks-to-apache-spark)  
5. Top 10 code mistakes that degrade your Spark performance \- Databricks Community, 1月 29, 2026にアクセス、 [https://community.databricks.com/t5/technical-blog/top-10-code-mistakes-that-degrade-your-spark-performance/ba-p/118468](https://community.databricks.com/t5/technical-blog/top-10-code-mistakes-that-degrade-your-spark-performance/ba-p/118468)  
6. The Ultimate Guide to Databricks Photon: Performance, Optimization & Troubleshooting, 1月 29, 2026にアクセス、 [https://b-eye.com/blog/databricks-photon-performance-optimization-guide/](https://b-eye.com/blog/databricks-photon-performance-optimization-guide/)  
7. Announcing Photon Public Preview: The Next Generation Query Engine on the Databricks Lakehouse Platform, 1月 29, 2026にアクセス、 [https://www.databricks.com/blog/2021/06/17/announcing-photon-public-preview-the-next-generation-query-engine-on-the-databricks-lakehouse-platform.html](https://www.databricks.com/blog/2021/06/17/announcing-photon-public-preview-the-next-generation-query-engine-on-the-databricks-lakehouse-platform.html)  
8. When to Use Databricks Photon for Maximum Performance | Unravel Data, 1月 29, 2026にアクセス、 [https://www.unraveldata.com/resources/when-to-use-databricks-photon/](https://www.unraveldata.com/resources/when-to-use-databricks-photon/)  
9. What is Photon? | Databricks on AWS, 1月 29, 2026にアクセス、 [https://docs.databricks.com/aws/en/compute/photon](https://docs.databricks.com/aws/en/compute/photon)  
10. Performance Tuning using Query Profile \- Databricks Community, 1月 29, 2026にアクセス、 [https://community.databricks.com/t5/technical-blog/performance-tuning-using-query-profile/ba-p/96779](https://community.databricks.com/t5/technical-blog/performance-tuning-using-query-profile/ba-p/96779)  
11. Delta Lake Small File Compaction with OPTIMIZE, 1月 29, 2026にアクセス、 [https://delta.io/blog/2023-01-25-delta-lake-small-file-compaction-optimize/](https://delta.io/blog/2023-01-25-delta-lake-small-file-compaction-optimize/)  
12. Compacting Delta tables \- Microsoft Fabric, 1月 29, 2026にアクセス、 [https://learn.microsoft.com/en-us/fabric/data-engineering/table-compaction](https://learn.microsoft.com/en-us/fabric/data-engineering/table-compaction)  
13. Optimize data file layout | Databricks on AWS, 1月 29, 2026にアクセス、 [https://docs.databricks.com/aws/en/delta/optimize](https://docs.databricks.com/aws/en/delta/optimize)  
14. Best practices for performance efficiency \- Azure Databricks \- Microsoft Learn, 1月 29, 2026にアクセス、 [https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/performance-efficiency/best-practices](https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/performance-efficiency/best-practices)  
15. Control data file size | Databricks on AWS, 1月 29, 2026にアクセス、 [https://docs.databricks.com/aws/en/delta/tune-file-size](https://docs.databricks.com/aws/en/delta/tune-file-size)  
16. Liquid Clustering: Optimizing Databricks Workloads for Performance and Cost, 1月 29, 2026にアクセス、 [https://dev.to/aj\_ankit85/liquid-clustering-optimizing-databricks-workloads-for-performance-and-cost-4aai](https://dev.to/aj_ankit85/liquid-clustering-optimizing-databricks-workloads-for-performance-and-cost-4aai)  
17. Repartitioning vs Partitioning vs Bucketing vs Z-Ordering vs Liquid Clustering and Vacuuming Delta Tables | by Kundan Singh | Medium, 1月 29, 2026にアクセス、 [https://medium.com/@kundansingh0619/partitioning-vs-z-ordering-in-delta-lake-28751b61c3d9](https://medium.com/@kundansingh0619/partitioning-vs-z-ordering-in-delta-lake-28751b61c3d9)  
18. When to Use and when Not to Use Liquid Clustering? \- Databricks Community \- 136190, 1月 29, 2026にアクセス、 [https://community.databricks.com/t5/data-engineering/when-to-use-and-when-not-to-use-liquid-clustering/td-p/136190](https://community.databricks.com/t5/data-engineering/when-to-use-and-when-not-to-use-liquid-clustering/td-p/136190)  
19. Use liquid clustering for tables | Databricks on AWS, 1月 29, 2026にアクセス、 [https://docs.databricks.com/aws/en/delta/clustering](https://docs.databricks.com/aws/en/delta/clustering)  
20. How does Databricks predictive optimization complement data actionability platforms?, 1月 29, 2026にアクセス、 [https://www.unraveldata.com/insights/databricks-predictive-optimization/](https://www.unraveldata.com/insights/databricks-predictive-optimization/)  
21. Predictive optimization for Unity Catalog managed tables | Databricks on AWS, 1月 29, 2026にアクセス、 [https://docs.databricks.com/aws/en/optimizations/predictive-optimization](https://docs.databricks.com/aws/en/optimizations/predictive-optimization)  
22. Apache Spark executor memory allocation \- Databricks Knowledge Base, 1月 29, 2026にアクセス、 [https://kb.databricks.com/clusters/spark-executor-memory](https://kb.databricks.com/clusters/spark-executor-memory)  
23. Tuning \- Spark 4.1.0 Documentation \- Apache Spark, 1月 29, 2026にアクセス、 [https://spark.apache.org/docs/latest/tuning.html](https://spark.apache.org/docs/latest/tuning.html)  
24. Resolving Databricks Driver Bottlenecks for Faster Spark Jobs | Unravel Data, 1月 29, 2026にアクセス、 [https://www.unraveldata.com/resources/databricks-driver-bottlenecks-faster-spark-jobs/](https://www.unraveldata.com/resources/databricks-driver-bottlenecks-faster-spark-jobs/)  
25. Preventing Databricks Executor Out of Memory Failures at Scale | Unravel Data Meta, 1月 29, 2026にアクセス、 [https://www.unraveldata.com/resources/preventing-databricks-executor-out-of-memory-failures-at-scale/](https://www.unraveldata.com/resources/preventing-databricks-executor-out-of-memory-failures-at-scale/)  
26. Adaptive query execution | Databricks on AWS, 1月 29, 2026にアクセス、 [https://docs.databricks.com/aws/en/optimizations/aqe](https://docs.databricks.com/aws/en/optimizations/aqe)  
27. Apache Spark AQE: The Game-Changer Your Data Pipeline Has Been Waiting For | by vishal dutt | Medium, 1月 29, 2026にアクセス、 [https://medium.com/@vishal.dutt.data.architect/apache-spark-aqe-the-game-changer-your-data-pipeline-has-been-waiting-for-5a5eab8c543c](https://medium.com/@vishal.dutt.data.architect/apache-spark-aqe-the-game-changer-your-data-pipeline-has-been-waiting-for-5a5eab8c543c)  
28. Unlocking Spark SQL Performance: AQE, Dynamic Partition Pruning & Join Strategy Controls in Databricks | by Mohanaselvan Kathamuthu | Medium, 1月 29, 2026にアクセス、 [https://medium.com/@mohanaslvn/unlocking-spark-sql-performance-aqe-dynamic-partition-pruning-join-strategy-controls-in-eb87aab35191](https://medium.com/@mohanaslvn/unlocking-spark-sql-performance-aqe-dynamic-partition-pruning-join-strategy-controls-in-eb87aab35191)  
29. Performance Tuning \- Spark 4.1.0 Documentation, 1月 29, 2026にアクセス、 [https://spark.apache.org/docs/latest/sql-performance-tuning.html](https://spark.apache.org/docs/latest/sql-performance-tuning.html)  
30. Eliminating Databricks Data Skew Before It Kills Performance, 1月 29, 2026にアクセス、 [https://www.unraveldata.com/resources/databricks-data-skew-balance-partition-workloads/](https://www.unraveldata.com/resources/databricks-data-skew-balance-partition-workloads/)  
31. Optimizing Performance in Databricks: Best Practices for Caching \- B EYE, 1月 29, 2026にアクセス、 [https://b-eye.com/blog/databricks-caching-best-practices/](https://b-eye.com/blog/databricks-caching-best-practices/)  
32. Optimize performance with caching on Databricks, 1月 29, 2026にアクセス、 [https://docs.databricks.com/aws/en/optimizations/disk-cache](https://docs.databricks.com/aws/en/optimizations/disk-cache)  
33. Optimize performance with caching on Azure Databricks \- Microsoft Learn, 1月 29, 2026にアクセス、 [https://learn.microsoft.com/en-us/azure/databricks/optimizations/disk-cache](https://learn.microsoft.com/en-us/azure/databricks/optimizations/disk-cache)  
34. SKEW Hint in Databricks: A Comprehensive Guide to Handling Skewed Data \- Medium, 1月 29, 2026にアクセス、 [https://medium.com/@chhayak02/skew-hint-in-databricks-a-comprehensive-guide-to-handling-skewed-data-322377c40b96](https://medium.com/@chhayak02/skew-hint-in-databricks-a-comprehensive-guide-to-handling-skewed-data-322377c40b96)  
35. Skew join optimization using skew hints \- Azure Databricks | Azure Docs, 1月 29, 2026にアクセス、 [https://docs.azure.cn/en-us/databricks/archive/legacy/skew-join](https://docs.azure.cn/en-us/databricks/archive/legacy/skew-join)  
36. Debugging with the Spark UI | Databricks on AWS, 1月 29, 2026にアクセス、 [https://docs.databricks.com/aws/en/compute/troubleshooting/debugging-spark-ui](https://docs.databricks.com/aws/en/compute/troubleshooting/debugging-spark-ui)  
37. Diagnose cost and performance issues using the Spark UI | Databricks on AWS, 1月 29, 2026にアクセス、 [https://docs.databricks.com/aws/en/optimizations/spark-ui-guide/](https://docs.databricks.com/aws/en/optimizations/spark-ui-guide/)  
38. Apache Spark: How to detect data skew using Spark web UI \- Stack Overflow, 1月 29, 2026にアクセス、 [https://stackoverflow.com/questions/74856755/apache-spark-how-to-detect-data-skew-using-spark-web-ui](https://stackoverflow.com/questions/74856755/apache-spark-how-to-detect-data-skew-using-spark-web-ui)  
39. Skew and spill | Databricks on AWS, 1月 29, 2026にアクセス、 [https://docs.databricks.com/aws/en/optimizations/spark-ui-guide/long-spark-stage-page](https://docs.databricks.com/aws/en/optimizations/spark-ui-guide/long-spark-stage-page)  
40. Spark shuffle spill mem and disk extremely high even when input data is small \- Reddit, 1月 29, 2026にアクセス、 [https://www.reddit.com/r/databricks/comments/1qi140z/spark\_shuffle\_spill\_mem\_and\_disk\_extremely\_high/](https://www.reddit.com/r/databricks/comments/1qi140z/spark_shuffle_spill_mem_and_disk_extremely_high/)  
41. Skew and spill \- Azure Databricks | Microsoft Learn, 1月 29, 2026にアクセス、 [https://learn.microsoft.com/en-us/azure/databricks/optimizations/spark-ui-guide/long-spark-stage-page](https://learn.microsoft.com/en-us/azure/databricks/optimizations/spark-ui-guide/long-spark-stage-page)  
42. Decoding Databricks Cluster Metrics: A Guide to Interpretation and Mitigation, 1月 29, 2026にアクセス、 [https://community.databricks.com/t5/technical-blog/decoding-databricks-cluster-metrics-a-guide-to-interpretation/ba-p/120215](https://community.databricks.com/t5/technical-blog/decoding-databricks-cluster-metrics-a-guide-to-interpretation/ba-p/120215)  
43. Spark job fails with Driver is temporarily unavailable \- Databricks Knowledge Base, 1月 29, 2026にアクセス、 [https://kb.databricks.com/jobs/driver-unavailable](https://kb.databricks.com/jobs/driver-unavailable)  
44. Query profile | Databricks on AWS, 1月 29, 2026にアクセス、 [https://docs.databricks.com/aws/en/sql/user/queries/query-profile](https://docs.databricks.com/aws/en/sql/user/queries/query-profile)  
45. 13 Ways to Optimize Databricks Autoscaling \- overcast blog, 1月 29, 2026にアクセス、 [https://overcast.blog/13-ways-to-optimize-databricks-autoscaling-dfaa4a17637b](https://overcast.blog/13-ways-to-optimize-databricks-autoscaling-dfaa4a17637b)