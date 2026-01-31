# Databricks Spark チューニング完全ガイド (詳細版)

このリポジトリは、Databricks上でのSparkジョブのパフォーマンスを最適化するための網羅的なガイドと、その効果を検証するための実行可能なコードを提供します。
Gemini DeepResearchによる最新の技術調査に基づき、**「なぜ遅いのか？」「なぜ落ちる(OOM)のか？」** の根本原因を科学的に特定し、解決するための手順を体系化しています。

## 目次

1. [チューニング・ワークフロー (Step-by-Step)](#チューニング・ワークフロー-step-by-step)
2. [Step 1: データレイアウトとストレージ最適化](#step-1-データレイアウトとストレージ最適化)
    - [Liquid Clustering vs Z-Order 徹底比較](#liquid-clustering-vs-z-order-徹底比較)
    - [Predictive Optimization (予測最適化)](#predictive-optimization-予測最適化)
3. [Step 2: データの再分配とシャッフル](#step-2-データの再分配とシャッフル)
4. [Step 3: クエリ実行戦略 (Join & AQE)](#step-3-クエリ実行戦略-join--aqe)
    - [Skew対策 (AQE vs Manual Salting)](#skew対策-aqe-vs-manual-salting)
5. [Step 4: コンピュートとキャッシュ](#step-4-コンピュートとキャッシュ)
    - [Photon Engine vs JVM](#photon-engine-vs-jvm)
    - [UDFの罠 (Python UDF vs Pandas UDF)](#udfの罠-python-udf-vs-pandas-udf)
    - [Disk Cache First 戦略](#disk-cache-first-戦略)
6. [Step 5: トラブルシューティング (OOM対策)](#step-5-トラブルシューティング-oom対策)
7. [検証コードの実行方法](#検証コードの実行方法)

---

## チューニング・ワークフロー (Step-by-Step)

パフォーマンス問題に直面した際、やみくもに設定を変えるのではなく、以下の順序で確認・対応してください。

1.  **現状把握 (Monitoring)**:
    *   Spark UIで「どこで時間を使っているか」特定する (Scan? Shuffle? CPU?)
    *   Gangliaでリソース使用状況を見る (メモリ不足? Disk Spill?)
2.  **I/Oの削減 (Scan削減)**: **【効果大】**
    *   読み込むデータ量が多すぎないか？
    *   **対策**: 不要な列・行を削る。Liquid Clustering / Z-Order で Data Skipping を効かせる。
3.  **Shuffleの最適化**:
    *   `Exchange` ステージで時間がかかっているか？
    *   **対策**: Broadcast Joinが使えないか検討する。Skew (偏り) がないか確認し、あればAQEのSkew Joinを有効化する。
4.  **Computeの効率化**:
    *   CPUが張り付いているか？
    *   **対策**: Photonエンジンの利用。Python UDFを排除する。Disk Cacheで2回目以降を高速化する。

---

## Step 1: データレイアウトとストレージ最適化

Sparkの高速化において最も重要なのは **「いかにデータを読まないか (Data Skipping)」** です。

### Liquid Clustering vs Z-Order 徹底比較

どちらも「似たデータを物理的に近くに配置する」ことで、クエリ時に不要なファイル読み込みをスキップさせる技術ですが、現在は **Liquid Clustering が圧倒的に推奨** です。

| 特徴 | Liquid Clustering (推奨) | Z-Order (従来) |
| :--- | :--- | :--- |
| **仕組み** | データを柔軟にクラスタリングし、増分でメンテナンス | パーティション内の全ファイルをリライトしてソート |
| **書き込み競合** | **強い** (Row Level Concurrency対応) | 弱い (書き込み中にロック競合しやすい) |
| **メンテナンス** | `OPTIMIZE` が増分で高速に終わる | `OPTIMIZE ZORDER` は全量スキャンに近く重い |
| **Skew耐性** | **強い** (偏りを自動で分散吸収) | 弱い (パーティション毎に偏りが残る) |
| **設定方法** | `CLUSTER BY (col1, col2)` | `OPTIMIZE ... ZORDER BY (col1)` |

**【結論】**: 新規テーブル構築や、Databricks Runtime 13.3以降を利用できる環境であれば、迷わず **Liquid Clustering** を採用してください。

### Predictive Optimization (予測最適化)
「いつOPTIMIZEを実行するか？」という悩みを解消します。Unity Catalog管理テーブルにおいて、Databricks AIが自動で `OPTIMIZE` や `VACUUM` を実行します。

---

## Step 2: データの再分配とシャッフル

*   **並列性を上げたい時**: `repartition(n)`。タスク数が少なすぎてCPUが遊んでいる場合に使用。Shuffleが発生。
*   **ファイルを減らしたい時**: `coalesce(n)`。フィルタリング後などでデータが小さくなったのにパーティション数が多い場合に使用。Shuffleなしで高速。

---

## Step 3: クエリ実行戦略 (Join & AQE)

**AQE (Adaptive Query Execution)** はSpark 3.xの最強機能です。
*   **Dynamic Join Strategy**: SortMergeJoin → BroadcastJoin への自動切り替え。
*   **Skew Join Optimization**: 極端に大きいタスクを分割して並列処理。

### Skew対策 (AQE vs Manual Salting)
通常はAQEに任せればOKですが、AQEでも検知できない極端なSkewがある場合、手動で **Salting (ソルティング)** を行います。
*   **手法**: 結合キーにランダムなサフィックス (`_0`〜`_9`など) を付与し、相手側データも複製して結合します。
*   **検証コード**: `src/06_skew_salting.py`

---

## Step 4: コンピュートとキャッシュ

### Photon Engine vs JVM
PhotonはC++で書かれたベクトル化エンジンです。SIMD命令を活用し、JVMのオーバーヘッド（GCなど）を回避します。
*   **有効確認**: `explain()` で Physical Plan に `Photon` が含まれているか確認。

### UDFの罠 (Python UDF vs Pandas UDF)
Python UDFはパフォーマンスのボトルネックになりがちです。
*   **Python UDF**: 1行ずつPickle化してPythonプロセスと通信するため **非常に遅い**。
*   **Pandas UDF**: Arrow形式でバッチ転送するため高速。
*   **Native Spark**: 最速。可能な限りこれを使う。
*   **検証コード**: `src/05_photon_vs_udf.py`

### Disk Cache First 戦略
メモリキャッシュ (`.cache()`) よりも、まず **Disk Cache** の活用を検討してください。
*   **Disk Cache**: WorkerのSSDを使用。メモリを圧迫せず、GCも起きない。Workerタイプで "d" 付き (例: Standard_L4ds) を選ぶと自動有効。
*   **Spark Cache**: JVMヒープを使用。OOMのリスクがあるため、計算コストが非常に高い中間データにのみ限定して使用する。

---

## Step 5: トラブルシューティング (OOM対策)

OOM (Out Of Memory) には2種類あり、対策が異なります。

### 1. Driver OOM
*   **症状**: `Driver is temporarily unavailable` エラー。
*   **原因**: `collect()` で大量データをDriverに集めている。または Broadcast Join の閾値が大きすぎてDriverがパンクしている。
*   **対策**: `collect()` をやめる。BroadCast閾値を下げる。

### 2. Executor OOM
*   **症状**: `Container killed by YARN for exceeding memory limits`。
*   **原因**: パーティションサイズが大きすぎる (Skew)。メモリ効率の悪いUDF。
*   **対策**:
    *   **Skew**: AQE有効化、Salting。
    *   **Memory**: `repartition` でパーティションを増やして1タスクのデータ量を減らす。Executorのメモリを増やす。

---

## 実践練習問題 (Dojo)
知識を定着させるために、わざと「遅いクエリ」を書いた練習用ノートブックを用意しました。
`src/99_practice_problems.py` を開き、以下の課題に挑戦してください。

1.  **Challenge 1**: 日次レポートが遅すぎる！ (Full Scan vs Data Skipping)
2.  **Challenge 2**: 爆売れ商品の集計が終わらない！ (Skew Join)
3.  **Challenge 3**: 消費税計算ごときでなぜ待たされる？ (Python UDF vs Native)
4.  **Challenge 4**: "Shuffleしすぎ" 問題 (Repartition vs Coalesce)
5.  **Challenge 5**: マスタ結合なのにSortしている (Join Strategy)
6.  **Challenge 6**: 同じ計算を2回するな (Cache)

各課題にはヒントがあります。
行き詰まったら、**`src/99_practice_problems_solutions.py` (模範解答集)** を参照してください。

---

## 検証コードの実行方法
`src/` 配下のコードで、上記の挙動を実際に比較検証できます。

| ファイル名 | 内容 |
| --- | --- |
| **[00_data_generation.py](src/00_data_generation.py)** | 検証用データの生成。売上(1000万件)と商品(1万件)。 |
| **[01_storage_optimization.py](src/01_storage_optimization.py)** | Liquid Clustering vs Z-Order の比較。 |
| **[02_repartition_strategies.py](src/02_repartition_strategies.py)** | Shuffle有無の挙動の違い。 |
| **[03_join_strategies.py](src/03_join_strategies.py)** | Join戦略とAQE Skew Joinの検証。 |
| **[04_compute_and_cache.py](src/04_compute_and_cache.py)** | Cache戦略とPhoton確認。 |
| **[05_photon_vs_udf.py](src/05_photon_vs_udf.py)** | **【New】** Native vs Python UDF vs Pandas UDF 速度比較。 |
| **[06_skew_salting.py](src/06_skew_salting.py)** | **【New】** 手動SaltingによるSkew対策の実装例。 |
| **[99_practice_data_generation.py](src/99_practice_data_generation.py)** | **【Dojo】** 練習問題用のデータ生成 (個別実行)。 |
| **[99_practice_problems.py](src/99_practice_problems.py)** | **【Dojo】** 実践練習問題 (全6問)。 |
| **[99_practice_problems_solutions.py](src/99_practice_problems_solutions.py)** | **【Answers】** 練習問題の模範解答。 |
