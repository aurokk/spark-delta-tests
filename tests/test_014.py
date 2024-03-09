from delta import DeltaTable
import dbldatagen as dg  # type: ignore
from pyspark.sql import SparkSession, DataFrame
import pytest
from tests.common import build_schema, build_session


class Test014:

    #
    # Делаю merge (A->B) с обновлением 1 строчки 100 раз
    # Ликвид кластеринг по id, статистики включены, размер файликов 1 MB
    #
    # ------------------------------------------- benchmark: 1 tests ------------------------------------------
    # Name (time in s)        Min     Max    Mean  StdDev  Median     IQR  Outliers     OPS  Rounds  Iterations
    # ---------------------------------------------------------------------------------------------------------
    # test                 1.5593  3.1265  1.7770  0.3325  1.6268  0.1152     12;22  0.5628     100           1
    # ---------------------------------------------------------------------------------------------------------
    #
    def setup_method(self, method) -> None:
        def configure(x: SparkSession.Builder):
            (
                x.config("spark.sql.files.maxRecordsPerFile", "6250")
                .config("spark.databricks.delta.optimize.maxFileSize", "1048576")
                .config(
                    "spark.databricks.delta.clusteredTable.enableClusteringTablePreview",
                    "true",
                )
            )  # 10_000_000 / 1600

        self.session = build_session(configure)
        self.schema = build_schema()
        self.location = "s3a://tests/test_014"
        self.tmp_location = "s3a://tests/test_014_tmp"
        self.table = (
            DeltaTable.createIfNotExists(self.session)
            .addColumns(self.schema)
            .location(self.tmp_location)
            .execute()
        )
        self.session.sql(
            f"CREATE TABLE IF NOT EXISTS delta.`{self.location}` "
            "USING DELTA "
            "CLUSTER BY (id) "
            f"AS SELECT * FROM delta.`{self.tmp_location}`;"
        )
        self.session.sql(
            f"ALTER TABLE delta.`{self.location}` "
            "SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = 32)"
        )
        self.session.read.format("delta").load("s3a://tests/test_004").write.format(
            "delta"
        ).mode("append").save(self.location)
        self.startingId = 10_000_000

    def act(self) -> None:
        df: DataFrame = (
            dg.DataGenerator(
                self.session,
                rows=1,
                partitions=1,
                seedColumnName="_id",
                startingId=self.startingId,
            )
            .withSchema(self.schema)
            .withColumnSpec("id", minValue=0, maxValue=9_999_999, random=True)
            .withColumnSpec(
                "field_0",
                values=["online", "offline", "unknown"],
            )
            .withColumnSpec(
                "field_3",
                text=dg.ILText(paragraphs=(1, 2), sentences=(1, 2)),
            )
            .withColumnSpec(
                "field_6",
                text=dg.ILText(paragraphs=(1, 2), sentences=(1, 2)),
            )
            .withColumnSpec(
                "field_9",
                text=dg.ILText(paragraphs=(1, 2), sentences=(1, 2)),
            )
            .build()
        )
        self.table.alias("B").merge(
            df.alias("A"), "A.id = B.id"
        ).whenNotMatchedInsertAll().whenMatchedUpdateAll().execute()
        self.startingId += 1

    @pytest.mark.benchmark(disable_gc=True, warmup=False)
    def test(self, benchmark) -> None:
        benchmark.pedantic(self.act, iterations=1, rounds=100)

    def test_check(self) -> None:
        self.session.sql(f"describe detail delta.`{self.location}`").show(
            truncate=False
        )

    def teardown_method(self, method) -> None:
        self.session.stop()
