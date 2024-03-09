from delta import DeltaTable
import pytest
from tests.common import build_schema, build_session


class Test002:

    #
    # Делаю append 100 раз (160 MB * 100 ~= 16 GB)
    # Расчет статистик на 5 колонках (дефолт)
    #
    # ------------------------------------------- benchmark: 1 tests -------------------------------------------
    # Name (time in s)        Min      Max    Mean  StdDev  Median     IQR  Outliers     OPS  Rounds  Iterations
    # ----------------------------------------------------------------------------------------------------------
    # test                 2.9770  13.0775  3.2884  1.0280  3.0762  0.1222      2;17  0.3041     100           1
    # ----------------------------------------------------------------------------------------------------------
    #
    # +------+------------------------------------+----+-----------+--------------------+-----------------------+-------------------+----------------+--------+-----------+---------------------------------------+----------------+----------------+------------------------+
    # |format|id                                  |name|description|location            |createdAt              |lastModified       |partitionColumns|numFiles|sizeInBytes|properties                             |minReaderVersion|minWriterVersion|tableFeatures           |
    # +------+------------------------------------+----+-----------+--------------------+-----------------------+-------------------+----------------+--------+-----------+---------------------------------------+----------------+----------------+------------------------+
    # |delta |72ef2e5f-8b4c-4080-9a38-cf4fc22827ce|NULL|NULL       |s3a://tests/test_002|2024-03-09 12:56:15.683|2024-03-09 13:07:18|[]              |200     |16835060000|{delta.dataSkippingNumIndexedCols -> 5}|1               |2               |[appendOnly, invariants]|
    # +------+------------------------------------+----+-----------+--------------------+-----------------------+-------------------+----------------+--------+-----------+---------------------------------------+----------------+----------------+------------------------+
    #
    # {
    #     "commitInfo": {
    #         "timestamp": 1709989310800,
    #         "operation": "WRITE",
    #         "operationParameters": {
    #         "mode": "Append",
    #         "partitionBy": "[]"
    #         },
    #         "readVersion": 100,
    #         "isolationLevel": "Serializable",
    #         "isBlindAppend": true,
    #         "operationMetrics": {
    #         "numFiles": "2",
    #         "numOutputRows": "1000000",
    #         "numOutputBytes": "168350600"
    #         },
    #         "engineInfo": "Apache-Spark/3.5.1 Delta-Lake/3.1.0",
    #         "txnId": "e3335b2c-9648-4bdb-a819-e419484fa4d5"
    #     }
    # }
    # {
    #     "add": {
    #         "path": "part-00000-52f94dfc-c547-4e20-a674-8508a573678e-c000.snappy.parquet",
    #         "partitionValues": {},
    #         "size": 84179435,
    #         "modificationTime": 1709989310000,
    #         "dataChange": true,
    #         "stats": "{\"numRecords\":500000,\"minValues\":{\"id\":0,\"field_0\":\"offline\",\"field_1\":\"2023-01-01T00:00:00.000Z\",\"field_2\":0,\"field_3\":\"Ad ad aute aute.\"},\"maxValues\":{\"id\":499999,\"field_0\":\"unknown\",\"field_1\":\"2023-12-31T00:00:00.000Z\",\"field_2\":499999,\"field_3\":\"Voluptate voluptate laboris ut. �\"},\"nullCount\":{\"id\":0,\"field_0\":0,\"field_1\":0,\"field_2\":0,\"field_3\":0}}"
    #     }
    # }
    # {
    #     "add": {
    #         "path": "part-00001-6f67377b-8cd0-4812-95e3-7da7bcdea323-c000.snappy.parquet",
    #         "partitionValues": {},
    #         "size": 84171165,
    #         "modificationTime": 1709989310000,
    #         "dataChange": true,
    #         "stats": "{\"numRecords\":500000,\"minValues\":{\"id\":500000,\"field_0\":\"offline\",\"field_1\":\"2023-01-01T00:00:00.000Z\",\"field_2\":500000,\"field_3\":\"Ad ad aute aute.\"},\"maxValues\":{\"id\":999999,\"field_0\":\"unknown\",\"field_1\":\"2023-12-31T00:00:00.000Z\",\"field_2\":999999,\"field_3\":\"Voluptate voluptate laboris ut. �\"},\"nullCount\":{\"id\":0,\"field_0\":0,\"field_1\":0,\"field_2\":0,\"field_3\":0}}"
    #     }
    # }
    #
    def setup_method(self, method) -> None:
        self.session = build_session()
        self.schema = build_schema()
        self.location = "s3a://tests/test_002"
        self.table = (
            DeltaTable.createIfNotExists(self.session)
            .addColumns(self.schema)
            .location(self.location)
            .execute()
        )
        self.session.sql(
            f"ALTER TABLE delta.`{self.location}` "
            "SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = 5)"
        )
        self.test_000_data = (
            self.session.read.format("delta").load("s3a://tests/test_000").persist()
        )

    def act(self) -> None:
        self.test_000_data.write.format("delta").mode("append").save(self.location)

    @pytest.mark.benchmark(disable_gc=True, warmup=False)
    def test(self, benchmark) -> None:
        benchmark.pedantic(self.act, iterations=1, rounds=100)

    def test_check(self) -> None:
        self.session.sql(f"describe detail delta.`{self.location}`").show(
            truncate=False
        )

    def teardown_method(self, method) -> None:
        self.session.stop()
