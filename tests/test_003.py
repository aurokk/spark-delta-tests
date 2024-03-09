from delta import DeltaTable
import pytest
from tests.common import build_schema, build_session


class Test003:

    #
    # Расчет статистик на 32 колонках (максимум)
    # Делаю Append 100 раз (160mb * 100 ~= 16gb)
    #
    #
    # ------------------------------------------- benchmark: 1 tests -------------------------------------------
    # Name (time in s)        Min      Max    Mean  StdDev  Median     IQR  Outliers     OPS  Rounds  Iterations
    # ----------------------------------------------------------------------------------------------------------
    # test                 3.2532  13.5983  3.5719  1.0464  3.3659  0.1756      1;12  0.2800     100           1
    # ----------------------------------------------------------------------------------------------------------
    #
    # +------+------------------------------------+----+-----------+--------------------+-----------------------+-------------------+----------------+--------+-----------+----------------------------------------+----------------+----------------+------------------------+
    # |format|id                                  |name|description|location            |createdAt              |lastModified       |partitionColumns|numFiles|sizeInBytes|properties                              |minReaderVersion|minWriterVersion|tableFeatures           |
    # +------+------------------------------------+----+-----------+--------------------+-----------------------+-------------------+----------------+--------+-----------+----------------------------------------+----------------+----------------+------------------------+
    # |delta |97a51410-4bba-4d4c-92b6-3f5cb2b9c328|NULL|NULL       |s3a://tests/test_003|2024-03-09 13:10:13.386|2024-03-09 13:17:47|[]              |200     |16835060000|{delta.dataSkippingNumIndexedCols -> 32}|1               |2               |[appendOnly, invariants]|
    # +------+------------------------------------+----+-----------+--------------------+-----------------------+-------------------+----------------+--------+-----------+----------------------------------------+----------------+----------------+------------------------+
    #
    # {
    #     "commitInfo": {
    #         "timestamp": 1709990176520,
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
    #         "txnId": "34c75a97-2da0-43dd-8c54-4b04d6bebcc6"
    #     }
    # }
    # {
    #     "add": {
    #         "path": "part-00000-2e703757-0a27-4270-82c1-9dd74ed6d1fd-c000.snappy.parquet",
    #         "partitionValues": {},
    #         "size": 84179435,
    #         "modificationTime": 1709990175000,
    #         "dataChange": true,
    #         "stats": "{\"numRecords\":500000,\"minValues\":{\"id\":0,\"field_0\":\"offline\",\"field_1\":\"2023-01-01T00:00:00.000Z\",\"field_2\":0,\"field_3\":\"Ad ad aute aute.\",\"field_4\":\"2023-01-01T00:00:00.000Z\",\"field_5\":0,\"field_6\":\"Ad ad irure ipsum ut elit ad.\",\"field_7\":\"2023-01-01T00:00:00.000Z\",\"field_8\":0,\"field_9\":\"Ad ad ut aliquip elit labore qui\",\"field_10\":\"2023-01-01T00:00:00.000Z\",\"field_11\":0,\"field_12\":\"0\",\"field_13\":\"2023-01-01T00:00:00.000Z\",\"field_14\":0,\"field_15\":\"0\",\"field_16\":\"2023-01-01T00:00:00.000Z\",\"field_17\":0,\"field_18\":\"0\",\"field_19\":\"2023-01-01T00:00:00.000Z\"},\"maxValues\":{\"id\":499999,\"field_0\":\"unknown\",\"field_1\":\"2023-12-31T00:00:00.000Z\",\"field_2\":499999,\"field_3\":\"Voluptate voluptate laboris ut. �\",\"field_4\":\"2023-12-31T00:00:00.000Z\",\"field_5\":499999,\"field_6\":\"Voluptate voluptate veniam aute �\",\"field_7\":\"2023-12-31T00:00:00.000Z\",\"field_8\":499999,\"field_9\":\"Voluptate veniam dolore sit pari�\",\"field_10\":\"2023-12-31T00:00:00.000Z\",\"field_11\":499999,\"field_12\":\"99999\",\"field_13\":\"2023-12-31T00:00:00.000Z\",\"field_14\":499999,\"field_15\":\"99999\",\"field_16\":\"2023-12-31T00:00:00.000Z\",\"field_17\":499999,\"field_18\":\"99999\",\"field_19\":\"2023-12-31T00:00:00.000Z\"},\"nullCount\":{\"id\":0,\"field_0\":0,\"field_1\":0,\"field_2\":0,\"field_3\":0,\"field_4\":0,\"field_5\":0,\"field_6\":0,\"field_7\":0,\"field_8\":0,\"field_9\":0,\"field_10\":0,\"field_11\":0,\"field_12\":0,\"field_13\":0,\"field_14\":0,\"field_15\":0,\"field_16\":0,\"field_17\":0,\"field_18\":0,\"field_19\":0}}"
    #     }
    # }
    # {
    #     "add": {
    #         "path": "part-00001-9b787bb4-042b-476a-b489-686c6c24ddcc-c000.snappy.parquet",
    #         "partitionValues": {},
    #         "size": 84171165,
    #         "modificationTime": 1709990175000,
    #         "dataChange": true,
    #         "stats": "{\"numRecords\":500000,\"minValues\":{\"id\":500000,\"field_0\":\"offline\",\"field_1\":\"2023-01-01T00:00:00.000Z\",\"field_2\":500000,\"field_3\":\"Ad ad aute aute.\",\"field_4\":\"2023-01-01T00:00:00.000Z\",\"field_5\":500000,\"field_6\":\"Ad ad irure ipsum ut elit ad.\",\"field_7\":\"2023-01-01T00:00:00.000Z\",\"field_8\":500000,\"field_9\":\"Ad ad ut aliquip elit labore qui\",\"field_10\":\"2023-01-01T00:00:00.000Z\",\"field_11\":500000,\"field_12\":\"500000\",\"field_13\":\"2023-01-01T00:00:00.000Z\",\"field_14\":500000,\"field_15\":\"500000\",\"field_16\":\"2023-01-01T00:00:00.000Z\",\"field_17\":500000,\"field_18\":\"500000\",\"field_19\":\"2023-01-01T00:00:00.000Z\"},\"maxValues\":{\"id\":999999,\"field_0\":\"unknown\",\"field_1\":\"2023-12-31T00:00:00.000Z\",\"field_2\":999999,\"field_3\":\"Voluptate voluptate laboris ut. �\",\"field_4\":\"2023-12-31T00:00:00.000Z\",\"field_5\":999999,\"field_6\":\"Voluptate voluptate veniam aute �\",\"field_7\":\"2023-12-31T00:00:00.000Z\",\"field_8\":999999,\"field_9\":\"Voluptate veniam dolore sit pari�\",\"field_10\":\"2023-12-31T00:00:00.000Z\",\"field_11\":999999,\"field_12\":\"999999\",\"field_13\":\"2023-12-31T00:00:00.000Z\",\"field_14\":999999,\"field_15\":\"999999\",\"field_16\":\"2023-12-31T00:00:00.000Z\",\"field_17\":999999,\"field_18\":\"999999\",\"field_19\":\"2023-12-31T00:00:00.000Z\"},\"nullCount\":{\"id\":0,\"field_0\":0,\"field_1\":0,\"field_2\":0,\"field_3\":0,\"field_4\":0,\"field_5\":0,\"field_6\":0,\"field_7\":0,\"field_8\":0,\"field_9\":0,\"field_10\":0,\"field_11\":0,\"field_12\":0,\"field_13\":0,\"field_14\":0,\"field_15\":0,\"field_16\":0,\"field_17\":0,\"field_18\":0,\"field_19\":0}}"
    #     }
    # }
    #

    def setup_method(self, method) -> None:
        self.session = build_session()
        self.schema = build_schema()
        self.location = "s3a://tests/test_003"
        self.table = (
            DeltaTable.createIfNotExists(self.session)
            .addColumns(self.schema)
            .location(self.location)
            .execute()
        )
        self.session.sql(
            f"ALTER TABLE delta.`{self.location}` "
            "SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = 32)"
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
