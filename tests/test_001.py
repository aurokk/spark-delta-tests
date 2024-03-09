from delta import DeltaTable
import pytest
from tests.common import build_schema, build_session


class Test001:

    #
    # Делаю append 100 раз (160 MB * 100 ~= 16 GB)
    # Расчет статистик отключен
    #
    # ------------------------------------------- benchmark: 1 tests -------------------------------------------
    # Name (time in s)        Min      Max    Mean  StdDev  Median     IQR  Outliers     OPS  Rounds  Iterations
    # ----------------------------------------------------------------------------------------------------------
    # test                 2.9751  12.2624  3.3554  0.9503  3.1435  0.2342      2;12  0.2980     100           1
    # ----------------------------------------------------------------------------------------------------------
    #
    # +------+------------------------------------+----+-----------+--------------------+----------------------+-------------------+----------------+--------+-----------+---------------------------------------+----------------+----------------+------------------------+
    # |format|id                                  |name|description|location            |createdAt             |lastModified       |partitionColumns|numFiles|sizeInBytes|properties                             |minReaderVersion|minWriterVersion|tableFeatures           |
    # +------+------------------------------------+----+-----------+--------------------+----------------------+-------------------+----------------+--------+-----------+---------------------------------------+----------------+----------------+------------------------+
    # |delta |ce3f4190-145c-47e5-bcd0-1e6dfca91b9f|NULL|NULL       |s3a://tests/test_001|2024-03-09 12:43:45.87|2024-03-09 12:53:02|[]              |200     |16835060000|{delta.dataSkippingNumIndexedCols -> 0}|1               |2               |[appendOnly, invariants]|
    # +------+------------------------------------+----+-----------+--------------------+----------------------+-------------------+----------------+--------+-----------+---------------------------------------+----------------+----------------+------------------------+
    #
    # {
    #     "commitInfo": {
    #         "timestamp": 1709988582739,
    #         "operation": "WRITE",
    #         "operationParameters": {
    #         "mode": "Append",
    #         "partitionBy": "[]"
    #         },
    #         "readVersion": 101,
    #         "isolationLevel": "Serializable",
    #         "isBlindAppend": true,
    #         "operationMetrics": {
    #         "numFiles": "2",
    #         "numOutputRows": "1000000",
    #         "numOutputBytes": "168350600"
    #         },
    #         "engineInfo": "Apache-Spark/3.5.1 Delta-Lake/3.1.0",
    #         "txnId": "eeab3d24-1b36-49e5-931a-fec2f6913895"
    #     }
    # }
    # {
    #     "add": {
    #         "path": "part-00000-6f083381-178f-4e54-8139-af13d56d5175-c000.snappy.parquet",
    #         "partitionValues": {},
    #         "size": 84179435,
    #         "modificationTime": 1709988582000,
    #         "dataChange": true,
    #         "stats": "{\"numRecords\":500000}"
    #     }
    # }
    # {
    #     "add": {
    #         "path": "part-00001-0fad8298-328e-43ba-971d-3d86af84755e-c000.snappy.parquet",
    #         "partitionValues": {},
    #         "size": 84171165,
    #         "modificationTime": 1709988582000,
    #         "dataChange": true,
    #         "stats": "{\"numRecords\":500000}"
    #     }
    # }
    #
    def setup_method(self, method) -> None:
        self.session = build_session()
        self.schema = build_schema()
        self.location = "s3a://tests/test_001"
        self.table = (
            DeltaTable.createIfNotExists(self.session)
            .addColumns(self.schema)
            .location(self.location)
            .execute()
        )
        self.session.sql(
            f"ALTER TABLE delta.`{self.location}` "
            "SET TBLPROPERTIES ('delta.dataSkippingNumIndexedCols' = 0)"
        )
        self.test_000_data = (
            self.session.read.format("delta").load("s3a://tests/test_000").persist()
        )

    def act(self) -> None:
        self.test_000_data.write.format("delta").mode("append").save(self.location)

    @pytest.mark.benchmark(disable_gc=True, warmup=False)
    def test(self, benchmark) -> None:
        benchmark.pedantic(self.act, iterations=1, rounds=100)

    def test_check_00(self) -> None:
        self.session.sql(f"describe detail delta.`{self.location}`").show(
            truncate=False
        )

    def test_check_01(self) -> None:
        self.table.toDF().show()

    def teardown_method(self, method) -> None:
        self.session.stop()
