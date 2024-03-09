from delta import DeltaTable
import dbldatagen as dg  # type: ignore
from tests.common import build_schema, build_session


class Test004:
    #
    # Генерирую данные
    # ~1600 MB, 10_000_000 строк, 2 партиции

    def setup_method(self, method) -> None:
        self.session = build_session()
        self.schema = build_schema()
        self.location = "s3a://tests/test_004"
        self.table = (
            DeltaTable.createIfNotExists(self.session)
            .addColumns(self.schema)
            .location(self.location)
            .execute()
        )

    def test(self) -> None:
        df = (
            dg.DataGenerator(
                self.session,
                rows=10_000_000,
                partitions=2,
                seedColumnName="_id",
            )
            .withSchema(self.schema)
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
        df.write.format("delta").mode("append").save(self.location)

    def test_check(self) -> None:
        self.test_000_data = (
            self.session.read.format("delta")
            .load(f"s3a://tests/{self.location}")
            .persist()
        )
        self.test_000_data.show()

    def teardown_method(self, method) -> None:
        self.session.stop()
