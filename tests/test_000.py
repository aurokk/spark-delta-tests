from delta import DeltaTable
from tests.common import build_schema, build_session


class Test000:
    def setup_method(self, method):
        self.session = build_session()
        self.schema = build_schema()
        self.location = "s3a://tests/test000"
        self.table = (
            DeltaTable.createIfNotExists(self.session)
            .addColumns(self.schema)
            .location(self.location)
            .execute()
        )

    def act(self):
        df = self.session.createDataFrame([(1, 1), (2, 2)], self.schema)
        df.write.format("delta").mode("append").save(self.location)

    def test(self, benchmark):
        benchmark.pedantic(self.act, iterations=1, rounds=100)

    def teardown_method(self, method):
        self.session.stop()
