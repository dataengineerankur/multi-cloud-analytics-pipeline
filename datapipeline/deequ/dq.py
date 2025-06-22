from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationSuite

class DataQuality:
    def __init__(self, spark, df):
        self.spark = spark
        self.df = df
        self.suite = VerificationSuite(spark).onData(df)
        self.checks = []

    def add_size_check(self, min_rows, level=CheckLevel.Error):
        self.checks.append(
            Check(self.spark, level, f"size_at_least_{min_rows}").hasSize(lambda s: s >= min_rows)
        )
        return self

    def add_not_null(self, column, level=CheckLevel.Error):
        """Ensure no NULLs in `column`."""
        self.checks.append(
            Check(self.spark, level, f"{column}_not_null").isComplete(column)
        )
        return self

    def add_unique(self, column, level=CheckLevel.Error):
        """Ensure all values in `column` are unique."""
        self.checks.append(
            Check(self.spark, level, f"{column}_unique").isUnique(column)
        )
        return self

    def run(self):
        # attach all checks
        for c in self.checks:
            self.suite = self.suite.addCheck(c)
        result = self.suite.run()

        if result.status != "Success":
            failures = []
            for _, checks in result.checkResults.items():
                for cr in checks:
                    if cr["status"] != "Success":
                        failures.append(f"{cr['constraint']}: {cr['message']}")
            raise AssertionError("Data Quality failed:\n" + "\n".join(failures))

        return result 