from sqlalchemy.dialects import registry

registry.register("pandas", "calchipan.base", "PandasDialect")
registry.register("pandas.calchipan", "calchipan.base", "PandasDialect")

from sqlalchemy.testing import runner

runner.main()
