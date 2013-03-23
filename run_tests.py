from sqlalchemy.dialects import registry

registry.register("pandas", "calhipan.base", "PandasDialect")
registry.register("pandas.calhipan", "calhipan.base", "PandasDialect")

from sqlalchemy.testing import runner

runner.main()
