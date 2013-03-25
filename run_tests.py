from sqlalchemy.dialects import registry

registry.register("pandas", "calchipan.base", "PandasDialect")
registry.register("pandas.calchipan", "calchipan.base", "PandasDialect")

from sqlalchemy.testing import runner

def setup_py_test():
    runner.setup_py_test()

if __name__ == '__main__':
    runner.main()
