# wappy


[![pypi](https://img.shields.io/pypi/v/wappy.svg)](https://pypi.org/project/wappy/)
[![python](https://img.shields.io/pypi/pyversions/wappy.svg)](https://pypi.org/project/wappy/)
[![Build Status](https://github.com/oeuf/wappy/actions/workflows/dev.yml/badge.svg)](https://github.com/oeuf/wappy/actions/workflows/dev.yml)
[![codecov](https://codecov.io/gh/oeuf/wappy/branch/main/graphs/badge.svg)](https://codecov.io/github/oeuf/wappy)



Basic implementation of Write-Audit-Publish using PySpark.


* Documentation: <https://oeuf.github.io/wappy>
* GitHub: <https://github.com/oeuf/wappy>
* PyPI: <https://pypi.org/project/wappy/>
* Free software: Apache-2.0


## TODO

* Add setup documentation
* Add wap module with tests
* Add github action(s) to run tests + linting.
* (Write)
* (Audit) Initial data quality checks: completeness, consistency, uniqueness, timeliness, relevance,
  accuracy, validity. [Ref](https://ssmertin.com/articles/strategies-for-data-quality-with-apache-spark/), [Ref](https://netflixtechblog.com/data-pipeline-asset-management-with-dataflow-86525b3e21ca), and [Ref](https://netflixtechblog.com/ready-to-go-sample-data-pipelines-with-dataflow-17440a9e141d)
* (Publish): Add high watermark timestamps.

## Credits

This package was created with [Cookiecutter](https://github.com/audreyr/cookiecutter) and the [waynerv/cookiecutter-pypackage](https://github.com/waynerv/cookiecutter-pypackage) project template.
